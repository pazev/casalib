"""Tests for casalib.sql_connection."""
import pytest
from typing import List, Optional, Tuple
from unittest.mock import MagicMock, call
from dataclasses import dataclass

import pandas as pd

from casalib.sql_connection.connection import Connection
from casalib.sql_connection.metadata import (
    Metadata,
    QueryInfo,
    TableInfo,
)
from casalib.sql_connection.query import Query
from casalib.sql_connection.sql_dialect_abstract import (
    SqlDialectAbstract,
)
from casalib.sql_connection.table import Table
from casalib.sql_connection.worker_abstract import (
    WorkerAbstract,
)


# ------------------------------------
# Fixtures: concrete implementations
# ------------------------------------

@dataclass
class MockDialect(SqlDialectAbstract):
    """Minimal dialect returning predictable SQL."""

    def select(self, table_name: str) -> str:
        return f"SELECT * FROM {table_name}"

    def agg(  # pylint: disable=too-many-arguments
        self,
        query: str,
        groupby=None,
        count_=None,
        count_null_=None,
        count_distinct_=None,
        sum_=None,
        mean_=None,
        min_=None,
        max_=None,
        percentile_=None,
        percentile_ignore_values_=None,
        cols_before=None,
        cols_after=None,
    ) -> str:
        return f"SELECT agg FROM ({query})"

    def get_duplicates(
        self, keys: List[str]
    ) -> str:
        return f"SELECT dupes FROM ({self.input_query})"

    def jsonify(
        self,
        keys: List[str],
        columns: List[str],
    ) -> str:
        return f"SELECT json FROM ({self.input_query})"

    def sample(self, num_samples: int) -> str:
        return (
            f"SELECT * FROM ({self.input_query})"
            f" LIMIT {num_samples}"
        )

    def last_partitions(
        self,
        date_ingestion: str,
        columns: List[str],
    ) -> str:
        return (
            f"SELECT last FROM ({self.input_query})"
        )

    def enrich(
        self,
        other,
        keys,
        prefix=None,
    ) -> str:
        return (
            f"SELECT enriched FROM ({self.input_query})"
        )

    def get_diffs(
        self,
        other: str,
        keys,
        columns,
    ) -> str:
        return (
            f"SELECT diffs FROM ({self.input_query})"
        )

    def op(
        self,
        add=None,
        rename=None,
        select_only=None,
        exclude=None,
    ) -> str:
        return f"SELECT op FROM ({self.input_query})"

    @classmethod
    def util_table_name_has_fullname(
        cls, table_name: str
    ) -> bool:
        return "." in table_name

    @classmethod
    def util_table_name_split_schema(
        cls, table_name: str
    ) -> Tuple[str, str]:
        schema, table = table_name.split(".", 1)
        return schema, table


@pytest.fixture
def dialect():
    return MockDialect


@pytest.fixture
def worker():
    return MagicMock(spec=WorkerAbstract)


@pytest.fixture
def table_info():
    return TableInfo(
        table_name="schema.tbl",
        schema="schema",
        table="tbl",
        partition_cols={"dt": "string", "region": "string"},
        custom_metadata={},
    )


@pytest.fixture
def metadata(table_info):
    return Metadata(
        cols={"id": "int", "name": "string"},
        table=table_info,
    )


# ------------------------------------
# Metadata / TableInfo / QueryInfo
# ------------------------------------

class TestMetadata:
    def test_table_info_fields(self, table_info):
        assert table_info.table_name == "schema.tbl"
        assert table_info.schema == "schema"
        assert table_info.table == "tbl"
        assert "dt" in table_info.partition_cols

    def test_query_info_fields(self):
        qi = QueryInfo(
            input_tables=["a", "b"],
            partition_cols={"dt": "string"},
            custom_metadata={},
        )
        assert qi.input_tables == ["a", "b"]

    def test_metadata_table_defaults_to_none(self):
        m = Metadata(cols={})
        assert m.table is None
        assert m.query_info is None

    def test_metadata_with_table(self, metadata):
        assert metadata.table is not None
        assert metadata.cols == {
            "id": "int",
            "name": "string",
        }


# ------------------------------------
# Query
# ------------------------------------

class TestQuery:
    def test_from_table_name(self, dialect):
        q = Query.from_table_name("schema.tbl", dialect)
        assert q.query == "SELECT * FROM schema.tbl"
        assert q.dialect is dialect

    def test_worker_raises_when_not_set(self, dialect):
        q = Query(query="SELECT 1", dialect=dialect)
        with pytest.raises(RuntimeError):
            _ = q.worker

    def test_set_worker_and_get(self, dialect, worker):
        q = Query(query="SELECT 1", dialect=dialect)
        q.set_worker(worker)
        assert q.worker is worker

    def test_set_worker_returns_self(self, dialect, worker):
        q = Query(query="SELECT 1", dialect=dialect)
        result = q.set_worker(worker)
        assert result is q

    def test_set_worker_none(self, dialect):
        q = Query(query="SELECT 1", dialect=dialect)
        q.set_worker(None)
        with pytest.raises(RuntimeError):
            _ = q.worker

    def test_collect_delegates_to_worker(
        self, dialect, worker
    ):
        expected = pd.DataFrame({"a": [1]})
        worker.run_query.return_value = expected
        q = Query(
            query="SELECT 1", dialect=dialect
        ).set_worker(worker)
        result = q.collect()
        worker.run_query.assert_called_once_with(
            "SELECT 1"
        )
        pd.testing.assert_frame_equal(result, expected)

    def test_create_insert_delegates(
        self, dialect, worker
    ):
        q = Query(
            query="SELECT 1", dialect=dialect
        ).set_worker(worker)
        result = q.create_insert(
            "schema.tbl", partition_cols=["dt"]
        )
        worker.create_insert.assert_called_once_with(
            query="SELECT 1",
            table_name="schema.tbl",
            partition_cols=["dt"],
        )
        assert result is q

    def test_create_ctas_delegates(
        self, dialect, worker
    ):
        q = Query(
            query="SELECT 1", dialect=dialect
        ).set_worker(worker)
        result = q.create_ctas("schema.tbl")
        worker.create_ctas.assert_called_once_with(
            query="SELECT 1",
            table_name="schema.tbl",
            partition_cols=None,
        )
        assert result is q

    def test_metadata_delegates(
        self, dialect, worker, metadata
    ):
        worker.get_query_metadata.return_value = metadata
        q = Query(
            query="SELECT 1", dialect=dialect
        ).set_worker(worker)
        result = q.metadata()
        worker.get_query_metadata.assert_called_once_with(
            "SELECT 1"
        )
        assert result is metadata


# ------------------------------------
# _QueryBuilder (via Query.q)
# ------------------------------------

class TestQueryBuilder:
    def test_q_returns_builder(self, dialect, worker):
        q = Query(
            query="SELECT 1", dialect=dialect
        ).set_worker(worker)
        assert q.q is not None

    def test_select_returns_wired_query(
        self, dialect, worker
    ):
        q = Query(
            query="SELECT 1", dialect=dialect
        ).set_worker(worker)
        result = q.q.select("schema.tbl")
        assert isinstance(result, Query)
        assert result.dialect is dialect
        assert result.worker_ is worker
        assert result.query == (
            "SELECT * FROM schema.tbl"
        )

    def test_sample_returns_wired_query(
        self, dialect, worker
    ):
        q = Query(
            query="SELECT * FROM t", dialect=dialect
        ).set_worker(worker)
        result = q.q.sample(10)
        assert isinstance(result, Query)
        assert "LIMIT 10" in result.query
        assert result.worker_ is worker

    def test_agg_returns_wired_query(
        self, dialect, worker
    ):
        q = Query(
            query="SELECT * FROM t", dialect=dialect
        ).set_worker(worker)
        result = q.q.agg(
            query="SELECT * FROM t",
            groupby=["col"],
        )
        assert isinstance(result, Query)
        assert result.worker_ is worker

    def test_chaining_propagates_worker(
        self, dialect, worker
    ):
        q = Query(
            query="SELECT * FROM t", dialect=dialect
        ).set_worker(worker)
        result = q.q.select("t").q.sample(5)
        assert isinstance(result, Query)
        assert result.worker_ is worker

    def test_builder_without_worker(self, dialect):
        q = Query(query="SELECT 1", dialect=dialect)
        result = q.q.sample(1)
        assert isinstance(result, Query)
        assert result.worker_ is None

    def test_get_duplicates(self, dialect, worker):
        q = Query(
            query="SELECT * FROM t", dialect=dialect
        ).set_worker(worker)
        result = q.q.get_duplicates(["id"])
        assert isinstance(result, Query)
        assert result.worker_ is worker

    def test_jsonify(self, dialect, worker):
        q = Query(
            query="SELECT * FROM t", dialect=dialect
        ).set_worker(worker)
        result = q.q.jsonify(["id"], ["col"])
        assert isinstance(result, Query)
        assert result.worker_ is worker

    def test_enrich(self, dialect, worker):
        q = Query(
            query="SELECT * FROM t", dialect=dialect
        ).set_worker(worker)
        result = q.q.enrich(
            "SELECT * FROM u", keys=["id"]
        )
        assert isinstance(result, Query)
        assert result.worker_ is worker

    def test_get_diffs(self, dialect, worker):
        q = Query(
            query="SELECT * FROM t", dialect=dialect
        ).set_worker(worker)
        result = q.q.get_diffs(
            "SELECT * FROM u",
            keys=["id"],
            columns=["val"],
        )
        assert isinstance(result, Query)
        assert result.worker_ is worker

    def test_op(self, dialect, worker):
        q = Query(
            query="SELECT * FROM t", dialect=dialect
        ).set_worker(worker)
        result = q.q.op(exclude=["col"])
        assert isinstance(result, Query)
        assert result.worker_ is worker

    def test_last_partitions(self, dialect, worker):
        q = Query(
            query="SELECT * FROM t", dialect=dialect
        ).set_worker(worker)
        result = q.q.last_partitions(
            "dt", ["id"]
        )
        assert isinstance(result, Query)
        assert result.worker_ is worker


# ------------------------------------
# Table
# ------------------------------------

class TestTable:
    def test_worker_raises_when_not_set(
        self, dialect
    ):
        t = Table(
            table_name="schema.tbl", dialect=dialect
        )
        with pytest.raises(RuntimeError):
            _ = t.worker

    def test_set_worker_and_get(
        self, dialect, worker
    ):
        t = Table(
            table_name="schema.tbl", dialect=dialect
        ).set_worker(worker)
        assert t.worker is worker

    def test_set_worker_returns_self(
        self, dialect, worker
    ):
        t = Table(
            table_name="schema.tbl", dialect=dialect
        )
        assert t.set_worker(worker) is t

    def test_drop_delegates(self, dialect, worker):
        t = Table(
            table_name="schema.tbl", dialect=dialect
        ).set_worker(worker)
        result = t.drop()
        worker.drop.assert_called_once_with(
            "schema.tbl"
        )
        assert result is t

    def test_metadata_delegates(
        self, dialect, worker, metadata
    ):
        worker.get_table_metadata.return_value = metadata
        t = Table(
            table_name="schema.tbl", dialect=dialect
        ).set_worker(worker)
        result = t.metadata()
        worker.get_table_metadata.assert_called_once_with(
            "schema.tbl"
        )
        assert result is metadata

    def test_list_partitions_returns_dataframe(
        self, dialect, worker, metadata
    ):
        worker.get_table_metadata.return_value = metadata
        worker.list_partitions.return_value = [
            ("2024-01-01", "us"),
            ("2024-01-02", "eu"),
        ]
        t = Table(
            table_name="schema.tbl", dialect=dialect
        ).set_worker(worker)
        df = t.list_partitions()
        assert list(df.columns) == ["dt", "region"]
        assert len(df) == 2
        assert df["dt"].iloc[0] == "2024-01-01"

    def test_list_partitions_passes_filters(
        self, dialect, worker, metadata
    ):
        worker.get_table_metadata.return_value = metadata
        worker.list_partitions.return_value = []
        t = Table(
            table_name="schema.tbl", dialect=dialect
        ).set_worker(worker)
        t.list_partitions("2024-*", "us")
        worker.list_partitions.assert_called_once_with(
            "schema.tbl", "2024-*", "us"
        )

    def test_list_partitions_raises_without_table_meta(
        self, dialect, worker
    ):
        worker.get_table_metadata.return_value = (
            Metadata(cols={})
        )
        t = Table(
            table_name="schema.tbl", dialect=dialect
        ).set_worker(worker)
        with pytest.raises(RuntimeError):
            t.list_partitions()

    def test_drop_partitions_delegates(
        self, dialect, worker
    ):
        t = Table(
            table_name="schema.tbl", dialect=dialect
        ).set_worker(worker)
        t.drop_partitions("2024-*", "us")
        worker.drop_partitions.assert_called_once_with(
            "schema.tbl", "2024-*", "us"
        )

    def test_query_property_returns_wired_query(
        self, dialect, worker
    ):
        t = Table(
            table_name="schema.tbl", dialect=dialect
        ).set_worker(worker)
        q = t.query
        assert isinstance(q, Query)
        assert q.dialect is dialect
        assert q.worker_ is worker
        assert q.query == (
            "SELECT * FROM schema.tbl"
        )

    def test_collect_delegates(
        self, dialect, worker
    ):
        expected = pd.DataFrame({"a": [1]})
        worker.run_query.return_value = expected
        t = Table(
            table_name="schema.tbl", dialect=dialect
        ).set_worker(worker)
        result = t.collect()
        pd.testing.assert_frame_equal(result, expected)


# ------------------------------------
# Connection
# ------------------------------------

class TestConnection:
    def test_worker_is_none_when_not_set(
        self, dialect
    ):
        c = Connection(dialect=dialect)
        assert c.worker is None

    def test_set_worker_and_get(
        self, dialect, worker
    ):
        c = Connection(dialect=dialect)
        c.set_worker(worker)
        assert c.worker is worker

    def test_set_worker_returns_self(
        self, dialect, worker
    ):
        c = Connection(dialect=dialect)
        assert c.set_worker(worker) is c

    def test_query_returns_wired_query(
        self, dialect, worker
    ):
        c = Connection(dialect=dialect).set_worker(
            worker
        )
        q = c.query("SELECT 1")
        assert isinstance(q, Query)
        assert q.query == "SELECT 1"
        assert q.dialect is dialect
        assert q.worker_ is worker

    def test_table_returns_wired_table(
        self, dialect, worker
    ):
        c = Connection(dialect=dialect).set_worker(
            worker
        )
        t = c.table("schema.tbl")
        assert isinstance(t, Table)
        assert t.table_name == "schema.tbl"
        assert t.dialect is dialect
        assert t.worker_ is worker

    def test_query_without_worker(self, dialect):
        c = Connection(dialect=dialect)
        q = c.query("SELECT 1")
        assert q.worker_ is None

    def test_table_without_worker(self, dialect):
        c = Connection(dialect=dialect)
        t = c.table("schema.tbl")
        assert t.worker_ is None


# ------------------------------------
# SqlDialectAbstract utils
# ------------------------------------

class TestSqlDialectUtils:
    def test_util_has_fullname_true(self, dialect):
        assert dialect.util_table_name_has_fullname(
            "schema.tbl"
        ) is True

    def test_util_has_fullname_false(self, dialect):
        assert dialect.util_table_name_has_fullname(
            "tbl"
        ) is False

    def test_util_split_schema(self, dialect):
        schema, table = (
            dialect.util_table_name_split_schema(
                "schema.tbl"
            )
        )
        assert schema == "schema"
        assert table == "tbl"
