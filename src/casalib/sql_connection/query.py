"""Query class."""
from dataclasses import dataclass, field
from typing import Dict, List, Optional, Tuple, Type, Union

import pandas as pd

from .metadata import Metadata
from .sql_dialect_abstract import (
    SqlDialectAbstract,
    SqlDialectProtocol,
)
from .worker_abstract import (
    AsyncWorkerAbstract,
    WorkerAbstract,
)


class _QueryBuilder(SqlDialectProtocol["Query"]):
    """Proxy for a dialect instance that wires
    worker/dialect onto returned Queries.

    All methods mirror ``SqlDialectAbstract``
    and return fully-wired ``Query`` objects so
    callers do not need to configure them
    manually.

    Attributes:
        _instance: Instantiated dialect used to
            generate SQL.
        _dialect: Dialect type to attach to
            returned Query objects.
        _worker: Worker to attach to returned
            Query objects.
    """

    def __init__(
        self,
        dialect_instance: SqlDialectAbstract,
        worker: Optional[WorkerAbstract],
    ) -> None:
        self._instance = dialect_instance
        self._worker = worker

    def __repr__(self) -> str:
        """Show dialect and worker."""
        return (
            f"{self.__class__.__name__}("
            f"dialect="
            f"{self._instance.__class__.__name__}"
            f", worker={self._worker!r})"
        )

    def _wrap(self, result: str) -> "Query":
        return Query(
            query=result,
            dialect=self._instance.__class__,
        ).set_worker(self._worker)

    def select(self, table_name: str) -> "Query":
        """Generate a SELECT * query for a table.

        Args:
            table_name: Fully qualified table
                name.

        Returns:
            A Query wrapping
            ``SELECT * FROM <table_name>``.
        """
        return self._wrap(
            self._instance.select(table_name)
        )

    def agg(  # pylint: disable=too-many-arguments
        self,
        groupby: Optional[List[str]] = None,
        *,
        count_: Optional[List[str]] = None,
        count_null_: Optional[List[str]] = None,
        count_distinct_: Optional[List[str]] = None,
        sum_: Optional[List[str]] = None,
        mean_: Optional[List[str]] = None,
        min_: Optional[List[str]] = None,
        max_: Optional[List[str]] = None,
        percentile_: Optional[
            Dict[int, List[str]]
        ] = None,
        percentile_ignore_values_: Optional[
            Dict[str, List[float]]
        ] = None,
        cols_before: Optional[
            List[Union[str, Tuple[str, str]]]
        ] = None,
        cols_after: Optional[
            List[Union[str, Tuple[str, str]]]
        ] = None,
    ) -> "Query":
        """Generate an aggregation query.

        Args:
            groupby: Columns to GROUP BY.
            count_: Columns to COUNT.
            count_null_: Columns to COUNT
                including nulls.
            count_distinct_: Columns to COUNT
                DISTINCT.
            sum_: Columns to SUM.
            mean_: Columns to AVG.
            min_: Columns to MIN.
            max_: Columns to MAX.
            percentile_: Mapping of percentile
                value (0–100) to columns.
            percentile_ignore_values_: Mapping
                of column name to values to
                exclude.
            cols_before: Extra expressions
                prepended to SELECT.
            cols_after: Extra expressions
                appended to SELECT.

        Returns:
            A Query wrapping the aggregation SQL.
        """
        return self._wrap(
            self._instance.agg(
                groupby=groupby,
                count_=count_,
                count_null_=count_null_,
                count_distinct_=count_distinct_,
                sum_=sum_,
                mean_=mean_,
                min_=min_,
                max_=max_,
                percentile_=percentile_,
                percentile_ignore_values_=(
                    percentile_ignore_values_
                ),
                cols_before=cols_before,
                cols_after=cols_after,
            )
        )

    def get_duplicates(
        self, keys: List[str]
    ) -> "Query":
        """Return all rows whose key combination
        appears more than once.

        Args:
            keys: Columns that form the
                duplicate key.

        Returns:
            A Query containing only the
            duplicated rows.
        """
        return self._wrap(
            self._instance.get_duplicates(keys)
        )

    def jsonify(
        self,
        keys: List[str],
        columns: List[str],
    ) -> "Query":
        """Collapse columns into a single JSON
        value, keeping keys intact.

        Args:
            keys: Columns to keep as regular
                output columns.
            columns: Columns to fold into a
                JSON value.

        Returns:
            A Query with ``keys`` as columns
            and a JSON column for the rest.
        """
        return self._wrap(
            self._instance.jsonify(keys, columns)
        )

    def sample(self, num_samples: int) -> "Query":
        """Return a sample of rows from the
        input query.

        Args:
            num_samples: Number of rows to
                return.

        Returns:
            A Query limited to ``num_samples``
            rows.
        """
        return self._wrap(
            self._instance.sample(num_samples)
        )

    def last_partitions(
        self,
        date_ingestion: str,
        columns: List[str],
    ) -> "Query":
        """Select only the most recent partition
        rows for each key group.

        Args:
            date_ingestion: Name of the
                date/timestamp column that
                identifies the ingestion
                partition.
            columns: Columns that define the
                partition key.

        Returns:
            A Query containing only the
            latest-partition rows.
        """
        return self._wrap(
            self._instance.last_partitions(
                date_ingestion, columns
            )
        )

    def enrich(
        self,
        other: Union[str, List[str]],
        keys: List[Union[str, Tuple[str, str]]],
        prefix: Optional[Union[str, List[str]]] = None,
    ) -> "Query":
        """LEFT JOIN the input query with one or
        more queries.

        Args:
            other: A single query string or a
                list of query strings to join.
            keys: Join condition. Each element
                is either a column name or a
                ``(left_col, right_col)`` tuple.
            prefix: Optional prefix for columns
                from the joined side(s).

        Returns:
            A Query wrapping the enriched result
            set.
        """
        return self._wrap(
            self._instance.enrich(
                other, keys, prefix
            )
        )

    def get_diffs(
        self,
        other: str,
        keys: List[Union[str, Tuple[str, str]]],
        columns: List[Union[str, Tuple[str, str]]],
    ) -> "Query":
        """Compare column values between the
        input query and another query.

        Args:
            other: The second query to compare
                against.
            keys: Join keys. Each element is a
                column name or a
                ``(left_col, right_col)`` tuple.
            columns: Columns to compare. Each
                element is a column name or a
                ``(left_col, right_col)`` tuple.

        Returns:
            A Query showing the differing rows
            and their values.
        """
        return self._wrap(
            self._instance.get_diffs(
                other, keys, columns
            )
        )

    def op(
        self,
        add: Optional[Dict[str, str]] = None,
        rename: Optional[Dict[str, str]] = None,
        select_only: Optional[List[str]] = None,
        exclude: Optional[List[str]] = None,
    ) -> "Query":
        """Apply column-level operations to the
        input query.

        Args:
            add: Mapping of
                ``{new_col: sql_expression}``
                for columns to add.
            rename: Mapping of
                ``{new_name: old_name}`` for
                columns to rename.
            select_only: Keep only these
                columns.
            exclude: Drop these columns.

        Returns:
            A Query with the column
            transformations applied.
        """
        return self._wrap(
            self._instance.op(
                add=add,
                rename=rename,
                select_only=select_only,
                exclude=exclude,
            )
        )


@dataclass
class Query:
    """A stored SQL string paired with a dialect
    type and an optional worker.

    Attributes:
        query: SQL string.
        dialect: Dialect class used to generate
            and transform SQL.
    """

    query: str
    dialect: Type[SqlDialectAbstract]
    worker_: Optional[WorkerAbstract] = field(
        default=None, init=False, repr=False
    )

    @classmethod
    def from_table_name(
        cls,
        table_name: str,
        dialect: Type[SqlDialectAbstract],
    ) -> "Query":
        """Create a Query that selects all data
        from a table.

        Args:
            table_name: Fully qualified table
                name.
            dialect: Dialect class to use for
                SQL generation.

        Returns:
            A Query wrapping
            ``SELECT * FROM <table_name>``.
        """
        return cls(
            query=dialect(
                input_query=table_name
            ).select(table_name),
            dialect=dialect,
        )

    @property
    def worker(self) -> WorkerAbstract:
        """Return the database worker.

        Returns:
            The configured WorkerAbstract
            instance.

        Raises:
            RuntimeError: If the worker has not
                been set.
        """
        if self.worker_ is not None:
            return self.worker_

        raise RuntimeError(
            '`Worker` not set. Please check'
        )

    def set_worker(
        self, worker: Optional[WorkerAbstract]
    ) -> "Query":
        """Set the database worker.

        Args:
            worker: Worker instance to attach,
                or None.

        Returns:
            Self, for chaining.
        """
        self.worker_ = worker
        return self

    @property
    def q(self) -> _QueryBuilder:
        """Return a dialect builder initialised
        with the current query string.

        Any Query returned by the builder's
        methods will have the worker and dialect
        already set.

        Returns:
            A _QueryBuilder wrapping the
            instantiated dialect.
        """
        return _QueryBuilder(
            dialect_instance=self.dialect(
                input_query=self.query
            ),
            worker=self.worker_,
        )

    def collect(self) -> pd.DataFrame:
        """Execute the query and return results
        as a DataFrame.

        Returns:
            A DataFrame with the query results.
        """
        return self.worker.run_query(self.query)

    def create_insert(
        self,
        table_name: str,
        partition_cols: Optional[List[str]] = None,
    ) -> "Query":
        """Create the destination table if
        absent, then insert query results.

        Args:
            table_name: Destination table name.
            partition_cols: Partition keys to
                use on table creation.

        Returns:
            Self, for chaining.
        """
        self.worker.create_insert(
            query=self.query,
            table_name=table_name,
            partition_cols=partition_cols,
        )
        return self

    def create_ctas(
        self,
        table_name: str,
        partition_cols: Optional[List[str]] = None,
    ) -> "Query":
        """Create a table from this query using
        CREATE TABLE AS SELECT.

        Args:
            table_name: Name for the new table.
            partition_cols: Partition keys for
                the new table.

        Returns:
            Self, for chaining.
        """
        self.worker.create_ctas(
            query=self.query,
            table_name=table_name,
            partition_cols=partition_cols,
        )
        return self

    def metadata(self) -> Metadata:
        """Return metadata for the query result
        set.

        Returns:
            Metadata describing the columns and
            types of the result set.
        """
        return self.worker.get_query_metadata(
            self.query
        )


class _AsyncQueryBuilder(
    SqlDialectProtocol["AsyncQuery"]
):
    """Async proxy for a dialect instance.

    Mirrors ``_QueryBuilder`` but returns
    fully-wired ``AsyncQuery`` objects so
    callers do not need to configure them
    manually.

    Attributes:
        _instance: Instantiated dialect.
        _dialect: Dialect type to attach.
        _worker: Async worker to attach.
    """

    def __init__(
        self,
        dialect_instance: SqlDialectAbstract,
        worker: Optional[AsyncWorkerAbstract],
    ) -> None:
        self._instance = dialect_instance
        self._worker = worker

    def __repr__(self) -> str:
        """Show dialect and worker."""
        return (
            f"{self.__class__.__name__}("
            f"dialect="
            f"{self._instance.__class__.__name__}"
            f", worker={self._worker!r})"
        )

    def _wrap(
        self, result: str
    ) -> "AsyncQuery":
        return AsyncQuery(
            query=result,
            dialect=self._instance.__class__,
        ).set_worker(self._worker)

    def select(
        self, table_name: str
    ) -> "AsyncQuery":
        """Generate a SELECT * query.

        Args:
            table_name: Fully qualified name.

        Returns:
            An AsyncQuery wrapping the SQL.
        """
        return self._wrap(
            self._instance.select(table_name)
        )

    def agg(  # pylint: disable=too-many-arguments
        self,
        groupby: Optional[List[str]] = None,
        *,
        count_: Optional[List[str]] = None,
        count_null_: Optional[
            List[str]
        ] = None,
        count_distinct_: Optional[
            List[str]
        ] = None,
        sum_: Optional[List[str]] = None,
        mean_: Optional[List[str]] = None,
        min_: Optional[List[str]] = None,
        max_: Optional[List[str]] = None,
        percentile_: Optional[
            Dict[int, List[str]]
        ] = None,
        percentile_ignore_values_: Optional[
            Dict[str, List[float]]
        ] = None,
        cols_before: Optional[
            List[Union[str, Tuple[str, str]]]
        ] = None,
        cols_after: Optional[
            List[Union[str, Tuple[str, str]]]
        ] = None,
    ) -> "AsyncQuery":
        """Generate an aggregation query.

        Args:
            groupby: Columns to GROUP BY.
            count_: Columns to COUNT.
            count_null_: Columns to count NULLs.
            count_distinct_: COUNT DISTINCT cols.
            sum_: Columns to SUM.
            mean_: Columns to AVG.
            min_: Columns to MIN.
            max_: Columns to MAX.
            percentile_: Percentile → cols map.
            percentile_ignore_values_: Exclusion
                map.
            cols_before: Prepended expressions.
            cols_after: Appended expressions.

        Returns:
            An AsyncQuery wrapping the SQL.
        """
        return self._wrap(
            self._instance.agg(
                groupby=groupby,
                count_=count_,
                count_null_=count_null_,
                count_distinct_=(
                    count_distinct_
                ),
                sum_=sum_,
                mean_=mean_,
                min_=min_,
                max_=max_,
                percentile_=percentile_,
                percentile_ignore_values_=(
                    percentile_ignore_values_
                ),
                cols_before=cols_before,
                cols_after=cols_after,
            )
        )

    def get_duplicates(
        self, keys: List[str]
    ) -> "AsyncQuery":
        """Return rows with duplicate keys.

        Args:
            keys: Columns forming the key.

        Returns:
            An AsyncQuery of duplicated rows.
        """
        return self._wrap(
            self._instance.get_duplicates(keys)
        )

    def jsonify(
        self,
        keys: List[str],
        columns: List[str],
    ) -> "AsyncQuery":
        """Collapse columns into a JSON value.

        Args:
            keys: Columns to keep as-is.
            columns: Columns to fold into JSON.

        Returns:
            An AsyncQuery with the JSON col.
        """
        return self._wrap(
            self._instance.jsonify(
                keys, columns
            )
        )

    def sample(
        self, num_samples: int
    ) -> "AsyncQuery":
        """Return a sample of rows.

        Args:
            num_samples: Number of rows.

        Returns:
            An AsyncQuery limited to that count.
        """
        return self._wrap(
            self._instance.sample(num_samples)
        )

    def last_partitions(
        self,
        date_ingestion: str,
        columns: List[str],
    ) -> "AsyncQuery":
        """Select the latest-partition rows.

        Args:
            date_ingestion: Ingestion date col.
            columns: Partition key columns.

        Returns:
            An AsyncQuery of latest rows.
        """
        return self._wrap(
            self._instance.last_partitions(
                date_ingestion, columns
            )
        )

    def enrich(
        self,
        other: Union[str, List[str]],
        keys: List[
            Union[str, Tuple[str, str]]
        ],
        prefix: Optional[
            Union[str, List[str]]
        ] = None,
    ) -> "AsyncQuery":
        """LEFT JOIN the query with other(s).

        Args:
            other: Query string(s) to join.
            keys: Join conditions.
            prefix: Optional column prefix(es).

        Returns:
            An AsyncQuery of the joined result.
        """
        return self._wrap(
            self._instance.enrich(
                other, keys, prefix
            )
        )

    def get_diffs(
        self,
        other: str,
        keys: List[
            Union[str, Tuple[str, str]]
        ],
        columns: List[
            Union[str, Tuple[str, str]]
        ],
    ) -> "AsyncQuery":
        """Compare columns between two queries.

        Args:
            other: Second query to compare.
            keys: Join keys.
            columns: Columns to compare.

        Returns:
            An AsyncQuery showing diffs.
        """
        return self._wrap(
            self._instance.get_diffs(
                other, keys, columns
            )
        )

    def op(
        self,
        add: Optional[Dict[str, str]] = None,
        rename: Optional[
            Dict[str, str]
        ] = None,
        select_only: Optional[
            List[str]
        ] = None,
        exclude: Optional[List[str]] = None,
    ) -> "AsyncQuery":
        """Apply column-level operations.

        Args:
            add: Columns to add.
            rename: Columns to rename.
            select_only: Columns to keep.
            exclude: Columns to drop.

        Returns:
            An AsyncQuery with transformations.
        """
        return self._wrap(
            self._instance.op(
                add=add,
                rename=rename,
                select_only=select_only,
                exclude=exclude,
            )
        )


@dataclass
class AsyncQuery:
    """Async counterpart of Query.

    SQL building via ``.q`` is synchronous.
    Terminal methods are coroutines.

    Attributes:
        query: SQL string.
        dialect: Dialect class for SQL
            generation.
    """

    query: str
    dialect: Type[SqlDialectAbstract]
    worker_: Optional[
        AsyncWorkerAbstract
    ] = field(
        default=None, init=False, repr=False
    )

    @classmethod
    def from_table_name(
        cls,
        table_name: str,
        dialect: Type[SqlDialectAbstract],
    ) -> "AsyncQuery":
        """Create an AsyncQuery selecting all
        rows from a table.

        Args:
            table_name: Fully qualified name.
            dialect: Dialect class to use.

        Returns:
            An AsyncQuery wrapping SELECT *.
        """
        return cls(
            query=dialect(
                input_query=table_name
            ).select(table_name),
            dialect=dialect,
        )

    @property
    def worker(self) -> AsyncWorkerAbstract:
        """Return the async worker.

        Returns:
            The configured worker.

        Raises:
            RuntimeError: If not set.
        """
        if self.worker_ is not None:
            return self.worker_
        raise RuntimeError(
            '`Worker` not set. Please check'
        )

    def set_worker(
        self,
        worker: Optional[AsyncWorkerAbstract],
    ) -> "AsyncQuery":
        """Set the async worker.

        Args:
            worker: Worker to attach, or None.

        Returns:
            Self, for chaining.
        """
        self.worker_ = worker
        return self

    @property
    def q(self) -> _AsyncQueryBuilder:
        """Return an async dialect builder.

        Any AsyncQuery returned by the builder
        will have the worker and dialect set.

        Returns:
            An _AsyncQueryBuilder wrapping the
            instantiated dialect.
        """
        return _AsyncQueryBuilder(
            dialect_instance=self.dialect(
                input_query=self.query
            ),
            worker=self.worker_,
        )

    async def collect(self) -> pd.DataFrame:
        """Execute the query and return results.

        Returns:
            A DataFrame with the query results.
        """
        return await self.worker.run_query(
            self.query
        )

    async def create_insert(
        self,
        table_name: str,
        partition_cols: Optional[
            List[str]
        ] = None,
    ) -> "AsyncQuery":
        """Create or insert into a table.

        Args:
            table_name: Destination table name.
            partition_cols: Partition keys.

        Returns:
            Self, for chaining.
        """
        await self.worker.create_insert(
            query=self.query,
            table_name=table_name,
            partition_cols=partition_cols,
        )
        return self

    async def create_ctas(
        self,
        table_name: str,
        partition_cols: Optional[
            List[str]
        ] = None,
    ) -> "AsyncQuery":
        """Create a table using CTAS.

        Args:
            table_name: Name for the new table.
            partition_cols: Partition keys.

        Returns:
            Self, for chaining.
        """
        await self.worker.create_ctas(
            query=self.query,
            table_name=table_name,
            partition_cols=partition_cols,
        )
        return self

    async def metadata(self) -> Metadata:
        """Return metadata for the result set.

        Returns:
            Metadata for the query result.
        """
        return await self.worker.get_query_metadata(
            self.query
        )
