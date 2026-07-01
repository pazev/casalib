"""Tests for dialect helpers and rendering."""
# pylint: disable=protected-access,too-few-public-methods
# pylint: disable=import-error,no-name-in-module
import re
from pathlib import Path
from typing import List, Optional

import pytest

from casalib.sql_connection.dialects.helpers import (
    AggCol,
    AggOperationEnum,
    PercentileConfig,
    ProcessedAggDef,
    normalise_cols,
    process_agg_args,
    retrieve_agg_parameters,
)
from casalib.sql_connection.dialects.dialect_def import (
    RenderedAgg,
)
from casalib.sql_connection.dialects import (
    AnsiDialect,
    PrestoDialect,
)

ANSI = AnsiDialect._dialect_def
PRESTO = PrestoDialect._dialect_def


def _ws(sql: str) -> str:
    """Collapse whitespace for comparison."""
    return re.sub(r"\s+", " ", sql).strip()


# normalise_cols

class TestNormaliseCols:
    """Tests for normalise_cols."""

    def test_string_becomes_identity_tuple(self) -> None:
        """Bare string yields identity pair."""
        assert normalise_cols(["a"]) == [("a", "a")]

    def test_tuple_passthrough(self) -> None:
        """Existing tuple is returned unchanged."""
        result = normalise_cols([("a", "b")])
        assert result == [("a", "b")]

    def test_mixed(self) -> None:
        """Mix of str and tuple is handled."""
        result = normalise_cols(["x", ("y", "z")])
        assert result == [("x", "x"), ("y", "z")]

    def test_empty(self) -> None:
        """Empty input yields empty list."""
        assert normalise_cols([]) == []


# AggCol ordering

class TestAggColOrdering:
    """Tests for AggCol.__lt__ ordering."""

    def _col(
        self,
        col: str,
        op: AggOperationEnum,
        perc: Optional[int] = None,
    ) -> AggCol:
        """Build AggCol for ordering tests."""
        cfg = (
            PercentileConfig(percentile=perc)
            if perc is not None
            else None
        )
        return AggCol(
            col=col, op=op, percentile_config=cfg
        )

    def test_order_by_col_name(self) -> None:
        """Earlier col name sorts first."""
        a = self._col("a", AggOperationEnum.COUNT)
        b = self._col("b", AggOperationEnum.COUNT)
        assert a < b
        assert not b < a

    def test_order_by_op_same_col(self) -> None:
        """Different ops on same col are ordered."""
        a = self._col("x", AggOperationEnum.COUNT)
        b = self._col("x", AggOperationEnum.SUM)
        assert a < b or b < a

    def test_none_percentile_less_than_some(
        self,
    ) -> None:
        """No config sorts before having config."""
        a = AggCol(
            col="x",
            op=AggOperationEnum.PERCENTILE,
            percentile_config=None,
        )
        b = AggCol(
            col="x",
            op=AggOperationEnum.PERCENTILE,
            percentile_config=PercentileConfig(
                percentile=50
            ),
        )
        assert a < b

    def test_none_vs_none_not_less(self) -> None:
        """Two identical AggCols are not less-than."""
        a = AggCol(col="x", op=AggOperationEnum.COUNT)
        b = AggCol(col="x", op=AggOperationEnum.COUNT)
        assert not a < b

    def test_percentile_ordered_by_value(self) -> None:
        """Lower percentile value sorts before higher."""
        a = AggCol(
            col="x",
            op=AggOperationEnum.PERCENTILE,
            percentile_config=PercentileConfig(
                percentile=25
            ),
        )
        b = AggCol(
            col="x",
            op=AggOperationEnum.PERCENTILE,
            percentile_config=PercentileConfig(
                percentile=75
            ),
        )
        assert a < b


# PercentileConfig

class TestPercentileConfig:
    """Tests for PercentileConfig dataclass."""

    def test_defaults(self) -> None:
        """ignore_values defaults to empty list."""
        cfg = PercentileConfig(percentile=50)
        assert cfg.percentile == 50
        assert cfg.ignore_values == []

    def test_with_ignore(self) -> None:
        """ignore_values stores provided list."""
        cfg = PercentileConfig(
            percentile=90,
            ignore_values=[0.0, -1.0],
        )
        assert cfg.ignore_values == [0.0, -1.0]


# retrieve_agg_parameters

class TestRetrieveAggParameters:
    """Tests for retrieve_agg_parameters factory."""

    def test_simple_ops_sorted(self) -> None:
        """Result is sorted by col name."""
        result = retrieve_agg_parameters(
            simple_agg={
                AggOperationEnum.SUM: ["b"],
                AggOperationEnum.COUNT: ["a"],
            },
            percentiles={},
            percentiles_ignore_vals={},
        )
        cols = [c.col for c in result]
        assert cols == sorted(cols)

    def test_percentile_config_attached(self) -> None:
        """PercentileConfig is built from inputs."""
        result = retrieve_agg_parameters(
            simple_agg={},
            percentiles={50: ["amount"]},
            percentiles_ignore_vals={"amount": [0.0]},
        )
        assert len(result) == 1
        cfg = result[0].percentile_config
        assert cfg is not None
        assert cfg.percentile == 50
        assert cfg.ignore_values == [0.0]

    def test_empty(self) -> None:
        """Empty inputs yield empty list."""
        result = retrieve_agg_parameters(
            simple_agg={},
            percentiles={},
            percentiles_ignore_vals={},
        )
        assert result == []


# process_agg_args

class TestProcessAggArgs:
    """Tests for the process_agg_args public API."""

    def test_basic_groupby_and_count(self) -> None:
        """groupby and count_ produce expected output."""
        result = process_agg_args(
            groupby=["region"],
            count_=["id"],
        )
        assert isinstance(result, ProcessedAggDef)
        assert result.groupby == [
            ("region", "region")
        ]
        assert len(result.ops) == 1
        assert result.ops[0].col == "id"
        assert (
            result.ops[0].op == AggOperationEnum.COUNT
        )

    def test_groupby_tuple_passthrough(self) -> None:
        """Tuple in groupby passes through unchanged."""
        result = process_agg_args(groupby=[("a", "b")])
        assert result.groupby == [("a", "b")]

    def test_all_none_gives_empty(self) -> None:
        """No arguments produce empty ProcessedAggDef."""
        result = process_agg_args()
        assert result.ops == []
        assert result.groupby == []
        assert result.cols_before == []
        assert result.cols_after == []

    def test_percentile_with_ignore(self) -> None:
        """percentile_ and ignore values wire through."""
        result = process_agg_args(
            percentile_={95: ["price"]},
            percentile_ignore_values_={
                "price": [-1.0]
            },
        )
        assert len(result.ops) == 1
        cfg = result.ops[0].percentile_config
        assert cfg is not None
        assert cfg.percentile == 95
        assert cfg.ignore_values == [-1.0]

    def test_cols_before_after_normalised(self) -> None:
        """cols_before/after are normalised to pairs."""
        result = process_agg_args(
            cols_before=[
                "UPPER(name)",
                ("raw", "clean"),
            ],
            cols_after=["extra"],
        )
        assert result.cols_before == [
            ("UPPER(name)", "UPPER(name)"),
            ("raw", "clean"),
        ]
        assert result.cols_after == [
            ("extra", "extra")
        ]


# RenderedAgg.from_agg_def

class TestRenderedAgg:
    """Tests for RenderedAgg.from_agg_def."""

    def test_ops_dispatched(self) -> None:
        """Each AggCol in ops is dispatched."""
        proc = ProcessedAggDef(
            groupby=[("g", "g")],
            cols_before=[],
            cols_after=[],
            ops=[
                AggCol(
                    col="x",
                    op=AggOperationEnum.SUM,
                )
            ],
        )
        rendered = RenderedAgg.from_agg_def(
            proc, ANSI._dispatch_agg
        )
        assert rendered.groupby == [("g", "g")]
        assert len(rendered.ops) == 1
        sql_code, alias = rendered.ops[0]
        assert "SUM(x)" in sql_code
        assert alias == "x__sum"


# _render_case_not_in

class TestRenderCaseNotIn:
    """Tests for _render_case_not_in."""

    def test_no_ignore(self) -> None:
        """Without ignore_values col is returned as-is."""
        result = ANSI._render_case_not_in("amount")
        assert result == "amount"

    def test_with_ignore(self) -> None:
        """With ignore_values a CASE WHEN is built."""
        result = ANSI._render_case_not_in(
            "amount",
            ignore_values=[0.0, -1.0],
        )
        assert "CASE WHEN" in result
        assert "NOT IN" in result
        assert "0.0" in result
        assert "-1.0" in result
        assert "amount" in result


# _render_percentile_function

class TestRenderPercentileFunction:
    """Tests for _render_percentile_function."""

    def test_default_no_adj(self) -> None:
        """Default factor passes percentile as-is."""
        result = ANSI._render_percentile_function(
            "col", 50
        )
        assert result == "approx_percentile(col, 50.0)"

    def test_adj_factor_100(self) -> None:
        """Factor 100 converts integer to fraction."""
        result = ANSI._render_percentile_function(
            "col", 95, perc_adj_factor=100.0
        )
        assert result == "approx_percentile(col, 0.95)"

    def test_custom_func(self) -> None:
        """Custom func name replaces approx_percentile."""
        result = ANSI._render_percentile_function(
            "col", 50, func="percentile_approx"
        )
        assert result.startswith("percentile_approx(")


# _render_agg_* functions

class TestRenderAggFunctions:
    """Tests for each _render_agg_* method."""

    def _col(
        self,
        name: str,
        op: AggOperationEnum = AggOperationEnum.COUNT,
    ) -> AggCol:
        """Build a minimal AggCol."""
        return AggCol(col=name, op=op)

    def test_count(self) -> None:
        """COUNT renders COUNT(col) and __count alias."""
        col = self._col("id")
        sql, alias = ANSI._render_agg_count(col)
        assert sql == "COUNT(id)"
        assert alias == "id__count"

    def test_count_null(self) -> None:
        """COUNT_NULL renders SUM(CASE WHEN IS NULL)."""
        col = self._col("x")
        sql, alias = ANSI._render_agg_count_null(col)
        assert "IS NULL" in sql
        assert "SUM(CASE" in sql
        assert alias == "x__count_null"

    def test_count_distinct(self) -> None:
        """COUNT_DISTINCT renders COUNT(DISTINCT col)."""
        col = self._col("u")
        sql, alias = (
            ANSI._render_agg_count_distinct(col)
        )
        assert sql == "COUNT(DISTINCT u)"
        assert alias == "u__count_distinct"

    def test_sum(self) -> None:
        """SUM renders SUM(col) and __sum alias."""
        col = self._col("amount")
        sql, alias = ANSI._render_agg_sum(col)
        assert sql == "SUM(amount)"
        assert alias == "amount__sum"

    def test_mean(self) -> None:
        """MEAN renders AVG(col) and __mean alias."""
        col = self._col("score")
        sql, alias = ANSI._render_agg_mean(col)
        assert sql == "AVG(score)"
        assert alias == "score__mean"

    def test_min(self) -> None:
        """MIN renders MIN(col) and __min alias."""
        col = self._col("dt")
        sql, alias = ANSI._render_agg_min(col)
        assert sql == "MIN(dt)"
        assert alias == "dt__min"

    def test_max(self) -> None:
        """MAX renders MAX(col) and __max alias."""
        col = self._col("dt")
        sql, alias = ANSI._render_agg_max(col)
        assert sql == "MAX(dt)"
        assert alias == "dt__max"


# _render_agg_percentile

class TestRenderAggPercentile:
    """Tests for _render_agg_percentile conversion."""

    def _perc_col(
        self,
        col: str,
        perc: int,
        ignore: Optional[List[float]] = None,
    ) -> AggCol:
        """Build a PERCENTILE AggCol."""
        return AggCol(
            col=col,
            op=AggOperationEnum.PERCENTILE,
            percentile_config=PercentileConfig(
                percentile=perc,
                ignore_values=ignore or [],
            ),
        )

    def test_ansi_divides_by_100(self) -> None:
        """ANSI converts integer 95 to fraction 0.95."""
        agg_col = self._perc_col("price", 95)
        sql, alias = ANSI._render_agg_percentile(
            agg_col
        )
        assert "0.95" in sql
        assert alias == "price__p95"

    def test_ansi_p50(self) -> None:
        """ANSI percentile 50 becomes 0.5."""
        agg_col = self._perc_col("score", 50)
        sql, _ = ANSI._render_agg_percentile(agg_col)
        assert "0.5" in sql

    def test_ansi_with_ignore_values(self) -> None:
        """ignore_values wraps col in CASE WHEN NOT IN."""
        agg_col = self._perc_col(
            "amount", 90, ignore=[0.0]
        )
        sql, alias = ANSI._render_agg_percentile(
            agg_col
        )
        assert "CASE WHEN" in sql
        assert "0.0" in sql
        assert "0.9" in sql
        assert alias == "amount__p90"

    def test_raises_without_config(self) -> None:
        """PERCENTILE without percentile_config raises."""
        agg_col = AggCol(
            col="x",
            op=AggOperationEnum.PERCENTILE,
        )
        with pytest.raises(
            ValueError,
            match="percentile_config",
        ):
            ANSI._render_agg_percentile(agg_col)


# _dispatch_agg

class TestDispatchAgg:
    """Tests for _dispatch_agg routing."""

    def _make(
        self,
        op: AggOperationEnum,
        col: str = "x",
        perc: Optional[int] = None,
    ) -> AggCol:
        """Build AggCol for dispatch tests."""
        cfg: Optional[PercentileConfig] = None
        if perc is not None:
            cfg = PercentileConfig(percentile=perc)
        return AggCol(
            col=col, op=op, percentile_config=cfg
        )

    @pytest.mark.parametrize(
        "op,expected_alias_suffix",
        [
            (AggOperationEnum.COUNT, "__count"),
            (
                AggOperationEnum.COUNT_NULL,
                "__count_null",
            ),
            (
                AggOperationEnum.COUNT_DISTINCT,
                "__count_distinct",
            ),
            (AggOperationEnum.SUM, "__sum"),
            (AggOperationEnum.MEAN, "__mean"),
            (AggOperationEnum.MIN, "__min"),
            (AggOperationEnum.MAX, "__max"),
        ],
    )
    def test_dispatch_simple_ops(
        self,
        op: AggOperationEnum,
        expected_alias_suffix: str,
    ) -> None:
        """Each op dispatches to correct render method."""
        agg_col = self._make(op)
        _, alias = ANSI._dispatch_agg(agg_col)
        assert alias.endswith(expected_alias_suffix)

    def test_dispatch_percentile(self) -> None:
        """PERCENTILE op returns expected alias."""
        agg_col = self._make(
            AggOperationEnum.PERCENTILE, perc=75
        )
        _, alias = ANSI._dispatch_agg(agg_col)
        assert alias == "x__p75"


# render_select

class TestRenderSelect:
    """Tests for render_select template."""

    def test_basic(self) -> None:
        """SELECT * FROM table_name is emitted."""
        sql = ANSI.render_select("my_schema.my_table")
        assert "SELECT *" in sql
        assert "my_schema.my_table" in sql

    def test_table_name_in_output(self) -> None:
        """Arbitrary table name appears verbatim."""
        sql = ANSI.render_select("db.schema.tbl")
        assert "db.schema.tbl" in sql


# render_sample

class TestRenderSample:
    """Tests for render_sample template."""

    def test_limit_appears(self) -> None:
        """Requested num_samples appears in LIMIT."""
        sql = ANSI.render_sample(
            "SELECT 1", num_samples=42
        )
        assert "LIMIT 42" in sql
        assert "SELECT 1" in sql

    def test_default_samples(self) -> None:
        """Default num_samples is 100."""
        sql = ANSI.render_sample("SELECT 1")
        assert "LIMIT 100" in sql


# render_get_duplicates

class TestRenderGetDuplicates:
    """Tests for render_get_duplicates template."""

    def test_structure(self) -> None:
        """Duplicate query counts rows and filters."""
        sql = ANSI.render_get_duplicates(
            input_query="SELECT * FROM t",
            keys=["id", "date"],
            join_on=(
                "base.id = counts.id"
                " AND base.date = counts.date"
            ),
        )
        w = _ws(sql)
        assert "COUNT(*)" in w
        assert "id" in w
        assert "date" in w
        assert "__n > 1" in w

    def test_keys_in_group_by(self) -> None:
        """Provided keys appear in GROUP BY."""
        sql = ANSI.render_get_duplicates(
            input_query="SELECT * FROM t",
            keys=["user_id"],
            join_on="base.user_id = counts.user_id",
        )
        assert "user_id" in _ws(sql)


# render_last_partitions

class TestRenderLastPartitions:
    """Tests for render_last_partitions template."""

    def test_structure(self) -> None:
        """Latest-partition query uses MAX and JOIN."""
        sql = ANSI.render_last_partitions(
            input_query="SELECT * FROM t",
            date_ingestion="ingested_at",
            columns=["region"],
            join_on=(
                "base.region = __latest.region"
                " AND base.ingested_at"
                " = __latest.ingested_at"
            ),
        )
        w = _ws(sql)
        assert "MAX(ingested_at)" in w
        assert "region" in w
        assert "INNER JOIN" in w

    def test_date_col_in_max(self) -> None:
        """The date_ingestion column appears in MAX()."""
        sql = ANSI.render_last_partitions(
            input_query="SELECT 1",
            date_ingestion="loaded_at",
            columns=["x"],
            join_on="base.x = __latest.x",
        )
        assert "MAX(loaded_at)" in sql


# render_enrich

class TestRenderEnrich:
    """Tests for render_enrich template."""

    def test_single_other(self) -> None:
        """Single other query produces one LEFT JOIN."""
        sql = ANSI.render_enrich(
            input_query="SELECT * FROM base",
            other_queries=["SELECT * FROM other"],
            join_ons=["base.id = __other_0.id"],
        )
        w = _ws(sql)
        assert "__base" in w
        assert "__other_0" in w
        assert "LEFT JOIN" in w

    def test_two_others(self) -> None:
        """Two other queries produce two CTEs."""
        sql = ANSI.render_enrich(
            input_query="SELECT 1 AS id",
            other_queries=[
                "SELECT 2 AS id",
                "SELECT 3 AS id",
            ],
            join_ons=[
                "base.id = __other_0.id",
                "base.id = __other_1.id",
            ],
        )
        assert "__other_0" in sql
        assert "__other_1" in sql


# render_jsonify (ANSI and Presto)

class TestRenderJsonifyAnsi:
    """Tests for ANSI render_jsonify."""

    def test_json_object_syntax(self) -> None:
        """ANSI uses JSON_OBJECT with VALUE keyword."""
        sql = ANSI.render_jsonify(
            input_query="SELECT * FROM t",
            keys=["id"],
            columns=["name", "val"],
        )
        w = _ws(sql)
        assert "JSON_OBJECT" in w
        assert "'name'" in w
        assert "'val'" in w
        assert "__json" in w
        assert "id" in w

    def test_multiple_columns(self) -> None:
        """Multiple columns all appear in output."""
        sql = ANSI.render_jsonify(
            input_query="SELECT 1",
            keys=[],
            columns=["a", "b", "c"],
        )
        w = _ws(sql)
        assert "'a'" in w
        assert "'b'" in w
        assert "'c'" in w


class TestRenderJsonifyPresto:
    """Tests for Presto render_jsonify."""

    def test_map_from_arrays_syntax(self) -> None:
        """Presto uses map_from_arrays with ARRAYs."""
        sql = PRESTO.render_jsonify(
            input_query="SELECT * FROM t",
            keys=["id"],
            columns=["name", "val"],
        )
        w = _ws(sql)
        assert "map_from_arrays" in w
        assert "ARRAY[" in w
        assert "'name'" in w
        assert "'val'" in w
        assert "__json" in w

    def test_uses_presto_template(self) -> None:
        """Template path is inside the presto dir."""
        path: Path = PRESTO._get_jsonify_template()
        assert "presto" in str(path)


# render_get_diffs

class TestRenderGetDiffs:
    """Tests for render_get_diffs template."""

    def test_string_keys(self) -> None:
        """String keys are normalised to identity."""
        sql = ANSI.render_get_diffs(
            input_query="SELECT * FROM left",
            other_query="SELECT * FROM right",
            join_on=["id"],
            compare_cols=["amount"],
        )
        w = _ws(sql)
        assert "FULL OUTER JOIN" in w
        assert "COALESCE" in w
        assert "IS DISTINCT FROM" in w
        assert "amount__left" in w
        assert "amount__right" in w

    def test_tuple_keys(self) -> None:
        """Explicit (left, right) tuples are used."""
        sql = ANSI.render_get_diffs(
            input_query="SELECT * FROM l",
            other_query="SELECT * FROM r",
            join_on=[("lid", "rid")],
            compare_cols=[("lval", "rval")],
        )
        assert "lid" in sql
        assert "rid" in sql
        assert "lval__left" in sql
        assert "rval__right" in sql

    def test_mixed_keys(self) -> None:
        """Mix of str and tuple keys is handled."""
        sql = ANSI.render_get_diffs(
            input_query="SELECT 1",
            other_query="SELECT 1",
            join_on=["id", ("left_dt", "right_dt")],
            compare_cols=["val"],
        )
        assert "id" in sql
        assert "left_dt" in sql
        assert "right_dt" in sql


# render_agg

class TestRenderAgg:
    """End-to-end tests for render_agg."""

    def test_count_with_groupby(self) -> None:
        """GROUP BY and COUNT produce correct SQL."""
        proc = process_agg_args(
            groupby=["region"],
            count_=["id"],
        )
        sql = ANSI.render_agg(
            proc, "SELECT * FROM t"
        )
        w = _ws(sql)
        assert "COUNT(id)" in w
        assert "id__count" in w
        assert "region" in w
        assert "GROUP BY" in w
        assert "count_rows_" in w

    def test_multiple_ops(self) -> None:
        """Multiple agg ops all appear in output."""
        proc = process_agg_args(
            sum_=["amount"],
            mean_=["score"],
        )
        sql = ANSI.render_agg(
            proc, "SELECT * FROM t"
        )
        w = _ws(sql)
        assert "SUM(amount)" in w
        assert "AVG(score)" in w

    def test_percentile(self) -> None:
        """Percentile renders fraction and alias."""
        proc = process_agg_args(
            percentile_={95: ["price"]}
        )
        sql = ANSI.render_agg(
            proc, "SELECT * FROM t"
        )
        assert "0.95" in sql
        assert "price__p95" in sql

    def test_cols_before_after(self) -> None:
        """cols_before and cols_after appear."""
        proc = process_agg_args(
            count_=["id"],
            cols_before=[
                ("UPPER(name)", "name_upper")
            ],
            cols_after=[
                ("SUM(x)/SUM(y)", "ratio")
            ],
        )
        sql = ANSI.render_agg(
            proc, "SELECT * FROM t"
        )
        assert "name_upper" in sql
        assert "ratio" in sql


# render_build_op (ANSI)

class TestRenderBuildOpAnsi:
    """Tests for AnsiDialectDef.render_build_op."""

    def test_star_when_all_none(self) -> None:
        """No arguments produce SELECT * from CTE."""
        sql = ANSI.render_build_op(
            "SELECT * FROM t",
            add=None, rename=None,
            select_only=None, exclude=None,
        )
        w = _ws(sql)
        assert (
            "SELECT\n    *" in sql
            or "SELECT *" in w
        )

    def test_add_column(self) -> None:
        """add dict appends expression AS alias."""
        sql = ANSI.render_build_op(
            "SELECT * FROM t",
            add={"new_col": "old_col * 2"},
            rename=None,
            select_only=None,
            exclude=None,
        )
        assert "old_col * 2 AS new_col" in sql

    def test_rename_column(self) -> None:
        """rename dict emits old AS new."""
        sql = ANSI.render_build_op(
            "SELECT * FROM t",
            add=None,
            rename={"new_name": "old_name"},
            select_only=None,
            exclude=None,
        )
        assert "old_name AS new_name" in sql

    def test_select_only(self) -> None:
        """select_only lists only requested columns."""
        sql = ANSI.render_build_op(
            "SELECT 1",
            add=None, rename=None,
            select_only=["id", "name"],
            exclude=None,
        )
        w = _ws(sql)
        assert "id" in w
        assert "name" in w
        after = w.split("FROM __base")[0]
        outer = after.rsplit("SELECT", 1)[-1]
        assert "*" not in outer

    def test_select_only_with_rename(self) -> None:
        """select_only respects rename for listed cols."""
        sql = ANSI.render_build_op(
            "SELECT * FROM t",
            add=None,
            rename={"new_name": "old_name"},
            select_only=["new_name"],
            exclude=None,
        )
        assert "old_name AS new_name" in sql

    def test_exclude_alone_raises(self) -> None:
        """exclude without select_only raises ValueError."""
        with pytest.raises(
            ValueError, match="ANSI"
        ):
            ANSI.render_build_op(
                "SELECT * FROM t",
                add=None, rename=None,
                select_only=None,
                exclude=["unwanted"],
            )


# _build_op_select  (ANSI vs Presto)

class TestBuildOpSelectAnsi:
    """Unit tests for AnsiDialectDef._build_op_select."""

    def test_no_args_returns_star(self) -> None:
        """All-None args yield bare '*'."""
        result = ANSI._build_op_select(
            None, None, None, None
        )
        assert result == "*"

    def test_add_appends_to_star(self) -> None:
        """add dict produces '*, expr AS alias'."""
        result = ANSI._build_op_select(
            add={"new": "expr"},
            rename=None,
            select_only=None,
            exclude=None,
        )
        assert result == "*, expr AS new"

    def test_exclude_raises(self) -> None:
        """exclude without select_only raises."""
        with pytest.raises(ValueError):
            ANSI._build_op_select(
                add=None, rename=None,
                select_only=None,
                exclude=["col"],
            )


class TestBuildOpSelectPresto:
    """Unit tests for PrestoDialectDef._build_op_select."""

    def test_no_args_returns_star(self) -> None:
        """All-None args yield bare '*'."""
        result = PRESTO._build_op_select(
            None, None, None, None
        )
        assert result == "*"

    def test_exclude_uses_except(self) -> None:
        """exclude produces '* EXCEPT (col)'."""
        result = PRESTO._build_op_select(
            add=None, rename=None,
            select_only=None,
            exclude=["secret"],
        )
        assert "* EXCEPT" in result
        assert "secret" in result

    def test_exclude_with_rename(self) -> None:
        """exclude + rename emits EXCEPT and rename."""
        result = PRESTO._build_op_select(
            add=None,
            rename={"new": "old"},
            select_only=None,
            exclude=["other"],
        )
        assert "* EXCEPT" in result
        assert "old AS new" in result

    def test_select_only_no_except(self) -> None:
        """select_only does not use EXCEPT."""
        result = PRESTO._build_op_select(
            add=None, rename=None,
            select_only=["a", "b"],
            exclude=None,
        )
        assert "EXCEPT" not in result
        assert "a" in result
        assert "b" in result
