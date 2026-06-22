"""AnsiDialectDef — DialectDefinition for ANSI SQL."""
from pathlib import Path
from typing import List, Tuple

from .._dialect_def import DialectDefinition, RenderedAggDict
from .._helpers import AggCol, ProcessedAggDict

_TEMPLATE_FLD = Path(__file__).parent / "templates"


class AnsiDialectDef(DialectDefinition):
    """ANSI dialect rendering.

    Implements all SQL method renderers using ANSI-compliant
    templates. jsonify uses the SQL:2016 JSON_OBJECT syntax.
    Override any render_* method in a subclass for
    engine-specific syntax.
    """

    def render_percentile(
        self, agg_col: AggCol
    ) -> Tuple[str, str]:
        cfg = agg_col.percentile_config
        assert cfg is not None
        col = agg_col.col
        pct = cfg.percentile / 100.0
        if cfg.ignore_values:
            vals = ", ".join(
                str(v) for v in cfg.ignore_values
            )
            inner = (
                f"CASE WHEN {col} NOT IN ({vals})"
                f" THEN {col} ELSE NULL END"
            )
        else:
            inner = col
        return (
            f"approx_percentile({inner}, {pct})",
            f"{col}__p{cfg.percentile}",
        )

    def render_select(self, table_name: str) -> str:
        return self._load_template(
            _TEMPLATE_FLD / "select.sql"
        ).render(table_name=table_name)

    def render_agg(
        self,
        processed: ProcessedAggDict,
        input_query: str,
    ) -> str:
        agg_def = RenderedAggDict(
            groupby=processed['groupby'],
            cols_before=processed['cols_before'],
            cols_after=processed['cols_after'],
            ops=[
                self._dispatch_agg(ac)
                for ac in processed['ops']
            ],
        )
        return self._load_template(
            _TEMPLATE_FLD / "agg.sql"
        ).render(input_query=input_query, agg_def=agg_def)

    def render_sample(
        self, input_query: str, num_samples: int
    ) -> str:
        return self._load_template(
            _TEMPLATE_FLD / "sample.sql"
        ).render(
            input_query=input_query,
            num_samples=num_samples,
        )

    def render_get_duplicates(
        self,
        input_query: str,
        keys: List[str],
        join_on: str,
    ) -> str:
        return self._load_template(
            _TEMPLATE_FLD / "get_duplicates.sql"
        ).render(
            input_query=input_query,
            keys=keys,
            join_on=join_on,
        )

    def render_last_partitions(
        self,
        input_query: str,
        date_ingestion: str,
        columns: List[str],
        join_on: str,
    ) -> str:
        return self._load_template(
            _TEMPLATE_FLD / "last_partitions.sql"
        ).render(
            input_query=input_query,
            date_ingestion=date_ingestion,
            columns=columns,
            join_on=join_on,
        )

    def render_enrich(
        self,
        input_query: str,
        others: List[str],
        join_ons: List[str],
    ) -> str:
        return self._load_template(
            _TEMPLATE_FLD / "enrich.sql"
        ).render(
            input_query=input_query,
            others=others,
            join_ons=join_ons,
        )

    def render_get_diffs(
        self,
        input_query: str,
        other: str,
        join_on: str,
        key_cols: str,
        diff_cols: str,
        diff_where: str,
    ) -> str:
        return self._load_template(
            _TEMPLATE_FLD / "get_diffs.sql"
        ).render(
            input_query=input_query,
            other=other,
            join_on=join_on,
            key_cols=key_cols,
            diff_cols=diff_cols,
            diff_where=diff_where,
        )

    def render_op(
        self, input_query: str, select_list: str
    ) -> str:
        return self._load_template(
            _TEMPLATE_FLD / "op.sql"
        ).render(
            input_query=input_query,
            select_list=select_list,
        )

    def render_jsonify(
        self,
        input_query: str,
        keys: List[str],
        columns: List[str],
    ) -> str:
        json_args = ",\n        ".join(
            f"'{col}' VALUE CAST({col} AS VARCHAR)"
            for col in columns
        )
        return self._load_template(
            _TEMPLATE_FLD / "jsonify.sql"
        ).render(
            input_query=input_query,
            keys=keys,
            json_args=json_args,
        )
