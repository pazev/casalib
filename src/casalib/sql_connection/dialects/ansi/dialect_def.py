"""AnsiDialectDef — DialectDefinition for ANSI SQL."""
from pathlib import Path
from typing import Dict, List, Optional

from ..dialect_def import DialectDefinition
from ..helpers import AggCol, RenderedCol

_TEMPLATE_FLD = Path(__file__).parent / "templates"


class AnsiDialectDef(DialectDefinition):
    """ANSI dialect rendering.

    Implements all SQL method renderers using ANSI-compliant
    templates. jsonify uses the SQL:2016 JSON_OBJECT syntax.
    Override any render_* method in a subclass for
    engine-specific syntax.
    """

    # Template paths
    # ==============
    def _get_select_template(self) -> Path:
        return _TEMPLATE_FLD / "select.sql"

    def _get_agg_template(self) -> Path:
        return _TEMPLATE_FLD / "agg.sql"

    def _get_sample_template(self) -> Path:
        return _TEMPLATE_FLD / "sample.sql"

    def _get_get_duplicates_template(self) -> Path:
        return _TEMPLATE_FLD / "get_duplicates.sql"

    def _get_last_partition_template(self) -> Path:
        return _TEMPLATE_FLD / "last_partitions.sql"

    def _get_enrich_template(self) -> Path:
        return _TEMPLATE_FLD / "enrich.sql"

    def _get_jsonify_template(self) -> Path:
        return _TEMPLATE_FLD / "jsonify.sql"

    def _get_missing_keys_template(self) -> Path:
        return _TEMPLATE_FLD / "missing_keys.sql"

    def _get_get_diffs_template(self) -> Path:
        return _TEMPLATE_FLD / "get_diffs.sql"

    def _get_build_op_template(self) -> Path:
        return _TEMPLATE_FLD / "op.sql"

    # Agg primitive renderers
    # =======================
    def _render_case_not_in_float(
        self,
        col: str,
        ignore_values: Optional[List[float]] = None,
    ) -> str:
        """Wrap col in CASE WHEN NOT IN if needed."""
        if ignore_values is None:
            return col

        ign = self._from_string(
            '''CASE WHEN {{col}} NOT IN'''
            ''' ({{ign | join(', ')}})'''
            ''' THEN {{col}} END'''
        )
        return ign.render(
            col=col, ign=ignore_values,
        )

    def _render_agg_percentile_sql_code(
        self, col: str, sql_code: str, perc: int,
    ) -> RenderedCol:
        """Render the percentile function call."""
        temp = self._from_string(
            'approx_percentile({{sql_code}}, {{perc_adj}})'
        )
        sql_code_rendered = temp.render(
            sql_code=sql_code, perc_adj=perc / 100.0,
        )
        return RenderedCol(
            sql_code=sql_code_rendered,
            alias=f'{col}__p{perc}',
        )

    # Agg renderers
    # =============
    def _render_agg_count(
        self, col: AggCol
    ) -> RenderedCol:
        """Render COUNT."""
        return RenderedCol(
            sql_code=f"COUNT({col.col})",
            alias=f"{col.col}__count",
        )

    def _render_agg_count_null(
        self, col: AggCol
    ) -> RenderedCol:
        """Render count of NULLs via SUM(CASE ...)."""
        return RenderedCol(
            sql_code=(
                f"SUM(CASE WHEN {col.col}"
                f" IS NULL THEN 1 ELSE 0 END)"
            ),
            alias=f"{col.col}__count_null",
        )

    def _render_agg_count_distinct(
        self, col: AggCol
    ) -> RenderedCol:
        """Render COUNT DISTINCT."""
        return RenderedCol(
            sql_code=f"COUNT(DISTINCT {col.col})",
            alias=f"{col.col}__count_distinct",
        )

    def _render_agg_sum(
        self, col: AggCol
    ) -> RenderedCol:
        """Render SUM."""
        return RenderedCol(
            sql_code=f"SUM({col.col})",
            alias=f"{col.col}__sum",
        )

    def _render_agg_mean(
        self, col: AggCol
    ) -> RenderedCol:
        """Render AVG."""
        return RenderedCol(
            sql_code=f"AVG({col.col})",
            alias=f"{col.col}__mean",
        )

    def _render_agg_min(
        self, col: AggCol
    ) -> RenderedCol:
        """Render MIN."""
        return RenderedCol(
            sql_code=f"MIN({col.col})",
            alias=f"{col.col}__min",
        )

    def _render_agg_max(
        self, col: AggCol
    ) -> RenderedCol:
        """Render MAX."""
        return RenderedCol(
            sql_code=f"MAX({col.col})",
            alias=f"{col.col}__max",
        )

    # ANSI-specific select-list building for op
    # ==========================================
    def _build_op_select(
        self,
        add: Optional[Dict[str, str]],
        rename: Optional[Dict[str, str]],
        select_only: Optional[List[str]],
        exclude: Optional[List[str]],
    ) -> str:
        add = add or {}
        rename = rename or {}

        if select_only:
            parts: List[str] = []
            for col in select_only:
                if col in rename:
                    parts.append(
                        f"{rename[col]} AS {col}"
                    )
                elif col in add:
                    parts.append(
                        f"{add[col]} AS {col}"
                    )
                else:
                    parts.append(col)
            return ",\n    ".join(parts)

        extras: List[str] = [
            f"{old} AS {new}"
            for new, old in rename.items()
        ] + [
            f"{expr} AS {new}"
            for new, expr in add.items()
        ]

        if exclude:
            raise ValueError(
                "exclude without select_only"
                " requires SELECT * EXCEPT,"
                " which is not ANSI SQL. Use a"
                " dialect that supports it"
                " (e.g. PrestoDialect)."
            )

        if not extras:
            return "*"
        return "*, " + ", ".join(extras)

    def render_build_op(  # pylint: disable=too-many-arguments
        self,
        input_query: str,
        add: Optional[Dict[str, str]],
        rename: Optional[Dict[str, str]],
        select_only: Optional[List[str]],
        exclude: Optional[List[str]],
    ) -> str:
        select_list = self._build_op_select(
            add, rename, select_only, exclude
        )
        return self._load_template(
            self._get_build_op_template()
        ).render(
            input_query=input_query,
            select_list=select_list,
        )
