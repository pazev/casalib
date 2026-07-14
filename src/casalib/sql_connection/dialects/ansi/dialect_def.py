"""AnsiDialectDef — DialectDefinition for ANSI SQL."""
from pathlib import Path
from typing import Dict, List, Optional, Tuple

from ..dialect_def import DialectDefinition
from ..helpers import AggCol

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

    # Generators for some SQL common functions
    # ========================================
    def _render_case_not_in(
        self,
        col: str,
        ignore_values: Optional[List[float]] = None,
    ) -> str:
        """ Render how the case when is rendered """
        noign = self._from_string('{{col}}')
        ign = self._from_string(
            '''CASE WHEN {{col}} NOT IN'''
            ''' ({{ign | join(', ')}})'''
            ''' THEN {{col}} END'''
        )
        template_to_use = (
            ign if ignore_values else noign
        )
        return template_to_use.render(
            col=col, ign=ignore_values,
        )

    def _render_agg_percentile(self, col: str, perc: int) -> str:
        """Render the percentile function call."""
        temp = self._from_string(
            'approx_percentile({{col}}, '
            '{{perc_adj}})'
        )
        return temp.render(
            col=col,
            perc_adj=perc / 100,
        )

    def _render_agg_count(self, col: AggCol) -> Tuple[str, str]:
        """Render COUNT."""
        return (
            f"COUNT({col.col})",
            f"{col.col}__count",
        )

    def _render_agg_count_null(self, col: AggCol) -> Tuple[str, str]:
        """Render count of NULLs via SUM(CASE ...)."""
        return (
            f"SUM(CASE WHEN {col.col}"
            f" IS NULL THEN 1 ELSE 0 END)",
            f"{col.col}__count_null",
        )

    def _render_agg_count_distinct(self, col: AggCol) -> Tuple[str, str]:
        """Render COUNT DISTINCT."""
        return (
            f"COUNT(DISTINCT {col.col})",
            f"{col.col}__count_distinct",
        )

    def _render_agg_sum(self, col: AggCol) -> Tuple[str, str]:
        """Render SUM."""
        return (
            f"SUM({col.col})",
            f"{col.col}__sum",
        )

    def _render_agg_mean(self, col: AggCol) -> Tuple[str, str]:
        """Render AVG."""
        return (
            f"AVG({col.col})",
            f"{col.col}__mean",
        )

    def _render_agg_min(self, col: AggCol) -> Tuple[str, str]:
        """Render MIN."""
        return (
            f"MIN({col.col})",
            f"{col.col}__min",
        )

    def _render_agg_max(self, col: AggCol) -> Tuple[str, str]:
        """Render MAX."""
        return (
            f"MAX({col.col})",
            f"{col.col}__max",
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
            _TEMPLATE_FLD / "op.sql"
        ).render(
            input_query=input_query,
            select_list=select_list,
        )
