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

    def _render_agg_percentile(
        self,
        agg_col: AggCol,
        func: str = 'approx_percentile',
        perc_adj_factor: float = 100.0,
    ) -> Tuple[str, str]:
        return super()._render_agg_percentile(
            agg_col, func, perc_adj_factor
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
