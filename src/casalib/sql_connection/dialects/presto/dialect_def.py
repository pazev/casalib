"""PrestoDialectDef — DialectDefinition for Presto/Trino."""
from pathlib import Path
from typing import Dict, List, Optional

from ..ansi.dialect_def import AnsiDialectDef

_TEMPLATE_FLD = Path(__file__).parent / "templates"


class PrestoDialectDef(AnsiDialectDef):
    """Presto/Trino dialect rendering.

    Inherits all rendering from AnsiDialectDef.
    Overrides _build_op_select to support SELECT * EXCEPT
    and _get_jsonify_template to use map_from_arrays with
    ARRAY literals.
    """

    def _get_jsonify_template(self) -> Path:
        return _TEMPLATE_FLD / "jsonify.sql"

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

        excluded: List[str] = (
            list(rename.values()) + list(exclude or [])
        )
        extras: List[str] = [
            f"{old} AS {new}"
            for new, old in rename.items()
        ] + [
            f"{expr} AS {new}"
            for new, expr in add.items()
        ]

        if not excluded and not extras:
            return "*"
        if not excluded:
            return "*, " + ", ".join(extras)
        excl_str = ", ".join(excluded)
        if not extras:
            return f"* EXCEPT ({excl_str})"
        return (
            f"* EXCEPT ({excl_str}),\n    "
            + ",\n    ".join(extras)
        )
