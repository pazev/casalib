"""PrestoDialect — AnsiDialect subclass for Presto/Trino SQL.

Overrides only the methods that require Presto-specific
parameter treatment:
- op: adds SELECT * EXCEPT support for the exclude parameter.

jsonify rendering is handled by PrestoDialectDef via _dialect_def.
"""
from dataclasses import dataclass
from typing import (
    ClassVar,
    Dict,
    List,
    Optional,
)

from ..ansi.dialect import AnsiDialect
from .dialect_def import PrestoDialectDef


def _op_select(
    add: Optional[Dict[str, str]],
    rename: Optional[Dict[str, str]],
    select_only: Optional[List[str]],
    exclude: Optional[List[str]],
) -> str:
    """Build the SELECT expression for op,
    including SELECT * EXCEPT support.

    Requires Presto Engine v3 / Trino when
    exclude is used without select_only.
    """
    add = add or {}
    rename = rename or {}

    if select_only:
        parts: List[str] = []
        for col in select_only:
            if col in rename:
                old = rename[col]
                parts.append(f"{old} AS {col}")
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


@dataclass
class PrestoDialect(AnsiDialect):
    """AnsiDialect subclass for Presto/Trino SQL.

    Overrides op to support SELECT * EXCEPT.
    jsonify uses map_from_arrays via PrestoDialectDef.

    Note:
        op with exclude (and no select_only) requires
        Presto 0.217+ or Trino.
    """

    _dialect_def: ClassVar[PrestoDialectDef] = (  # type: ignore[assignment]
        PrestoDialectDef()
    )

    def op(
        self,
        add: Optional[Dict[str, str]] = None,
        rename: Optional[Dict[str, str]] = None,
        select_only: Optional[List[str]] = None,
        exclude: Optional[List[str]] = None,
    ) -> str:
        select_list = _op_select(
            add=add,
            rename=rename,
            select_only=select_only,
            exclude=exclude,
        )
        return self._dialect_def.render_op(
            self.input_query, select_list
        )
