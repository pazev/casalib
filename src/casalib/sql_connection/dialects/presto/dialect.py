"""PrestoDialect — AnsiDialect subclass for
Presto/Trino SQL.

Overrides only the methods that require
Presto-specific SQL:
- ``op``: adds ``SELECT * EXCEPT`` support for
  the ``exclude`` parameter.
- ``jsonify``: uses ``map_from_arrays`` and
  ``ARRAY`` literals.
"""
from dataclasses import dataclass
from pathlib import Path
from typing import (
    ClassVar,
    Dict,
    List,
    Optional,
)

from ..ansi.dialect import AnsiDialect
from .._dialect_def import DialectDefinition
from .._render import make_template_render
from .dialect_def import PrestoDialectDef

_ANSI_TEMPLATES = (
    Path(__file__).parent.parent / "ansi" / "templates"
)
_TEMPLATES = Path(__file__).parent / "templates"
_ansi_render = make_template_render(_ANSI_TEMPLATES)
_render = make_template_render(_TEMPLATES)


def _op_select(
    add: Optional[Dict[str, str]],
    rename: Optional[Dict[str, str]],
    select_only: Optional[List[str]],
    exclude: Optional[List[str]],
) -> str:
    """Build the SELECT expression for op,
    including ``SELECT * EXCEPT`` support.

    Requires Presto Engine v3 / Trino when
    ``exclude`` is used without ``select_only``.

    Args:
        add: ``{new_col: sql_expression}``.
        rename: ``{new_name: old_name}``.
        select_only: Final column names to keep.
        exclude: Final column names to drop.

    Returns:
        SQL SELECT expression string.
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

    # No select_only — use SELECT * EXCEPT
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
    _dialect_def: ClassVar[DialectDefinition] = PrestoDialectDef()

    """AnsiDialect implementation for
    Presto/Trino SQL.

    Overrides ``op`` to support
    ``SELECT * EXCEPT`` and ``jsonify`` to use
    ``map_from_arrays`` with ``ARRAY`` literals.

    Note:
        ``op`` with ``exclude`` (and no
        ``select_only``) requires Presto 0.217+
        or Trino.
    """

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
        return _ansi_render(
            "op.sql",
            input_query=self.input_query,
            select_list=select_list,
        )

    def jsonify(
        self,
        keys: List[str],
        columns: List[str],
    ) -> str:
        col_names = ", ".join(
            f"'{c}'" for c in columns
        )
        col_values = ", ".join(
            f"CAST({c} AS VARCHAR)"
            for c in columns
        )
        return _render(
            "jsonify.sql",
            input_query=self.input_query,
            keys=keys,
            col_names=col_names,
            col_values=col_values,
        )
