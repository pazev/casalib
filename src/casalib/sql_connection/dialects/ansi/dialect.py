"""AnsiDialect — SqlDialectAbstract
implementation for standard ANSI SQL.

All templates use only ANSI-compliant SQL
constructs. ``jsonify`` is engine-specific and
left abstract for concrete subclasses.
"""
from dataclasses import dataclass
from pathlib import Path
from typing import (
    ClassVar,
    Dict,
    List,
    Optional,
    Sequence,
    Tuple,
    Union,
)

from ...sql_dialect_abstract import SqlDialectAbstract
from .._dialect_def import DialectDefinition, render_agg_def
from .._helpers import normalise_cols, process_agg_args
from .._render import make_template_render
from .dialect_def import AnsiDialectDef


# ------------------
# Load dialect info
# ------------------
_TEMPLATES = Path(__file__).parent / "templates"
_render = make_template_render(_TEMPLATES)


# ----------------------------------
# Private helpers
# ----------------------------------
def _join_on(
    keys: Sequence[Union[str, Tuple[str, str]]],
    left: str = "l",
    right: str = "r",
) -> str:
    """Build a SQL join ON clause string.

    Args:
        keys: Column pairs defining the join.
        left: Alias for the left table.
        right: Alias for the right table.

    Returns:
        SQL string like
        ``l.a = r.a AND l.b = r.b``.
    """
    pairs = normalise_cols(keys)
    return " AND ".join(
        f"{left}.{lk} = {right}.{rk}"
        for lk, rk in pairs
    )


def _op_select(
    add: Optional[Dict[str, str]],
    rename: Optional[Dict[str, str]],
    select_only: Optional[List[str]],
    exclude: Optional[List[str]],
) -> str:
    """Build the SELECT expression for op.

    Args:
        add: ``{new_col: sql_expression}``.
        rename: ``{new_name: old_name}``.
        select_only: Final column names to keep.
        exclude: Not supported without
            ``select_only`` in ANSI SQL.

    Returns:
        SQL SELECT expression string.

    Raises:
        ValueError: If ``exclude`` is provided
            without ``select_only``. Dropping
            columns by name without an explicit
            column list requires
            ``SELECT * EXCEPT``, which is not
            ANSI SQL.
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

    # No select_only
    extras: List[str] = [
        f"{old} AS {new}"
        for new, old in rename.items()
    ] + [
        f"{expr} AS {new}"
        for new, expr in add.items()
    ]

    if exclude:
        raise ValueError(
            "exclude without select_only requires"
            " SELECT * EXCEPT, which is not ANSI"
            " SQL. Use a dialect that supports it"
            " (e.g. PrestoDialect)."
        )

    if not extras:
        return "*"
    return "*, " + ", ".join(extras)


# ----------------------------------
# Dialect
# ----------------------------------

@dataclass
class AnsiDialect(SqlDialectAbstract):
    """SqlDialectAbstract implementation for
    ANSI SQL.

    Implements all methods whose SQL output is
    standard ANSI-compliant. ``jsonify`` is
    engine-specific and left abstract.

    ``op`` with ``exclude`` (and no
    ``select_only``) raises ``ValueError``
    because it requires ``SELECT * EXCEPT``,
    which is not ANSI SQL.
    """

    _dialect_def: ClassVar[DialectDefinition] = AnsiDialectDef()

    def select(self, table_name: str) -> str:
        return _render(
            "select.sql",
            table_name=table_name,
        )

    def agg(  # pylint: disable=too-many-arguments
        self,
        groupby: Optional[List[str]] = None,
        *,
        count_: Optional[List[str]] = None,
        count_null_: Optional[List[str]] = None,
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
    ) -> str:
        processed = process_agg_args(
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
        agg_def = render_agg_def(
            processed, self._dialect_def
        )
        return _render(
            "agg.sql",
            input_query=self.input_query,
            agg_def=agg_def,
        )

    def get_duplicates(
        self, keys: List[str]
    ) -> str:
        join_on = _join_on(
            keys, left="base", right="counts"
        )
        return _render(
            "get_duplicates.sql",
            input_query=self.input_query,
            keys=keys,
            join_on=join_on,
        )

    def sample(self, num_samples: int) -> str:
        return _render(
            "sample.sql",
            input_query=self.input_query,
            num_samples=num_samples,
        )

    def last_partitions(
        self,
        date_ingestion: str,
        columns: List[str],
    ) -> str:
        join_on = _join_on(
            columns,
            left="base",
            right="__latest",
        )
        return _render(
            "last_partitions.sql",
            input_query=self.input_query,
            date_ingestion=date_ingestion,
            columns=columns,
            join_on=join_on,
        )

    def enrich(
        self,
        other: Union[str, List[str]],
        keys: List[Union[str, Tuple[str, str]]],
        prefix: Optional[
            Union[str, List[str]]
        ] = None,
    ) -> str:
        others = (
            [other] if isinstance(other, str)
            else other
        )
        join_ons = [
            _join_on(
                keys,
                left="base",
                right=f"__other_{i}",
            )
            for i in range(len(others))
        ]
        return _render(
            "enrich.sql",
            input_query=self.input_query,
            others=others,
            join_ons=join_ons,
        )

    def get_diffs(
        self,
        other: str,
        keys: List[Union[str, Tuple[str, str]]],
        columns: List[
            Union[str, Tuple[str, str]]
        ],
    ) -> str:
        norm_keys = normalise_cols(keys)
        norm_cols = normalise_cols(columns)
        join_on = _join_on(
            keys, left="l", right="r"
        )
        key_cols = ",\n    ".join(
            f"COALESCE(l.{lk}, r.{rk}) AS {lk}"
            for lk, rk in norm_keys
        )
        diff_cols = ",\n    ".join(
            f"l.{lc} AS {lc}__left,"
            f"\n    r.{rc} AS {rc}__right"
            for lc, rc in norm_cols
        )
        diff_where = "\n    OR ".join(
            f"(l.{lc} IS DISTINCT FROM r.{rc})"
            for lc, rc in norm_cols
        )
        return _render(
            "get_diffs.sql",
            input_query=self.input_query,
            other=other,
            join_on=join_on,
            key_cols=key_cols,
            diff_cols=diff_cols,
            diff_where=diff_where,
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
        return _render(
            "op.sql",
            input_query=self.input_query,
            select_list=select_list,
        )

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
