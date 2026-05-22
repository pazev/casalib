"""AnsiDialect — SqlDialectAbstract
implementation for standard ANSI SQL.

All templates use only ANSI-compliant SQL
constructs. ``jsonify`` is engine-specific and
left abstract for concrete subclasses.
"""
from dataclasses import dataclass
from pathlib import Path
from typing import (
    Dict,
    List,
    Optional,
    Sequence,
    Tuple,
    Union,
)

from ...sql_dialect_abstract import (
    SqlDialectAbstract,
)
from .._helpers import (
    normalise_cols,
    agg_select as agg_select_new
)
from .._render import make_template_render


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


def _col_expr(
    item: Union[str, Tuple[str, str]],
) -> str:
    """Render a column or (expr, alias) tuple
    as a SQL SELECT expression.

    Args:
        item: Column name or
            (expression, alias) tuple.

    Returns:
        SQL expression string.
    """
    if isinstance(item, tuple):
        expr, alias = item
        return f"{expr} AS {alias}"
    return item


_SIMPLE_AGGS: List[Tuple[str, str]] = [
    ("COUNT({})", "__count"),
    (
        "SUM(CASE WHEN {} IS NULL"
        " THEN 1 ELSE 0 END)",
        "__count_null",
    ),
    ("COUNT(DISTINCT {})", "__count_distinct"),
    ("SUM({})", "__sum"),
    ("AVG({})", "__mean"),
    ("MIN({})", "__min"),
    ("MAX({})", "__max"),
]


def _agg_select(
    groupby: Optional[List[str]] = None,
    *,
    count_: Optional[List[str]] = None,
    count_null_: Optional[List[str]] = None,
    count_distinct_: Optional[List[str]] = None,
    sum_: Optional[List[str]] = None,
    mean_: Optional[List[str]] = None,
    min_: Optional[List[str]] = None,
    max_: Optional[List[str]] = None,
    percentile_: Optional[Dict[int, List[str]]] = None,
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
    # pylint: disable=too-many-arguments
    # pylint: disable=too-many-locals
    """Build the SELECT expression list for agg.

    Args:
        groupby: Columns to GROUP BY.
        count_: Columns to COUNT (non-null).
        count_null_: Columns to count NULLs.
        count_distinct_: Columns to COUNT
            DISTINCT.
        sum_: Columns to SUM.
        mean_: Columns to AVG.
        min_: Columns to MIN.
        max_: Columns to MAX.
        percentile_: Percentile → columns map.
            Uses ``approx_percentile``, which
            is Presto/Trino-specific.
        percentile_ignore_values_: Column →
            values to exclude map.
        cols_before: Expressions prepended to
            SELECT.
        cols_after: Expressions appended to
            SELECT.

    Returns:
        Comma-separated SELECT expression
        string.
    """
    parts: List[str] = []
    ignore = percentile_ignore_values_ or {}

    for item in cols_before or []:
        parts.append(_col_expr(item))
    for col in groupby or []:
        parts.append(col)

    col_lists: List[Optional[List[str]]] = [
        count_, count_null_, count_distinct_,
        sum_, mean_, min_, max_,
    ]
    for (tpl, suffix), cols in zip(
        _SIMPLE_AGGS, col_lists
    ):
        for col in cols or []:
            parts.append(
                f"{tpl.format(col)} AS {col}{suffix}"
            )

    for p, cols in (percentile_ or {}).items():
        pct = p / 100.0
        for col in cols:
            excl = ignore.get(col, [])
            if excl:
                vals = ", ".join(
                    str(v) for v in excl
                )
                inner = (
                    f"CASE WHEN {col}"
                    f" NOT IN ({vals})"
                    f" THEN {col}"
                    f" ELSE NULL END"
                )
            else:
                inner = col
            parts.append(
                f"approx_percentile"
                f"({inner}, {pct})"
                f" AS {col}__p{p}"
            )
    for item in cols_after or []:
        parts.append(_col_expr(item))

    return ",\n    ".join(parts)


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

    Note:
        ``agg`` with ``percentile_`` uses
        ``approx_percentile``, a Presto/Trino
        extension. Override ``agg`` in a
        concrete subclass if targeting a
        different engine.
        ``op`` with ``exclude`` (and no
        ``select_only``) raises ``ValueError``
        because it requires ``SELECT * EXCEPT``,
        which is not ANSI SQL.
    """

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
        select_list = _agg_select(
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
        return _render(
            "agg.sql",
            input_query=self.input_query,
            select_list=select_list,
            groupby=groupby,
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
