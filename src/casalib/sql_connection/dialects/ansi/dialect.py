"""AnsiDialect — SqlDialectAbstract implementation for
standard ANSI SQL.

All parameter treatment is done here. All rendering is
delegated to AnsiDialectDef.
"""
from dataclasses import dataclass
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
from .._helpers import normalise_cols, process_agg_args
from .dialect_def import AnsiDialectDef


# ----------------------------------
# Private helpers (parameter treatment)
# ----------------------------------
def _join_on(
    keys: Sequence[Union[str, Tuple[str, str]]],
    left: str = "l",
    right: str = "r",
) -> str:
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
    """SqlDialectAbstract implementation for ANSI SQL.

    Parameter treatment happens here; all rendering is
    delegated to AnsiDialectDef. jsonify uses the SQL:2016
    JSON_OBJECT syntax.

    op with exclude (and no select_only) raises ValueError
    because it requires SELECT * EXCEPT, which is not ANSI SQL.
    """

    _dialect_def: ClassVar[AnsiDialectDef] = AnsiDialectDef()

    def select(self, table_name: str) -> str:
        return self._dialect_def.render_select(table_name)

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
        return self._dialect_def.render_agg(
            processed, self.input_query
        )

    def get_duplicates(
        self, keys: List[str]
    ) -> str:
        join_on = _join_on(
            keys, left="base", right="counts"
        )
        return self._dialect_def.render_get_duplicates(
            self.input_query, keys, join_on
        )

    def sample(self, num_samples: int) -> str:
        return self._dialect_def.render_sample(
            self.input_query, num_samples
        )

    def last_partitions(
        self,
        date_ingestion: str,
        columns: List[str],
    ) -> str:
        join_on = _join_on(
            columns, left="base", right="__latest"
        )
        return self._dialect_def.render_last_partitions(
            self.input_query,
            date_ingestion,
            columns,
            join_on,
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
        return self._dialect_def.render_enrich(
            self.input_query, others, join_ons
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
        return self._dialect_def.render_get_diffs(
            self.input_query,
            other,
            join_on,
            key_cols,
            diff_cols,
            diff_where,
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

    def jsonify(
        self,
        keys: List[str],
        columns: List[str],
    ) -> str:
        return self._dialect_def.render_jsonify(
            self.input_query, keys, columns
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
