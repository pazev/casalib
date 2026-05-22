"""Shared helpers for SQL dialect implementations."""
from dataclasses import dataclass
from typing import (
    Dict,
    List,
    Optional,
    Sequence,
    Tuple,
    TypedDict,
    Union,
)


def normalise_cols(
    cols: Sequence[Union[str, Tuple[str, str]]],
) -> List[Tuple[str, str]]:
    """
    Normalise columns, to always be in the format
    (left, right). If string, returns (left, right).

    Args:
        cols: Mix of bare column names and
            (left_col, right_col) tuples.

    Returns:
        List of (left_col, right_col) pairs.
    """
    def _norm(
        k: Union[str, Tuple[str, str]]
    ) -> Tuple[str, str]:
        if isinstance(k, tuple):
            return k
        return (k, k)

    return [_norm(k) for k in cols]


# Aggregation procedure
# =====================
@dataclass(slots=True)
class AggCol:
    col: str
    op: str

    percentile: Optional[int] = None
    ignore_values: Optional[List[float]] = None

    def __lt__(self, other: "AggCol") -> bool:
        for fld in self.__dataclass_fields__:
            a, b = getattr(self, fld), getattr(other, fld)
            if a == b:
                continue
            if a is None:
                return True
            if b is None:
                return False
            return a < b
        return False

    @classmethod
    def _from_simple_ops(
        cls, simple_agg: Dict[str, List[str]]
    ) -> List["AggCol"]:
        ''' Process the dict with data '''
        return sorted([
            cls(col, op)
            for op, list_cols in simple_agg.items()
            for col in list_cols
        ])

    @classmethod
    def _from_percentiles(
        cls,
        percentiles: Dict[int, List[str]],
        ignore_vals: Dict[str, List[float]],
    ) -> List["AggCol"]:
        ''' Create the percentiles from the input dicts '''
        return sorted([
            cls(col, 'percentile', percentile, ignore)
            for percentile, list_cols in percentiles.items()
            for col in list_cols
            for ignore in [ignore_vals.get(col, [])]
        ])

    @classmethod
    def process(
        cls,
        simple_agg: Dict[str, List[str]],
        percentiles: Dict[int, List[str]],
        ignore_vals: Dict[str, List[float]],
    ) -> List["AggCol"]:
        ''' Process the information '''
        return sorted([
            *cls._from_simple_ops(simple_agg),
            *cls._from_percentiles(percentiles, ignore_vals)
        ])


class ProcessedAggDict(TypedDict):
    groupby: List[Tuple[str, str]]
    cols_before: List[Tuple[str, str]]
    cols_after: List[Tuple[str, str]]
    ops: List[AggCol]



def agg_select(
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
) -> ProcessedAggDict:
    # pylint: disable=too-many-arguments
    # pylint: disable=too-many-locals
    """
    Build the SELECT expression list for agg.

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
    # Adjust aggregations
    ops = AggCol.process(
        simple_agg={
            'count_': count_ or [],
            'count_null_': count_null_ or [],
            'count_distinct_': count_distinct_ or [],
            'sum_': sum_ or [],
            'mean_': mean_ or [],
            'min_': min_ or [],
            'max_': max_ or [],
        },
        percentiles=percentile_ or {},
        ignore_vals=percentile_ignore_values_ or {},
    )
    return {
        'groupby': normalise_cols(groupby or []),
        'cols_before': normalise_cols(cols_before or []),
        'cols_after': normalise_cols(cols_after or []),
        'ops': ops,
    }
