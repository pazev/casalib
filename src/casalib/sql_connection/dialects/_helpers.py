"""Shared helpers for SQL dialect implementations."""
from dataclasses import dataclass, field
from enum import Enum, auto
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


# Aggregation simplification
# ==========================
class AggOperationEnum(Enum):
    COUNT = auto()
    COUNT_NULL = auto()
    COUNT_DISTINCT = auto()
    SUM = auto()
    MEAN = auto()
    MAX = auto()
    MIN = auto()
    PERCENTILE = auto()


@dataclass(slots=True)
class PercentileConfig:
    percentile: int
    ignore_values: List[float] = field(default_factory=list)


@dataclass(slots=True)
class AggCol:
    col: str
    op: AggOperationEnum
    percentile_config: Optional[PercentileConfig] = None

    def __lt__(self, other: "AggCol") -> bool:
        if self.col < other.col:
            return True
        if self.col > other.col:
            return False

        if self.op.value < other.op.value:
            return True
        if self.op.value > other.op.value:
            return False

        if self.percentile_config is None and other.percentile_config is None:
            return False
        if self.percentile_config is None:
            return True
        if other.percentile_config is None:
            return False

        return (
            self.percentile_config.percentile
            < other.percentile_config.percentile
        )


class ProcessedAggDict(TypedDict):
    groupby: List[Tuple[str, str]]
    cols_before: List[Tuple[str, str]]
    cols_after: List[Tuple[str, str]]
    ops: List[AggCol]


def retrieve_agg_parameters(
    simple_agg: Dict[str, List[str]],
    percentiles: Dict[int, List[str]],
    percentiles_ignore_vals: Dict[str, List[float]],
) -> List[AggCol]:
    simple_agg_cols = [
        AggCol(col, AggOperationEnum[op])
        for op, list_cols in simple_agg.items()
        for col in list_cols
    ]

    percentiles_cols = [
        AggCol(
            col=col,
            op=AggOperationEnum.PERCENTILE,
            percentile_config=PercentileConfig(
                percentile=percentile,
                ignore_values=percentiles_ignore_vals.get(col, []),
            )
        )
        for percentile, list_cols in percentiles.items()
        for col in list_cols
    ]

    all_operations = sorted([*simple_agg_cols, *percentiles_cols])

    return all_operations


def process_agg_args(
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
    ops = retrieve_agg_parameters(
        simple_agg={
            'COUNT': count_ or [],
            'COUNT_NULL': count_null_ or [],
            'COUNT_DISTINCT': count_distinct_ or [],
            'SUM': sum_ or [],
            'MEAN': mean_ or [],
            'MIN': min_ or [],
            'MAX': max_ or [],
        },
        percentiles=percentile_ or {},
        percentiles_ignore_vals=percentile_ignore_values_ or {},
    )
    return ProcessedAggDict(
        groupby=normalise_cols(groupby or []),
        cols_before=normalise_cols(cols_before or []),
        cols_after=normalise_cols(cols_after or []),
        ops=ops
    )
