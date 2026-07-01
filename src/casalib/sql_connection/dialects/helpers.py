"""Shared helpers for SQL dialect implementations."""
from dataclasses import dataclass, field
from enum import Enum, auto
from typing import (
    Dict,
    List,
    Optional,
    Sequence,
    Tuple,
    Union,
)


# Normalization function
# ======================
def normalise_cols(
    cols: Sequence[Union[str, Tuple[str, str]]],
) -> List[Tuple[str, str]]:
    """Normalise columns to (left, right) pairs.

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


# Aggregation helpers
# ===================
class AggOperationEnum(Enum):
    """Supported aggregation operations."""

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
    """Configuration for a percentile aggregation."""

    percentile: int
    ignore_values: List[float] = field(
        default_factory=list
    )


@dataclass(slots=True)
class AggCol:
    """A single column + aggregation operation."""

    col: str
    op: AggOperationEnum
    percentile_config: Optional[PercentileConfig] = None

    def __lt__(self, other: "AggCol") -> bool:
        """Order by col, then op, then percentile."""
        if self.col != other.col:
            return self.col < other.col
        if self.op.value != other.op.value:
            return self.op.value < other.op.value
        s_pct = self.percentile_config
        o_pct = other.percentile_config
        if s_pct is None and o_pct is None:
            return False
        if s_pct is None:
            return True
        if o_pct is None:
            return False
        return s_pct.percentile < o_pct.percentile


@dataclass(slots=True)
class ProcessedAggDef:
    """Parsed and validated agg arguments."""

    groupby: List[Tuple[str, str]]
    cols_before: List[Tuple[str, str]]
    cols_after: List[Tuple[str, str]]
    ops: List[AggCol]


def retrieve_agg_parameters(
    simple_agg: Dict[AggOperationEnum, List[str]],
    percentiles: Dict[int, List[str]],
    percentiles_ignore_vals: Dict[str, List[float]],
) -> List[AggCol]:
    """Build a sorted list of AggCol from raw inputs."""
    simple_agg_cols = [
        AggCol(col, op)
        for op, list_cols in simple_agg.items()
        for col in list_cols
    ]

    percentiles_cols = [
        AggCol(
            col=col,
            op=AggOperationEnum.PERCENTILE,
            percentile_config=PercentileConfig(
                percentile=percentile,
                ignore_values=(
                    percentiles_ignore_vals.get(col, [])
                ),
            ),
        )
        for percentile, list_cols in percentiles.items()
        for col in list_cols
    ]

    return sorted([*simple_agg_cols, *percentiles_cols])


def process_agg_args(  # pylint: disable=too-many-arguments
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
) -> ProcessedAggDef:
    """Build a ProcessedAggDef from user-facing agg args.

    Args:
        groupby: Columns to GROUP BY.
        count_: Columns to COUNT (non-null).
        count_null_: Columns to count NULLs.
        count_distinct_: Columns to COUNT DISTINCT.
        sum_: Columns to SUM.
        mean_: Columns to AVG.
        min_: Columns to MIN.
        max_: Columns to MAX.
        percentile_: Percentile → columns map.
        percentile_ignore_values_: Column →
            values-to-exclude map.
        cols_before: Expressions prepended to SELECT.
        cols_after: Expressions appended to SELECT.

    Returns:
        ProcessedAggDef ready for rendering.
    """
    ops = retrieve_agg_parameters(
        simple_agg={
            AggOperationEnum.COUNT: count_ or [],
            AggOperationEnum.COUNT_NULL: (
                count_null_ or []
            ),
            AggOperationEnum.COUNT_DISTINCT: (
                count_distinct_ or []
            ),
            AggOperationEnum.SUM: sum_ or [],
            AggOperationEnum.MEAN: mean_ or [],
            AggOperationEnum.MIN: min_ or [],
            AggOperationEnum.MAX: max_ or [],
        },
        percentiles=percentile_ or {},
        percentiles_ignore_vals=(
            percentile_ignore_values_ or {}
        ),
    )
    return ProcessedAggDef(
        groupby=normalise_cols(groupby or []),
        cols_before=normalise_cols(cols_before or []),
        cols_after=normalise_cols(cols_after or []),
        ops=ops,
    )
