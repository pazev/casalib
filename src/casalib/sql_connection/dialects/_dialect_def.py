"""DialectDefinition ABC and agg rendering dispatch."""
from abc import ABC, abstractmethod
from typing import (
    List,
    Tuple,
    TypedDict,
)

from ._helpers import AggCol, AggOperationEnum, ProcessedAggDict


class RenderedAggDict(TypedDict):
    groupby: List[Tuple[str, str]]
    cols_before: List[Tuple[str, str]]
    cols_after: List[Tuple[str, str]]
    ops: List[Tuple[str, str]]


class DialectDefinition(ABC):
    """Defines dialect-specific SQL rendering primitives.

    Concrete defaults cover universal ANSI SQL ops.
    Override only the methods that differ for your engine.
    """

    def render_count(self, col: str) -> Tuple[str, str]:
        return f"COUNT({col})", f"{col}__count"

    def render_count_null(self, col: str) -> Tuple[str, str]:
        return (
            f"SUM(CASE WHEN {col} IS NULL THEN 1 ELSE 0 END)",
            f"{col}__count_null",
        )

    def render_count_distinct(self, col: str) -> Tuple[str, str]:
        return f"COUNT(DISTINCT {col})", f"{col}__count_distinct"

    def render_sum(self, col: str) -> Tuple[str, str]:
        return f"SUM({col})", f"{col}__sum"

    def render_mean(self, col: str) -> Tuple[str, str]:
        return f"AVG({col})", f"{col}__mean"

    def render_min(self, col: str) -> Tuple[str, str]:
        return f"MIN({col})", f"{col}__min"

    def render_max(self, col: str) -> Tuple[str, str]:
        return f"MAX({col})", f"{col}__max"

    @abstractmethod
    def render_percentile(self, agg_col: AggCol) -> Tuple[str, str]:
        """Render a PERCENTILE aggregation. Engine-specific."""


def _dispatch(
    dialect_def: DialectDefinition,
    agg_col: AggCol,
) -> Tuple[str, str]:
    op = agg_col.op
    col = agg_col.col
    if op == AggOperationEnum.COUNT:
        return dialect_def.render_count(col)
    if op == AggOperationEnum.COUNT_NULL:
        return dialect_def.render_count_null(col)
    if op == AggOperationEnum.COUNT_DISTINCT:
        return dialect_def.render_count_distinct(col)
    if op == AggOperationEnum.SUM:
        return dialect_def.render_sum(col)
    if op == AggOperationEnum.MEAN:
        return dialect_def.render_mean(col)
    if op == AggOperationEnum.MIN:
        return dialect_def.render_min(col)
    if op == AggOperationEnum.MAX:
        return dialect_def.render_max(col)
    if op == AggOperationEnum.PERCENTILE:
        return dialect_def.render_percentile(agg_col)
    raise ValueError(f"Unknown AggOperationEnum value: {op}")


def render_agg_def(
    processed: ProcessedAggDict,
    dialect_def: DialectDefinition,
) -> RenderedAggDict:
    return RenderedAggDict(
        groupby=processed['groupby'],
        cols_before=processed['cols_before'],
        cols_after=processed['cols_after'],
        ops=[
            _dispatch(dialect_def, ac)
            for ac in processed['ops']
        ],
    )
