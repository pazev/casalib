"""DialectDefinition ABC and agg expression dispatch."""
from abc import ABC, abstractmethod
from pathlib import Path
from typing import (
    List,
    Tuple,
    TypedDict,
)

from jinja2 import Environment, StrictUndefined, Template

from ._helpers import AggCol, AggOperationEnum


_JINJA_ENV = Environment(
    undefined=StrictUndefined,
    trim_blocks=True,
    lstrip_blocks=True,
    keep_trailing_newline=True,
)


class RenderedAggDict(TypedDict):
    groupby: List[Tuple[str, str]]
    cols_before: List[Tuple[str, str]]
    cols_after: List[Tuple[str, str]]
    ops: List[Tuple[str, str]]


class DialectDefinition(ABC):
    """Defines dialect-specific SQL rendering primitives.

    Concrete defaults cover universal ANSI SQL agg ops.
    Override only the methods that differ for your engine.
    """

    def _load_template(self, template_path: Path) -> Template:
        return _JINJA_ENV.from_string(
            template_path.read_text()
        )

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

    @abstractmethod
    def render_jsonify(
        self,
        input_query: str,
        keys: List[str],
        columns: List[str],
    ) -> str:
        """Render a jsonify query. Engine-specific."""

    def _dispatch_agg(
        self, agg_col: AggCol
    ) -> Tuple[str, str]:
        op = agg_col.op
        col = agg_col.col
        if op == AggOperationEnum.COUNT:
            return self.render_count(col)
        if op == AggOperationEnum.COUNT_NULL:
            return self.render_count_null(col)
        if op == AggOperationEnum.COUNT_DISTINCT:
            return self.render_count_distinct(col)
        if op == AggOperationEnum.SUM:
            return self.render_sum(col)
        if op == AggOperationEnum.MEAN:
            return self.render_mean(col)
        if op == AggOperationEnum.MIN:
            return self.render_min(col)
        if op == AggOperationEnum.MAX:
            return self.render_max(col)
        if op == AggOperationEnum.PERCENTILE:
            return self.render_percentile(agg_col)
        raise ValueError(
            f"Unknown AggOperationEnum value: {op}"
        )
