"""DialectDefinition ABC and agg expression dispatch."""
from abc import ABC, abstractmethod
from dataclasses import dataclass
from pathlib import Path
from typing import (
    Callable,
    Dict,
    List,
    Optional,
    Tuple,
    Union,
)

import jinja2

from .helpers import (
    AggCol,
    AggOperationEnum,
    ProcessedAggDef,
    RenderedCol,
    normalise_cols,
)


@dataclass
class RenderedAgg:
    """
    Fully-rendered agg definition for the SQL template.
    """

    groupby: List[Tuple[str, str]]
    cols_before: List[RenderedCol]
    cols_after: List[RenderedCol]
    ops: List[RenderedCol]

    @classmethod
    def from_agg_def(
        cls,
        agg_def: ProcessedAggDef,
        _dispatch: Callable[[AggCol], RenderedCol],
    ) -> "RenderedAgg":
        """Build a RenderedAgg by dispatching each op."""
        return cls(
            groupby=agg_def.groupby,
            cols_before=[
                RenderedCol(sql_code=s, alias=a)
                for s, a in agg_def.cols_before
            ],
            cols_after=[
                RenderedCol(sql_code=s, alias=a)
                for s, a in agg_def.cols_after
            ],
            ops=[_dispatch(op) for op in agg_def.ops],
        )


class DialectDefinition(ABC):
    """Defines dialect-specific SQL rendering primitives.

    Concrete defaults cover universal ANSI SQL agg ops.
    Override only the methods that differ for your engine.
    """

    # How to load template
    # ====================
    def _env(self) -> jinja2.environment.Environment:
        """Return a configured Jinja2 environment."""
        return jinja2.Environment(
            undefined=jinja2.StrictUndefined,
            trim_blocks=True,
            lstrip_blocks=True,
            keep_trailing_newline=True,
        )

    def _load_template(
        self,
        template_path: Union[str, Path],
    ) -> jinja2.Template:
        """Load a template from a file path."""
        return self._env().from_string(
            Path(template_path).read_text(encoding="utf-8")
        )

    def _from_string(
        self, template_str: str
    ) -> jinja2.Template:
        """Compile a template from a string."""
        return self._env().from_string(template_str)

    # Template path
    # =============
    @abstractmethod
    def _get_select_template(self) -> Path:
        """Return the path to the select template."""

    @abstractmethod
    def _get_agg_template(self) -> Path:
        """Return the path to the agg template."""

    @abstractmethod
    def _get_sample_template(self) -> Path:
        """Return the path to the sample template."""

    @abstractmethod
    def _get_get_duplicates_template(self) -> Path:
        """Return the path to get_duplicates template."""

    @abstractmethod
    def _get_last_partition_template(self) -> Path:
        """Return the path to last_partition template."""

    @abstractmethod
    def _get_enrich_template(self) -> Path:
        """Return the path to the enrich template."""

    @abstractmethod
    def _get_jsonify_template(self) -> Path:
        """Return the path to the jsonify template."""

    @abstractmethod
    def _get_missing_keys_template(self) -> Path:
        """Return the path to the missing_keys template."""

    @abstractmethod
    def _get_get_diffs_template(self) -> Path:
        """Return the path to the get_diffs template."""

    @abstractmethod
    def _get_build_op_template(self) -> Path:
        """Return the path to the build_op template."""

    # Agg primitive renderers (abstract)
    # ===================================
    @abstractmethod
    def _render_case_not_in_float(
        self,
        col: str,
        ignore_values: Optional[List[float]] = None,
    ) -> str:
        """ Render how the case when is rendered """

    @abstractmethod
    def _render_agg_percentile_sql_code(
        self, col: str, sql_code: str, perc: int,
    ) -> RenderedCol:
        """Render the percentile function call."""

    @abstractmethod
    def _render_agg_count(self, col: AggCol) -> RenderedCol:
        """Render COUNT."""

    @abstractmethod
    def _render_agg_count_null(
        self, col: AggCol
    ) -> RenderedCol:
        """Render count of NULLs via SUM(CASE ...)."""

    @abstractmethod
    def _render_agg_count_distinct(
        self, col: AggCol
    ) -> RenderedCol:
        """Render COUNT DISTINCT."""

    @abstractmethod
    def _render_agg_sum(self, col: AggCol) -> RenderedCol:
        """Render SUM."""

    @abstractmethod
    def _render_agg_mean(self, col: AggCol) -> RenderedCol:
        """Render AVG."""

    @abstractmethod
    def _render_agg_min(self, col: AggCol) -> RenderedCol:
        """Render MIN."""

    @abstractmethod
    def _render_agg_max(self, col: AggCol) -> RenderedCol:
        """Render MAX."""

    # Agg orchestration
    # =================
    def _render_agg_percentile(
        self,
        agg_col: AggCol
    ) -> RenderedCol:
        """Render a PERCENTILE aggregation."""
        cfg = agg_col.percentile_config

        if cfg is None:
            raise ValueError(
                '`percentile_config` cannot be None'
            )

        adj_col = self._render_case_not_in_float(
            col=agg_col.col, ignore_values=cfg.ignore_values
        )
        return self._render_agg_percentile_sql_code(
            col=agg_col.col,
            sql_code=adj_col,
            perc=cfg.percentile,
        )

    def _dispatch_agg(
        self, agg_col: AggCol
    ) -> RenderedCol:
        """Dispatch an AggCol to its render method."""
        dispatch: Dict[
            AggOperationEnum,
            Callable[[AggCol], RenderedCol],
        ] = {
            AggOperationEnum.COUNT: (
                self._render_agg_count
            ),
            AggOperationEnum.COUNT_DISTINCT: (
                self._render_agg_count_distinct
            ),
            AggOperationEnum.COUNT_NULL: (
                self._render_agg_count_null
            ),
            AggOperationEnum.SUM: (
                self._render_agg_sum
            ),
            AggOperationEnum.MEAN: (
                self._render_agg_mean
            ),
            AggOperationEnum.MIN: (
                self._render_agg_min
            ),
            AggOperationEnum.MAX: (
                self._render_agg_max
            ),
            AggOperationEnum.PERCENTILE: (
                self._render_agg_percentile
            ),
        }

        if agg_col.op not in dispatch:
            raise ValueError(
                f"Unknown AggOperationEnum: {agg_col.op}"
            )

        return dispatch[agg_col.op](agg_col)

    # Render query
    # ============
    def render_select(self, table_name: str) -> str:
        """Render a SELECT * from a table."""
        return self._load_template(
            self._get_select_template()
        ).render(table_name=table_name)

    def render_agg(
        self,
        processed: ProcessedAggDef,
        input_query: str,
    ) -> str:
        """Render an aggregation query."""
        agg_def = RenderedAgg.from_agg_def(
            processed,
            self._dispatch_agg,
        )
        return self._load_template(
            self._get_agg_template()
        ).render(
            input_query=input_query,
            agg_def=agg_def,
        )

    def render_sample(
        self,
        input_query: str,
        num_samples: int = 100,
    ) -> str:
        """Render a LIMIT/TABLESAMPLE query."""
        return self._load_template(
            self._get_sample_template()
        ).render(
            input_query=input_query,
            num_samples=num_samples,
        )

    def render_get_duplicates(
        self,
        input_query: str,
        keys: List[str],
        join_on: str,
    ) -> str:
        """Render a duplicate-detection query."""
        return self._load_template(
            self._get_get_duplicates_template()
        ).render(
            input_query=input_query,
            keys=keys,
            join_on=join_on,
        )

    def render_last_partitions(
        self,
        input_query: str,
        date_ingestion: str,
        columns: List[str],
        join_on: str,
    ) -> str:
        """Render a latest-partition filter query."""
        return self._load_template(
            self._get_last_partition_template()
        ).render(
            input_query=input_query,
            date_ingestion=date_ingestion,
            columns=columns,
            join_on=join_on,
        )

    def render_enrich(
        self,
        input_query: str,
        other_queries: List[str],
        join_ons: List[str],
    ) -> str:
        """Render a multi-join enrichment query."""
        return self._load_template(
            self._get_enrich_template()
        ).render(
            input_query=input_query,
            other_queries=other_queries,
            join_ons=join_ons,
        )

    def render_jsonify(
        self,
        input_query: str,
        keys: List[str],
        columns: List[str],
    ) -> str:
        """Render a JSON serialisation query."""
        return self._load_template(
            self._get_jsonify_template()
        ).render(
            input_query=input_query,
            keys=keys,
            columns=columns,
        )

    def render_missing_keys(
        self,
        input_query: str,
        other_query: str,
        join_on: List[Tuple[str, str]],
    ) -> str:
        """Render a missing-key detection query."""
        return self._load_template(
            self._get_missing_keys_template()
        ).render(
            input_query=input_query,
            other_query=other_query,
            join_on=join_on,
        )

    def render_get_diffs(
        self,
        input_query: str,
        other_query: str,
        join_on: List[Union[str, Tuple[str, str]]],
        compare_cols: List[
            Union[str, Tuple[str, str]]
        ],
    ) -> str:
        """Render a side-by-side diff query."""
        join_on_ = normalise_cols(join_on)
        compare_cols_ = normalise_cols(compare_cols)
        return self._load_template(
            self._get_get_diffs_template()
        ).render(
            input_query=input_query,
            other_query=other_query,
            join_on=join_on_,
            compare_cols=compare_cols_,
        )

    def render_build_op(  # pylint: disable=too-many-arguments
        self,
        input_query: str,
        add: Optional[Dict[str, str]],
        rename: Optional[Dict[str, str]],
        select_only: Optional[List[str]],
        exclude: Optional[List[str]],
    ) -> str:
        """Render a column projection/rename query."""
        return self._load_template(
            self._get_build_op_template()
        ).render(
            input_query=input_query,
            add=add,
            rename=rename,
            select_only=select_only,
            exclude=exclude,
        )
