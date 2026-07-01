"""DialectDefinition ABC and agg expression dispatch."""
from abc import ABC, abstractmethod
from dataclasses import dataclass
from pathlib import Path
from typing import Callable, Dict, List, Optional, Tuple, Union

import jinja2

from .helpers import (
    AggCol,
    AggOperationEnum,
    ProcessedAggDef,
    normalise_cols,
)


@dataclass
class RenderedAgg:
    groupby: List[Tuple[str, str]]
    cols_before: List[Tuple[str, str]]
    cols_after: List[Tuple[str, str]]
    ops: List[Tuple[str, str]]

    @classmethod
    def from_agg_def(
        cls,
        agg_def: ProcessedAggDef,
        _dispatch: Callable[[AggCol], Tuple[str, str]]
    ) -> "RenderedAgg":
        return cls(
            groupby=agg_def.groupby,
            cols_before=agg_def.cols_before,
            cols_after=agg_def.cols_after,
            ops=[_dispatch(op) for op in agg_def.ops]
        )


class DialectDefinition(ABC):
    """Defines dialect-specific SQL rendering primitives.

    Concrete defaults cover universal ANSI SQL agg ops.
    Override only the methods that differ for your engine.
    """

    # Template path
    # =============
    @abstractmethod
    def _get_select_template(self) -> Path:
        """ Return the path to load the select template """

    @abstractmethod
    def _get_agg_template(self) -> Path:
        """ Return the path to load the agg template """

    @abstractmethod
    def _get_sample_template(self) -> Path:
        """ Return the path to load the sample template """

    @abstractmethod
    def _get_get_duplicates_template(self) -> Path:
        """ Return the path to load the get_duplicates template """

    @abstractmethod
    def _get_last_partition_template(self) -> Path:
        """ Return the last partition template """

    @abstractmethod
    def _get_enrich_template(self) -> Path:
        """ Return the enrich template """

    @abstractmethod
    def _get_jsonify_template(self) -> Path:
        """ Return the jsonify template """

    @abstractmethod
    def _get_missing_keys_template(self) -> Path:
        """ Return the missing keys template """

    @abstractmethod
    def _get_get_diffs_template(self) -> Path:
        """ Return the get_diffs template """

    @abstractmethod
    def _get_build_op_template(self) -> Path:
        """ Return the build_op template """

    # How to load template
    # ====================
    def _env(self) -> jinja2.environment.Environment:
        '''  '''
        import jinja2
        return jinja2.Environment(
            undefined=jinja2.StrictUndefined,
            trim_blocks=True,
            lstrip_blocks=True,
            keep_trailing_newline=True,
        )

    def _load_template(
        self,
        template_path: Union[str, Path]
    ) -> jinja2.Template:
        return self._env().from_string(
            Path(template_path).read_text()
        )

    def _from_string(self, template_str: str) -> jinja2.Template:
        return self._env().from_string(template_str)

    # How to render agg functions
    # ===========================
    def render_count(self, col: AggCol) -> Tuple[str, str]:
        return (
            f"COUNT({col.col})",
            f"{col.col}__count"
        )

    def render_count_null(self, col: AggCol) -> Tuple[str, str]:
        return (
            f"SUM(CASE WHEN {col.col} IS NULL THEN 1 ELSE 0 END)",
            f"{col.col}__count_null",
        )

    def render_count_distinct(self, col: AggCol) -> Tuple[str, str]:
        return (
            f"COUNT(DISTINCT {col.col})",
            f"{col.col}__count_distinct"
        )

    def render_sum(self, col: AggCol) -> Tuple[str, str]:
        return (
            f"SUM({col.col})",
            f"{col.col}__sum"
        )

    def render_mean(self, col: AggCol) -> Tuple[str, str]:
        return (
            f"AVG({col.col})",
            f"{col.col}__mean"
        )

    def render_min(self, col: AggCol) -> Tuple[str, str]:
        return (
            f"MIN({col.col})",
            f"{col.col}__min"
        )

    def render_max(self, col: AggCol) -> Tuple[str, str]:
        return (
            f"MAX({col.col})",
            f"{col.col}__max"
        )

    def render_percentile(
        self,
        agg_col: AggCol,
        func: str = 'approx_percentile',
    ) -> Tuple[str, str]:
        """ Render a PERCENTILE aggregation. """
        cfg = agg_col.percentile_config

        if cfg is None:
            raise ValueError(
                '`percentile_config` cannot be None'
            )

        func = 'approx_percentile'

        # Templates
        noign = self._from_string('{{func}}({{col}}, {{perc}})')
        ign = self._from_string(
            '''{{func}}('''
            '''CASE WHEN {{col}} NOT IN ({{ign | join(', ')}}'''
            ''') THEN {{col}} END, {{perc}})'''
        )
        template_to_use = ign if cfg.ignore_values else noign

        return (
            template_to_use.render(
                func=func,
                col=agg_col.col,
                perc=cfg.percentile,
                ign=cfg.ignore_values,
            ),
            f'{agg_col.col}__p{cfg.percentile}'
        )

    def _dispatch_agg(
        self, agg_col: AggCol
    ) -> Tuple[str, str]:
        dict_ops = {
            AggOperationEnum.COUNT: self.render_count,
            AggOperationEnum.COUNT_DISTINCT: self.render_count_distinct,
            AggOperationEnum.COUNT_NULL: self.render_count_null,
            AggOperationEnum.MAX: self.render_max,
            AggOperationEnum.MIN: self.render_min,
            AggOperationEnum.MEAN: self.render_mean,
            AggOperationEnum.SUM: self.render_sum,
            AggOperationEnum.PERCENTILE: self.render_percentile,
        }

        if agg_col.op not in dict_ops:
            raise ValueError(
                f"Unknown AggOperationEnum value: {agg_col.op}"
            )

        return dict_ops[agg_col.op](agg_col)

    # Render query
    def render_select(self, table_name: str) -> str:
        ''' Render select '''
        template = self._load_template(
            self._get_select_template()
        )
        return template.render(table_name=table_name)

    def render_agg(
        self,
        processed: ProcessedAggDef,
        input_query: str,
    ) -> str:
        """ Render agg query """
        template = self._load_template(
            self._get_agg_template()
        )
        agg_def = RenderedAgg.from_agg_def(
            processed,
            self._dispatch_agg
        )
        return template.render(
            input_query=input_query,
            agg_def=agg_def,
        )

    def render_sample(
        self,
        input_query: str,
        num_samples: int = 100
    ) -> str:
        ''' Render sample '''
        template = self._load_template(
            self._get_sample_template()
        )
        return template.render(
            input_query=input_query,
            num_samples=num_samples,
        )

    def render_get_duplicates(
        self,
        input_query: str,
        keys: List[str],
        join_on: str
    ) -> str:
        ''' Render get_duplicates '''
        template = self._load_template(
            self._get_get_duplicates_template()
        )
        return template.render(
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
        ''' Render last_partitions '''
        template = self._load_template(
            self._get_last_partition_template()
        )
        return template.render(
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
        ''' Render enrich '''
        template = self._load_template(
            self._get_enrich_template()
        )
        return template.render(
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
        """Render a jsonify query. """
        template = self._load_template(
            self._get_jsonify_template()
        )
        return template.render(
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
        ''' Render missing_keys '''
        template = self._load_template(
            self._get_missing_keys_template()
        )
        return template.render(
            input_query=input_query,
            other_query=other_query,
            join_on=join_on,
        )

    def render_get_diffs(
        self,
        input_query: str,
        other_query: str,
        join_on: List[Union[str, Tuple[str, str]]],
        compare_cols: List[Union[str, Tuple[str, str]]],
    ) -> str:
        ''' Render get_diffs '''
        template = self._load_template(
            self._get_get_diffs_template()
        )
        join_on_ = normalise_cols(join_on)
        compare_cols_ = normalise_cols(compare_cols)

        return template.render(
            input_query=input_query,
            other_query=other_query,
            join_on=join_on_,
            compare_cols=compare_cols_,
        )

    def render_build_op(
        self,
        input_query: str,
        add: Optional[Dict[str, str]],
        rename: Optional[Dict[str, str]],
        select_only: Optional[List[str]],
        exclude: Optional[List[str]],
    ) -> str:
        ''' Render build_op '''
        template = self._load_template(
            self._get_build_op_template()
        )
        return template.render(
            input_query=input_query,
            add=add,
            rename=rename,
            select_only=select_only,
            exclude=exclude,
        )
