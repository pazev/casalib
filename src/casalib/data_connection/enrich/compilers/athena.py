"""
Module define enrich functions for an AthenaCompiler
"""
from dataclasses import dataclass
from typing import Dict, List, Optional, Tuple, Union

import jinja2

from ..base import EnrichmentPlan, Enricher, Public


STEP_BASE_TEMPLATE = """
-- ------ --
-- Inputs --
-- ------ --
with
public_ as (
    {{ public.metadata['query'] | indent(4) }}
)
,
source_ as (
    select * from {{ source.metadata['table_name'] }}
)
-- ------------------------------- --
-- Select partitions in the source --
-- ------------------------------- --
,
list_event_ymd_ as (
    select {{ public.event_ymd_column }} as __event_ymd__
    from public_
    group by 1
)
,
list_info_ymd_ingestion_ as (
    select
        {{ source.info_ymd_column }} as __info_ymd__,
        max({{ source.ingestion_column }}) as __ingestion__
    from
        source_
    group by 1
)
,
list_event_info_ymd_ingestion_ as (
    select
        __event_ymd__,
        __info_ymd__,
        __ingestion__,
        max(__info_ymd__)
            over (partition by __event_ymd__)
                as max_info_ymd
    from
        list_event_ymd_,
        list_info_ymd_ingestion_
    where
        __event_ymd__ >= __info_ymd__
)
,
selected_info_ymd_ingestion_ as (
    select * from list_event_info_ymd_ingestion_
    where __info_ymd__ = max_info_ymd
)
,
source_selected_ as (
    select
        source_.*,
        selected_info_ymd_ingestion_.__event_ymd__
    from
        source_
    left join
        selected_info_ymd_ingestion_
            on  source_.{{source.info_ymd_column}} =
                selected_info_ymd_ingestion_.__info_ymd__
            and source_.{{source.ingestion_column}} =
                selected_info_ymd_ingestion_.__ingestion__
)
-- --------------- --
-- Public enriched --
-- --------------- --
,
public_enriched_ as (
    select
        public_.*,
        {%- for col, col_ren in renamed_columns.items() %}
        source_selected_.{{col}} as {{col_ren}}
        {%- if not loop.last%},{%endif%}
        {%- endfor %}
    from
            public_
        join
            source_selected_
                on  public_.{{ public.event_ymd_column }}
                    = source_selected_.__event_ymd__
                {%- for key in source.keys %}
                and public_.{{ renamed_keys.get(key, key) }}
                    = source_selected_.{{ key }}
                {%- endfor %}
)

select * from public_enriched_
"""


FINAL_BASE_TEMPLATE = """
with
public_ as (
    {{ public.metadata['query'] | indent(4) }}
)
,
{%- for table_name, output_cols in columns.items() %}
t{{ loop.index }}_ as (
    select * from {{ table_name }}
)
,
{%- endfor %}
cross_ as (
    select
        public_.*,
        {%- for table_name, out_cols in columns.items() %}
        {%- set tnumber = loop.index %}
        {%- for col, col_ren in out_cols.items() %}
        t{{ tnumber }}_.{{ col_ren }},
        {%- endfor %}
        {%- endfor %}
        1 as flag__
    from
            public_
        left join
            {%- for table_name in columns %}
            {%- set tnumber = loop.index %}
            t{{ tnumber }}_
                on
                    {%- for col in public.columns %}
                    public_.{{col}} =
                        t{{ tnumber }}_.{{col}}
                    {% if not loop.last %}and{% endif %}
                    {%- endfor %}
        {%- if not loop.last%}left join{% endif %}
        {%- endfor %}
)
select * from cross_
"""


def compile_step(
    plan: EnrichmentPlan
) -> Tuple[str, Dict[str, str]]:
    """ Compile a step in a query to be executed """
    env = jinja2.Environment()
    template = env.from_string(STEP_BASE_TEMPLATE)
    query = template.render(
        public=plan.public,
        source=plan.source,
        renamed_columns=plan.output_columns,
        renamed_keys=plan.renaming_keys,
    )
    return query, plan.output_columns


def compile_last_query(
    public: Public, columns: Dict[str, Dict[str, str]]
) -> str:
    """ Compile the last query to be executed """
    env = jinja2.Environment()
    template = env.from_string(FINAL_BASE_TEMPLATE)
    query = template.render(
        public=public,
        columns=columns
    )
    return query


@dataclass
class AthenaCompiler:
    """
    Compiler for an Athena connection. Generate the SQL
    queries to enrich the public.
    """
    prefix: str
    study: str

    def make_table_name_(
        self,
        idx: Union[int, str],
        optional_name: Optional[str] = None,
    ) -> str:
        """ Make the query name """
        name = f'{self.prefix}{self.study}_table_'

        if optional_name:
            name += f'_{optional_name}'

        return name

    def __call__(
        self, enricher: Enricher
    ) -> List[Dict[str, str]]:
        """ Run the compiler """
        return self.compile(enricher=enricher)

    def compile(
        self,
        enricher: Enricher
    ) -> List[Dict[str, str]]:
        """ Compile the queries """
        table_queries = {}
        table_columns = {}

        for idx, enr_plan in enumerate(enricher.sources):
            table_name = self.make_table_name_(
                idx, enr_plan.source.prefix
            )
            query, out_cols = compile_step(enr_plan)

            table_queries[table_name] = query
            table_columns[table_name] = out_cols

        return [
            {'table_name': tab_name, 'query': query}
            for tab_name, query in table_queries.items()
        ]
