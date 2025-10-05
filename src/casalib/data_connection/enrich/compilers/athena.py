"""
Module define enrich functions for an AthenaCompiler
"""
from dataclasses import dataclass
from typing import (
    Any, Callable, Dict, List, Optional, Tuple, Union
)

from casalib.data_connection.connectors.athena import (
    AthenaConnection
)
import jinja2
import pandas as pd
from tqdm import tqdm

from ..base import EnrichmentPlan, Enricher, Public


STEP_BASE_TEMPLATE = """
-- ------ --
-- Inputs --
-- ------ --
with
public_ as (
    select * from {{ public_table }}
)
,
public_selected_ as (
    select * from public_
    where {{ public_event_ymd_column }} = '{{ event_dt }}'
)
,
source_ as (
    select * from {{ source.metadata['table_name'] }}
)
,
source_selected_ as (
    select * from source_
    where
            {{ source.info_ymd_column }} = '{{ info_dt }}'
        and {{ source.ingestion_column }} = '{{ ingestion_dt }}'
)
-- --------------- --
-- Public enriched --
-- --------------- --
,
public_enriched_ as (
    select
        public_selected_.*,
        {%- for col, col_ren in renamed_columns.items() %}
        source_selected_.{{col}} as {{col_ren}}
        {%- if not loop.last%},{%endif%}
        {%- endfor %}
    from
            public_selected_
        join
            source_selected_
                on  1=1
                {%- for key in source.keys %}
                and public_selected_.{{ renamed_keys.get(key, key) }}
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
    plan: EnrichmentPlan,
    partition_list: List[Dict[str, str]],
    public_table: str,
) -> Tuple[str, Dict[str, str]]:
    """ Compile a step in a query to be executed """
    env = jinja2.Environment()
    template = env.from_string(STEP_BASE_TEMPLATE)

    queries = [
        template.render(
            public_table=public_table,
            public_event_ymd_column=plan.public.event_ymd_column,
            source=plan.source,
            renamed_columns=plan.output_columns,
            renamed_keys=plan.renaming_keys,
            event_dt=par['event_dt'],
            info_dt=par['info_dt'],
            ingestion_dt=par['ingestion_dt'],
        )
        for par in partition_list
    ]

    return {
        'queries': queries,
        'output_columns': plan.output_columns
    }


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


def source_discover_partitions_cross(
    event_date_df: pd.DataFrame,

    source_table_name: str,
    source_info_ymd_column: str,
    source_ingestion_column: str,

    conn: AthenaConnection,
) -> List[Dict[str, str]]:
    """
    Discover the correct Source partition to use to merge
    with public.
    """
    # Capture the info dates
    info_date = get_partition_rows_count(
        query=f'''select * from {source_table_name} ''',
        cols={
            source_info_ymd_column: 'info_dt',
            source_ingestion_column: 'ingestion_dt'
        },
        conn=conn
    )

    # Cartesian product
    cross_ = (
        event_date_df.assign(flag=1)
        .merge(
            info_date.assign(flag=1)
        )
        .drop('flag', axis=1)
    )

    # The info date must be before event date
    cross_ = cross_[
        (cross_['event_dt'] >= cross_['info_dt'])
        # &
        # (cross_['event_dt'].str[:6] == cross_['info_dt'].str[:6])
    ]


    # Select the correct partition
    max_date = cross_.merge(
        cross_.merge(
            cross_.groupby(
                ['event_dt'],
                as_index=False,
            )['info_dt'].max()
        ).groupby(
            ['event_dt', 'info_dt'],
            as_index=False,
        )['ingestion_dt'].max()
    )

    return [
        row.to_dict()
        for idx, row in max_date.iterrows()
    ]


def get_partition_rows_count(
    query: str,
    cols: Dict[str, str],
    conn: AthenaConnection,
) -> pd.DataFrame:
    """ Return the partition count """
    env = jinja2.Environment(undefined=jinja2.StrictUndefined)
    template = env.from_string(r'''
    with tab_ as ({{query}})
    select
    {%- for col, alias in cols.items() %}
    {{col}} as {{alias}}{%if not loop.last%},{%endif%}
    {%- endfor%}
    from tab_
    group by
    {%- for col, alias in cols.items() %}
    {{ loop.index }}{%if not loop.last%},{%endif%}
    {%- endfor%}
    ''')
    return conn.query(template.render(query=query, cols=cols))


def compile_sources(
    conn: AthenaConnection,
    make_name_function: Callable[[Any, ...], str],
    enricher: Enricher,

    public_table: str,
    public_event_ymd_columns: str,
    public_event_ymd_columns_pd: pd.DataFrame,

    partition_cols: Optional[List[str]] = None,
) -> List[Dict[str, str]]:
    """ Compile the queries """
    table_queries = {}
    partition_cols = partition_cols or []

    for enr_plan in enricher.sources:
        # Cria o nome da tabela
        table_name = make_name_function(
            optional_number=None,
            optional_name=enr_plan.source.prefix,
        )

        # Discover the valid partitions
        partitions_list = source_discover_partitions_cross(
            event_date_df=public_event_ymd_columns_pd,
            source_table_name=enr_plan.source.metadata['table_name'],
            source_info_ymd_column=enr_plan.source.info_ymd_column,
            source_ingestion_column=enr_plan.source.ingestion_column,
            conn=conn
        )

        # Generate the query
        queries = compile_step(
            plan=enr_plan,
            partition_list=partitions_list,
            public_table=public_table
        )

        table_queries[table_name] = queries['queries']

    # Flatten
    return [
        {
            'table_name': table_name,
            'query': query,
            'partition_cols': partition_cols
        }
        for table_name, list_query in table_queries.items()
        for query in list_query
    ]


@dataclass
class AthenaCompiler:
    """
    Compiler for an Athena connection. Generate the SQL
    queries to enrich the public.
    """
    prefix: str
    study: str
    partition_cols: Optional[List[str]] = None

    def __post_init__(self):
        """ Post-init """
        self.conn_maker: Optional[
            Callable[[], AthenaConnection]
        ] = None

    def get_conn_(self) -> AthenaConnection:
        """ Return the connection object """
        if self.conn_maker is None:
            raise ValueError(
                "Set the connection maker with set_conn_maker"
            )
        return self.conn_maker()

    def set_conn_maker(
        self,
        conn_maker: Callable[[], AthenaConnection]
    ) -> "AthenaCompiler":
        """ Set the conn maker """
        self.conn_maker = conn_maker
        return self

    def make_table_name_(
        self,
        optional_number: Optional[Union[int, str]] = None,
        optional_name: Optional[str] = None,
    ) -> str:
        """ Make the query name """
        name = f'{self.prefix}{self.study}_table_'

        if optional_number is not None:
            name += f'_{optional_number}_'

        if optional_name:
            name += f'_{optional_name}'

        return name

    def make_public_table_(
        self,
        enricher: Enricher,
        partition_cols: Optional[List[str]] = None
    ) -> List[Dict[str, str]]:
        """ Create the public table """
        partition_cols = partition_cols or []

        query_dict = {
            'query': enricher.public.metadata['query'],
            'table_name': self.make_table_name_(None, 'public_enricher'),
            'partition_cols': partition_cols
        }

        return query_dict

    def compile(
        self,
        enricher: Enricher,
    ) -> List[Dict[str, str]]:
        """ Compile the queries """
        queries: List[Dict[str, str]] = []

        conn = self.get_conn_()

        # Create public table with the indicated partitions
        public_query = self.make_public_table_(
            enricher=enricher,
            partition_cols=self.partition_cols
        )
        public_event_ymd_column = enricher.public.event_ymd_column

        public_event_ymd_columns_pd = get_partition_rows_count(
            query=enricher.public.metadata['query'],
            cols={public_event_ymd_column: 'event_dt'},
            conn=conn,
        )

        queries.append(public_query)

        # Enrich with targets
        # TODO

        # Source queries
        source_queries = compile_sources(
            conn=conn,
            make_name_function=self.make_table_name_,
            enricher=enricher,

            public_table=public_query['table_name'],
            public_event_ymd_column=public_event_ymd_column,
            public_event_ymd_columns_pd=public_event_ymd_columns_pd,

            partition_cols=self.partition_cols,
        )
        queries.extend(source_queries)

        return queries

    def __call__(
        self, enricher: Enricher
    ) -> List[Dict[str, str]]:
        """ Run the compiler """
        return self.compile(enricher=enricher)
