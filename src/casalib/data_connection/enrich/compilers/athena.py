"""
Module define enrich functions for an AthenaCompiler
"""
from collections import defaultdict
from dataclasses import dataclass, field
from itertools import chain, zip_longest
from typing import (
    Callable, Dict, List, Optional, Tuple, Union
)

import jinja2
import pandas as pd
from tqdm import tqdm  # type: ignore

from casalib.data_connection.connectors.athena import (
    AthenaConnection
)

from ..base import EnrichmentPlan, Enricher, Public


COMPILE_STEP_TEMPLATE = """
-- ------ --
-- Inputs --
-- ------ --
with
public_ as (
    select * from {{ public_table_name }}
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
        public_.*,
        {%- for col, col_ren in renamed_columns.items() %}
        source_selected_.{{col}} as {{col_ren}}
        {%- if not loop.last%},{%endif%}
        {%- endfor %}
    from
            public_selected_ as public_
        join
            source_selected_
                on  1=1
                {%- for key in source.keys %}
                and public_.{{ renamed_keys.get(key, key) }}
                    = source_selected_.{{ key }}
                {%- endfor %}
)

select distinct * from public_enriched_
"""


COMPILE_FINAL_QUERY_TEMPLATE = """
with
public_ as (
    select * from {{ public_table_name }}
)
{%- for tab, cols in columns_dict.items() %}
,
tab{{loop.index}}_ as (
    select * from {{tab}}
)
{%- endfor %}
,
join_ as (
    select

    from
)
select * from
"""


def compile_step(
    plan: EnrichmentPlan,
    partition_list: List[Dict[str, str]],
    public_table_name: str,
) -> Dict[str, Union[List[str], Dict[str, str]]]:
    """
    Compile a step in a query to be executed.

    The step can be a Target or a Source.

    For each partition indicated at
    """
    env = jinja2.Environment(undefined=jinja2.StrictUndefined)
    template = env.from_string(COMPILE_STEP_TEMPLATE)

    queries = [
        template.render(
            public_table_name=public_table_name,
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


def compile_enrichment_plans(
    conn: AthenaConnection,
    make_name_function: Callable[[Union[int, None], str], str],
    enrichment_plan_list: List[EnrichmentPlan],
    public_table_name: str,
    public_event_ymd_column: str,
    public_event_ymd_columns_pd: pd.DataFrame,
    partition_cols: Optional[List[str]] = None,
    ignore_numbering: bool = True
) -> Tuple[List[Dict[str, str]], Dict[str, Dict[str, str]]]:
    """
    Compile the queries for a list of Enrichment Plans
    """
    # TODO: why is public_event_ymd_column unused
    # pylint: disable=unused-argument

    # pylint: disable=too-many-locals
    # pylint: disable=too-many-arguments
    table_queries = {}
    table_columns = {}
    partition_cols = partition_cols or []

    for idx, enr_plan in enumerate(enrichment_plan_list):
        # Cria o nome da tabela
        optional_number = None if ignore_numbering else idx

        table_name = make_name_function(
            optional_number=optional_number,
            optional_name=enr_plan.source.prefix
        )  # type: ignore

        # Discover the valid partitions
        partitions_list = source_discover_partitions_cross_(
            event_date_df=public_event_ymd_columns_pd,
            source_table_name=enr_plan.source.metadata['table_name'],
            source_info_ymd_column=enr_plan.source.info_ymd_column,
            source_ingestion_column=enr_plan.source.ingestion_column,
            conn=conn,
        )

        # Generate the query
        queries = compile_step(
            plan=enr_plan,
            partition_list=partitions_list,
            public_table_name=public_table_name
        )

        table_queries[table_name] = queries['queries']
        table_columns[table_name] = queries['output_columns']

    # Flatten the queries list
    final_queries = [
        {
            'table_name': table_name,
            'query': query,
            'partition_cols': partition_cols
        }
        for table_name, list_query in table_queries.items()
        for query in list_query
    ]

    return final_queries, table_columns


def compile_final_query(
    conn: AthenaConnection,
    final_table_name: str,
    public_table_name: str,
    columns_dict: Dict[str, Dict[str, str]],
    selected_cols: Optional[List[str]] = None,
    partition_cols: Optional[List[str]] = None,
):  # -> Tuple[str, str]:
    """
    Compile the final query, that unify all variables of the
    enrichment plans in the same query.
    """
    # pylint: disable=too-many-arguments

    # TODO: end this function
    # pylint: disable=unused-argument
    # pylint: disable=forgotten-debug-statement
    # pylint: disable=multiple-statements
    # pylint: disable=import-outside-toplevel
    selected_cols = selected_cols or []
    partition_cols = partition_cols or []

    import pdb; pdb.set_trace()


def compile_enrichment_plans_targets(
    conn: AthenaConnection,
    make_name_function: Callable[[Union[int, None], str], str],
    enrichment_plan_list: Union[None, List[EnrichmentPlan]],

    public_table_name: str,
    public_event_ymd_column: str,
    public_event_ymd_columns_pd: pd.DataFrame,

    partition_cols: Optional[List[str]] = None,
) -> Tuple[List[Dict[str, str]], str]:
    """
    Compile the Enrichment Plan for sources.

    It will use compile_enrichment_plans as base, but will
    run some additional codes for targets (as the code to
    join everything).
    """
    # pylint: disable=too-many-arguments
    enrichment_plan_list = enrichment_plan_list or []

    # If no target is given, return an empty query list and the
    #    public table name given.
    if not enrichment_plan_list:
        return [], public_table_name

    # queries_list, columns_dict =
    queries_list, _ = compile_enrichment_plans(
        conn=conn,
        make_name_function=make_name_function,
        enrichment_plan_list=enrichment_plan_list,
        public_table_name=public_table_name,
        public_event_ymd_column=public_event_ymd_column,
        public_event_ymd_columns_pd=public_event_ymd_columns_pd,
        partition_cols=partition_cols,
        ignore_numbering=False
    )

    # TODO: end this function

    # Create a table with all targets columns
    # final_table_name = make_name_function(
    #     None, 'targets_final_unificado'
    # )

    # compile_final_query(
    #     conn=conn,
    #     final_table_name=final_table_name,
    #     public_table_name=public_table_name,
    #     columns_dict=columns_dict,
    #     partition_cols=partition_cols,
    # )

    return queries_list, public_table_name


def compile_enrichment_plans_sources(
    conn: AthenaConnection,
    make_name_function: Callable[[Union[int, None], str], str],
    enrichment_plan_list: List[EnrichmentPlan],

    public_table_name: str,
    public_event_ymd_column: str,
    public_event_ymd_columns_pd: pd.DataFrame,

    partition_cols: Optional[List[str]] = None,
) -> List[Dict[str, str]]:
    """
    Compile the Enrichment Plan for sources.

    It will use compile_enrichment_plans as base, but will
    run some additional codes for sources.
    """
    # pylint: disable=too-many-arguments
    queries_list, _ = compile_enrichment_plans(
        conn=conn,
        make_name_function=make_name_function,
        enrichment_plan_list=enrichment_plan_list,
        public_table_name=public_table_name,
        public_event_ymd_column=public_event_ymd_column,
        public_event_ymd_columns_pd=public_event_ymd_columns_pd,
        partition_cols=partition_cols,
        ignore_numbering=True
    )

    return queries_list


def get_partition_rows_count_(
    query: str,
    cols: Dict[str, str],
    conn: AthenaConnection,
) -> pd.DataFrame:
    """ Return the partition_count """
    env = jinja2.Environment(undefined=jinja2.StrictUndefined)
    template = env.from_string(r'''
    with tab_ as ({{query}})
    select
    {%- for col, alias in cols.items() %}
    {{col}} as {{alias}}{%if not loop.last%},{%endif%}
    {%- endfor %}
    from tab_
    group by
    {%- for col, alias in cols.items() %}
    {{ loop.index }}{%if not loop.last%},{%endif%}
    {%- endfor %}
    ''')
    return conn.query(template.render(query=query, cols=cols))


def source_discover_partitions_cross_(
    event_date_df: pd.DataFrame,

    source_table_name: str,
    source_info_ymd_column: str,
    source_ingestion_column: str,

    conn: AthenaConnection,
):
    """
    Discover the correct Source partition to use to merge
    with public.
    """
    # Capture the info dates
    info_date = get_partition_rows_count_(
        query=f'''select * from {source_table_name} ''',
        cols={
            source_info_ymd_column: 'info_dt',
            source_ingestion_column: 'ingestion_dt',
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
                as_index=False
            )['info_dt'].max()
        ).groupby(
            ['event_dt', 'info_dt'],
            as_index=False
        )['ingestion_dt'].max()
    )

    return [
        row.to_dict()
        for idx, row in max_date.iterrows()
    ]


def compile_public(
    conn: AthenaConnection,
    make_name_function: Callable[[Union[int, None], str], str],
    public: Public,
    partition_cols: Optional[List[str]] = None
) -> Tuple[Dict[str, str], str, str, pd.DataFrame]:
    """
    Create the public table
    """
    partition_cols = partition_cols or []

    public_table_name = make_name_function(None, 'public_enricher')

    public_query_dict = {
        'query': public.metadata['query'],
        'table_name': public_table_name,
        'partition_cols': partition_cols
    }

    public_event_ymd_columns_pd = get_partition_rows_count_(
        query=public.metadata['query'],
        cols={public.event_ymd_column: 'event_dt'},
        conn=conn,
    )

    return (
        public_query_dict,
        public_table_name,
        public.event_ymd_column,
        public_event_ymd_columns_pd,
    )


@dataclass
class AthenaQueries:
    """
    Class to control all the queries generated in the Enricher
    """
    queries_list: List[Dict[str, str]] = field(repr=False)
    partition_cols: List[str] = field(default_factory=list)

    def __getitem__(self, key: int):
        """ Return the item """
        return self.queries_list[key]

    def __len__(self) -> int:
        """ Length of the object """
        return len(self.queries_list)

    def table_names(self):
        """ Return the table names """
        return list(
            {
                qdict['table_name']: 1
                for qdict in self.queries_list
            }
        )

    def public_queries(self):
        """ Return the public query """
        return [self.queries_list[0]]

    def sources_queries(self):
        """ Return the queries """
        return self.queries_list[1:]

    def sources_queries_interleaved(self):
        """ Return the queries, but in a interleaved way """
        queries_index = defaultdict(list)
        for qdict in self.sources_queries():
            queries_index[qdict['table_name']].append(qdict)

        queries_reordered: List[Dict[str, str]] = list(
            filter(
                None,
                chain.from_iterable(
                    zip_longest(*queries_index.values())
                )
            )
        )

        return queries_reordered

    def count_rows(
        self, conn: AthenaConnection
    ) -> Tuple[pd.DataFrame, List[Tuple[str, Exception]]]:
        """ Count the number of lines in each table """
        res = []
        errors = []

        for tab in tqdm(self.table_names()):
            try:
                temp = conn.query.agg(
                    f'select * from {tab}',
                    ['mesref'],
                    cols_after=[
                        (f"'{tab}'", 'table_name')
                    ]
                )
                res.append(temp)
            except Exception as e:  # pylint: disable=broad-exception-caught
                errors.append((tab, e))

        return (
            pd.concat(res).sort_values(['mesref', 'table_name']),
            errors
        )

    def filter_table(self, table_name: str) -> List[Dict[str, str]]:
        """ Filter queries for a table """
        return [
            qdict
            for qdict in self.queries_list
            if qdict['table_name'] == table_name
        ]

    def count_duplicates(
        self, conn: AthenaConnection, cols_to_check: List[str]
    ) -> Tuple[pd.DataFrame, List[Tuple[str, Exception]]]:
        """ Count the duplicates in tables """
        res = []
        errors = []

        for tab in tqdm(self.table_names()):
            try:
                temp = (
                    conn.query(
                        conn.template().agg(
                            query=f'''select * from {tab}''',
                            groupby=cols_to_check,
                            cols_after=[
                                (f"'{tab}'", 'table_name')
                            ]
                        ) + ' where __count__ > 1'
                    )
                )
                res.append(temp)
            except Exception as e:  # pylint: disable=broad-exception-caught
                errors.append((tab, e))

        return (
            pd.concat(res).sort_values(['mesref', 'table_name']),
            errors
        )

    def load_metadatas(
        self, conn: AthenaConnection
    ) -> Dict[str, List[str]]:
        """ Load the metadata of all these tables """
        # All queries are the same, except for some parameters;
        #   so, we will get the last occurence for each
        #   table
        table_queries = {
            tab: query
            for qdict in self.queries_list
            for tab in [qdict['table_name']]
            for query in [qdict['query']]
        }

        table_cols = {
            tab: cols
            for tab, query in tqdm(table_queries.items())
            for metadata in [conn.metadata(query=query)]
            for cols in [list(metadata.columns | metadata.partition_cols)]
        }

        return table_cols

    def get_columns(
        self,
        conn: AthenaConnection
    ) -> Dict[str, List[str]]:
        """ Show all columns and where they belong in the tables """
        metadatas = self.load_metadatas(conn)
        cols_dict = defaultdict(list)

        for tab, cols_list in metadatas.items():
            for col in cols_list:
                cols_dict[col].append(tab)

        return dict(cols_dict)


@dataclass
class AthenaCompiler:
    """
    Compiler for an Athena connection. Generate the SQL queries
    to enrich the public.
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
        """ Captura a conexão """
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

    def __call__(
        self, enricher: Enricher
    ) -> AthenaQueries:
        """ Run the compiler """
        return self.compile(enricher=enricher)

    def compile(
        self,
        enricher: Enricher
    ) -> AthenaQueries:
        """ Compile the queries """
        queries: List[Dict[str, str]] = []

        conn = self.get_conn_()

        # Create public table with the indicated partitions
        (
            public_query_dict,
            public_table_name,
            public_event_ymd_column,
            public_event_ymd_columns_pd,
        ) = compile_public(
            conn=conn,
            make_name_function=self.make_table_name_,
            public=enricher.public,
            partition_cols=self.partition_cols
        )

        queries.append(public_query_dict)

        # Enrich with targets
        (
            target_queries,
            public_table_name
        ) = compile_enrichment_plans_targets(
            conn=conn,
            make_name_function=self.make_table_name_,
            enrichment_plan_list=enricher.targets,
            public_table_name=public_table_name,
            public_event_ymd_column=public_event_ymd_column,
            public_event_ymd_columns_pd=public_event_ymd_columns_pd,
            partition_cols=self.partition_cols,
        )
        queries.extend(target_queries)

        # Source queries
        source_queries = compile_enrichment_plans_sources(
            conn=conn,
            make_name_function=self.make_table_name_,
            enrichment_plan_list=enricher.sources,
            public_table_name=public_table_name,
            public_event_ymd_column=public_event_ymd_column,
            public_event_ymd_columns_pd=public_event_ymd_columns_pd,
            partition_cols=self.partition_cols,
        )
        queries.extend(source_queries)

        return AthenaQueries(queries)
