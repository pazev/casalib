"""
Template to run a LEFT JOIN
"""
from collections import defaultdict
from typing import List, Optional, Tuple, Union

import jinja2


TEMPLATE = r'''
{%- set root_alias = query_alias.keys()|first %}
with
{%- for alias, query in query_alias.items() %}
{{alias}} as (
    {{query | indent(4)}}
)
,
{%- endfor %}
join_ as (
    select
        {%- for col_ren, list_orig in cols_table_dict.items() %}
        {%- set alias = list_orig[0][0] %}
        {%- set col = list_orig[0][1] %}
        {%- if col != col_ren %}
        {{alias}}.{{col}} as {{col_ren}}{%if not loop.last%},{%endif%}
        {%- else %}
        {{alias}}.{{col}}{%if not loop.last%},{%endif%}
        {%- endif %}
        {%- endfor %}
    from
        {%- for alias, query in query_alias.items() %}
        {%- if loop.first %}
            {{alias}}
        {%- else %}
        left join
            {{alias}}
                on
                    {%- for col in join_cols %}
                        {{root_alias}}.{{col}} = {{alias}}.{{col}}
                    {%- if not loop.last %}
                    and
                    {%- endif%}
                    {%- endfor %}
        {%- endif %}
        {%- endfor %}
)
,
cols_ as (
    select
        *
        {%- if cols_after %}
        ,
        {%- for sql_code, alias in cols_after %}
        {{sql_code}} as {{alias}}{%if not loop.last%},{%endif%}
        {%- endfor %}
        {%- endif %}
    from
        join_
)
select * from cols_
{%- if samples %}
limit {{ samples }}
{%- endif %}
'''


def make_sql_left_join_(
    root_query_cols: Tuple[str, List[Union[str, Tuple[str, str]]]],
    other_queries_cols: List[Tuple[str, List[Union[str, Tuple[str, str]]]]],
    join_cols: List[str],
    cols_to_add_suffix: Optional[List[str]] = None,
    cols_after: Optional[List[Tuple[str, str]]] = None,
    select_cols: Optional[List[str]] = None,
    samples: Optional[int] = None,
) -> str:
    """ Generate a LEFT JOIN query """
    # pylint: disable=too-many-locals,too-many-arguments

    # 1. Generate the dictionaries
    #   a. aliases
    #   b. columns renamed
    #       - all columns must be tuples (replicate the
    #           first element if necessary)
    #       - add suffix (alias) to the column indicated
    cols_to_add_suffix = cols_to_add_suffix or []
    cols_after = cols_after or []
    select_cols = select_cols or []

    queries_cols_alias_dict = {
        key: query_col_tuple
        for idx, query_col_tuple in enumerate([
            root_query_cols,
            *other_queries_cols
        ])
        for key in ['query_root' if idx == 0 else f'query{idx}']
    }

    query_alias = {
        key: query_col_tuple[0]
        for key, query_col_tuple in queries_cols_alias_dict.items()
    }

    table_cols_dict = {
        key: [
            # This list comprehension:
            #   1. Make all elements of passed columns tuples
            #   2. Adjust the name of the columns, adding
            #       the alias as suffix;
            (col, col_ren_final)
            for c in list_cols
            for col_tuple in [
                c if isinstance(c, tuple) else (c, c)
            ]
            for col, col_ren in [col_tuple]
            for col_ren_final in [
                col_ren
                if col_ren not in cols_to_add_suffix or idx == 0
                else f'{col_ren}_{key}'
            ]
        ]
        for idx, (key, query_col_tuple) in enumerate(
            queries_cols_alias_dict.items()
        )
        for list_cols in [query_col_tuple[1]]
    }

    cols_table_dict = defaultdict(list)
    for table, list_cols_ren in table_cols_dict.items():
        for col, col_ren in list_cols_ren:
            cols_table_dict[col_ren].append((table, col))

    cols_table_dict_final = dict(cols_table_dict)
    if select_cols:
        cols_table_dict_final = {
            col_ren: tuple_ren
            for col_ren, tuple_ren in cols_table_dict.items()
            if col_ren in select_cols
        }

    env = jinja2.Environment(undefined=jinja2.StrictUndefined)
    template = env.from_string(TEMPLATE)

    return template.render(
        query_alias=query_alias,
        cols_table_dict=cols_table_dict_final,
        join_cols=join_cols,
        cols_after=cols_after,
        samples=samples
    )
