"""
Template to run a LEFT JOIN
"""
import textwrap
from typing import Any, List, Optional, Tuple, Union

import jinja2


TEMPLATE = r'''
with
{%- for alias, query in queries_alias.items() %}
input_{{alias}} as (
    {{query | indent(4)}}
)
,
{%- endfor %}
{%- for alias, cols_renaming_dict in tabs_cols.items() %}
{{alias}} as (
    select
        {%- for col, col_ren in cols_renaming_dict.items() %}
        {%- if col != col_ren %}
        {{col}} as {{col_ren}}{%if not loop.last%},{%endif%}
        {%- else %}
        {{col}}{%if not loop.last%},{%endif%}
        {%- endif %}
        {%- endfor %}
    from input_{{alias}}
)
,
{%- endfor %}
join_ as (
    select
        {%- for tab, col in cols_list_final %}
        {{tab}}.{{col}}{%if not loop.last%},{%endif%}
        {%- endfor %}
    from
        {%- for alias, query in queries_alias.items() %}
        {%- if loop.first %}
            {{alias}}
        {%- else %}
        left join
            {{alias}}
                on
                    {%- for col in join_cols %}
                        query_root_.{{col}} = {{alias}}.{{col}}
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
'''


def make_sql_left_join(
    root_query: str,
    root_columns: List[str, Tuple[str, str]],
    other_queries_columns: List[
        Tuple[
            str,
            List[
                Union[
                    str,
                    Tuple[str, str]
                ]
            ]
        ]
    ],
    join_cols: List[str],
    cols_to_add_suffix: Optional[List[str]] = None,
    cols_after: Optional[List[Tuple[str, str]]] = None,
) -> str:
    """ Generate a LEFT JOIN query """
