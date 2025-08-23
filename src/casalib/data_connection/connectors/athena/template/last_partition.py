"""
Template para selecionar a última partição de uma tabela
"""
from typing import Any, List

import jinja2


TEMPLATE = r'''
with
input__ as (
    select * from {{table_name}}
    {%- if filters %}
    where
            1 = 1
        {%- for col, values in filters.items() %}
        and
            {{ col }} in (
                {%- for val in values %}
                {{val}}{%if not loop.last%},{%endif%}
                {%- endfor %}
            )
        {%- endfor %}
    {%- endif %}
)
,
max_partition__ as (
    select
        {%- for col in groupby_cols %}
        {{ col }},
        {%- endfor %}
        max({{max_column}}) as {{max_column}}
    from
        input__
    {% if filters %}
    where
            1 = 1
        {%- for col, values in filters.items() %}
        and
            {{ col }} in (
                {%- for val in values %}
                {{val}}{%if not loop.last%},{%endif%}
                {%- endfor %}
            )
        {%- endfor %}
    {%- endif %}
    {%- if groupby_cols %}
    group by
        {%- for col in groupby_cols %}
        {{ col }}{%if not loop.last%},{%endif%}
        {%- endfor %}
    {%- endif %}
)
,
table_select__ as (
    select input__.* from
            input__
        inner join
            max_partition__
                on
                    {%- for col in groupby_cols %}
                        input__.{{col}} =
                        max_partition__.{{col}}
                    and
                    {%- endfor %}
                        input__.{{max_column}} =
                        max_partition__.{{max_column}}
)
select * from table_select__
'''


def make_sql_last_partition_(
    table_name: str,
    max_column_: str,
    cross_columns_: List[str],
    **filters: List[Any],
) -> str:
    """ Return the last partition for a table """
    if set([max_column_]) - set(cross_columns_):
        raise ValueError(
            'max_column must be in cross_columns'
        )

    groupby_cols = [
        col for col in cross_columns_ if col != max_column_
    ]

    filters_final = {
        col: [repr(val) for val in vals]
        for col, vals in filters.items()
    }

    env = jinja2.Environment(undefined=jinja2.StrictUndefined)
    template_ = env.from_string(TEMPLATE)

    return template_.render(
        table_name=table_name,
        groupby_cols=groupby_cols,
        max_column=max_column_,
        filters=filters_final,
    )
