"""
Template para selecionar a última partição de uma tabela
"""

TEMPLATE = r'''
with
input__ as (
    select * from {{table}}
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
        left join
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
