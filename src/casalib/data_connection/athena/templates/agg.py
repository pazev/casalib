"""
Aggregation template
"""

TEMPLATE = """
WITH
input_query_ AS (
    {{ query | indent(4) }}
)

SELECT
    {%- if groupby %}
    {%- for col in groupby %}
    {{ col }},
    {%- endfor %}
    {%- endif %}
    {%- for col, op_list in col_ops_dict.items() %}
    {%- for op_tuple in op_list %}
    {%- if op_tuple[0] == 'percentile' %}
    approx_percentile({{col}}, {{ op_tuple[1] / 100.0 }}) as {{ col }}__percentile_{{op_tuple[1]}},
    {% elif op_tuple[0] == 'count_distinct' %}
    count(distinct {{col}}) as {{ col }}__count_distinct,
    {%- else %}
    {{ op_tuple[0] }}({{col}}) as {{ col }}__{{ op_tuple[0] }},
    {%- endif %}
    {%- endfor %}
    {%- endfor %}
    count(*) as __count__
FROM
    input_query_
{%- if groupby %}
GROUP BY
    {%- for col in groupby %}
    {{ col }}{%if not loop.last%},{% endif %}
    {%- endfor %}
{%- endif %}
"""