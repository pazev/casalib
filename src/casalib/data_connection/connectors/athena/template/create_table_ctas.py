"""
Template para operação de criar uma tabela através do
resultado de uma query.
"""

TEMPLATE = r"""
CREATE TABLE {{schema_name}}.{{table_name}}
WITH (
    external_location='{{ s3_output }}/{{ location }}'
    {%- if partition_columns_types %}
    ,
    partitioned_by=ARRAY['{{ partition_columns_types | join("', '")}}']
    {%- endif %}
)
AS
WITH
input_query_ AS (
    {{ query | indent(4) }}
)

SELECT
    {%- for col, type in columns_types.items() %}
    {{col}}{% if not loop.last %},{% endif %}
    {%- endfor %}
    {%- if partition_columns_types %}
    ,
    {%- for col, type in partition_columns_types.items() %}
    {{col}}{% if not loop.last %},{% endif %}
    {%- endfor %}
    {%- endif %}
FROM
    input_query_
"""