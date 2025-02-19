TEMPLATE = """
CREATE EXTERNAL TABLE {{schema_name}}.{{table_name}}
(
    {%- for col, type in columns_types.items() %}
    {{col}} {{type}} {% if not loop.last %},{% endif %}
    {%- endfor %}
)
{%- if partition_columns_types %}
PARTITIONED BY (
    {%- for col, type in partition_columns_types.items() %}
    {{col}} {{type}} {% if not loop.last %},{% endif %}
    {%- endfor %}
)
{%- endif %}
STORED AS PARQUET
LOCATION '{{ s3_output }}/{{ location }}'
"""