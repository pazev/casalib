"""
Create table schema.
"""
from typing import Dict, Optional
import jinja2


TEMPLATE_STR = """
CREATE EXTERNAL TABLE {%if schema_name%}{{schema_name}}.{%endif%}{{table_name}}
(
    {%- for col, type in columns_types.items() %}
    {{col}} {{type}}{% if not loop.last %},{% endif %}
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
{% if location %}
LOCATION '{{ location }}/{% if schema_name%}{{schema_name}}.{%endif%}{{table_name}}'
{% endif %}
"""


def make_create_schema(
    columns_types: Dict[str, str],
    table_name: str,
    schema_name: Optional[str] = None,
    partition_columns_types: Optional[Dict[str, str]] = None,
    location: Optional[str] = None,
) -> str:
    """ Generate CREATE TABLE query """
    env = jinja2.Environment()
    template = env.from_string(TEMPLATE_STR)

    return template.render(
        schema_name=schema_name,
        table_name=table_name,
        columns_types=columns_types,
        partition_columns_types=partition_columns_types,
        location=location
    )
