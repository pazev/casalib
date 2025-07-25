"""
Create table as.
"""
from typing import List, Optional
import jinja2


TEMPLATE_STR = """
CREATE TABLE {% if schema_name %}{{schema_name}}.{%endif%}{{table_name}}
{%- if location or partition_columns_types %}
WITH (
    {%- if location %}
    external_location='{{ location }}/{% if schema_name %}{{schema_name}}.{%endif%}{{table_name}}'
    {%- endif %}
    {%- if location and partition_columns_types %},{% endif %}
    {%- if partition_columns_types %}
    partitioned_by=ARRAY['{{ partition_columns_types | join("', '")}}']
    {%- endif %}
)
{%- endif %}
AS
WITH
input_query_ AS (
    {{ input_query | indent(4) }}
)

SELECT
    *
FROM
    input_query_
"""


def make_create_ctas(
    input_query: str,
    table_name: str,
    schema_name: Optional[str] = None,
    partition_columns_types: Optional[List[str]] = None,
    location: Optional[str] = None,
) -> str:
    """ Generate CREATE TABLE AS query """
    env = jinja2.Environment()
    template = env.from_string(TEMPLATE_STR)

    return template.render(
        schema_name=schema_name,
        table_name=table_name,
        input_query=input_query,
        partition_columns_types=partition_columns_types,
        location=location
    )
