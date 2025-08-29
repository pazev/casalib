"""
Template para criar uma tabela através da especificação de
um schema.
"""
from typing import Dict, Optional

import jinja2


TEMPLATE = r"""
CREATE EXTERNAL TABLE IF NOT EXISTS
{{schema_name}}.{{table_name}}
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
{% if location %}
LOCATION '{{ location }}'
{% endif %}
"""


def make_sql_create_schema_(
    table_name: str,
    columns_types: Dict[str, str],
    partition_cols_types: Dict[str, str],
    schema_name: str,
    location: Optional[str] = None,
) -> str:
    """ Create a query to CREATE TABLE with schema """
    env = jinja2.Environment(undefined=jinja2.StrictUndefined)
    template = env.from_string(TEMPLATE)

    return template.render(
        table_name=table_name,
        columns_types=columns_types,
        partition_cols_types=partition_cols_types,
        schema_name=schema_name,
        location=location
    )
