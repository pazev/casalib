"""
Template para criar uma tabela através da especificação de
um schema.
"""
import re
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
{%- if location %}
LOCATION '{{ location }}'
{%- endif %}
"""


def treat_column_type(
    column_type: str
) -> str:
    """ Adjust the column type """
    if 'array' in column_type:
        column_type = re.sub(
            r'array\((.*)\)',
            r'array<\1>',
            column_type
        )

    if 'real' in column_type:
        return column_type.replace('real', 'double')

    if 'varchar' in column_type:
        return re.sub(
            r'varchar(\(\d+\))?',
            r'string',
            column_type
        )

    if 'timestamp' in column_type:
        return 'timestamp'

    return column_type


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

    columns_types = {
        col_: treat_column_type(type_)
        for col_, type_ in columns_types.items()
    }

    partition_cols_types = {
        col_: treat_column_type(type_)
        for col_, type_ in partition_cols_types.items()
    }

    return template.render(
        table_name=table_name,
        columns_types=columns_types,
        partition_columns_types=partition_cols_types,
        schema_name=schema_name,
        location=location
    )
