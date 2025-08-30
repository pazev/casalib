"""
Template para operação de criar uma tabela através do
resultado de uma query.
"""
from typing import List, Optional

import jinja2


TEMPLATE = r"""
CREATE TABLE {{schema_name}}.{{table_name}}
WITH (
    {%- if location %}
    external_location='{{ location }}'
    {%- endif %}
    {%- if location and partition_columns_types %}
    ,
    {%- endif %}
    {%- if partition_columns_types %}
    partitioned_by=ARRAY['{{ partition_columns_types | join("', '")}}']
    {%- endif %}
)
AS
WITH
input_query_ AS (
    {{ query | indent(4) }}
)

SELECT
    {%- if not cols_ordering %}
    *
    {%- else %}
    {%- for col in cols_ordering %}
    {{col}}{% if not loop.last %},{% endif %}
    {%- endfor %}
    {%- endif %}
FROM
    input_query_
"""


def make_sql_create_ctas_(
    query: str,
    table_name: str,
    partition_cols: Optional[List[str]] = None,
    schema_name: Optional[str] = None,
    location: Optional[str] = None,
    cols_ordering: Optional[List[str]] = None,
) -> str:
    """ Generate the CTAS query """
    env = jinja2.Environment(undefined=jinja2.StrictUndefined)
    template = env.from_string(TEMPLATE)

    return template.render(
        query=query,
        table_name=table_name,
        partition_columns_types=partition_cols,
        schema_name=schema_name,
        location=location,
        cols_ordering=cols_ordering,
    )
