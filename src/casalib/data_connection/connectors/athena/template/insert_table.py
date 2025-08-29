"""
Template para inserir dados em uma tabela.
"""
from typing import List, Optional

import jinja2

TEMPLATE = r"""
insert into {{ schema_name }}.{{ table_name }}
with
input_ as (
    {{ query | indent(4) }}
),
reordering as (
    select
        {%- for col in columns_types %}
        {{ col }}{% if not loop.last %},{% endif %}
        {%- endfor %}
    from
        input_
)
select * from reordering
"""


def make_sql_insert_(
    query: str,
    table_name: str,
    schema_name: Optional[str] = None,
) -> str:
    """ Create a insert INTO query """
    env = jinja2.Environment(undefined=jinja2.StrictUndefined)
    template = env.from_string(TEMPLATE)

    return template.render(
        query=query,
        table_name=table_name,
        schema_name=schema_name,
    )
