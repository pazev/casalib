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
        {%- if not cols_ordering %}
        *
        {%- else %}
        {%- for col in cols_ordering %}
        {{ col }}{% if not loop.last %},{% endif %}
        {%- endfor %}
        {%- endif %}
    from
        input_
)
select * from reordering
"""


def make_sql_insert_(
    query: str,
    table_name: str,
    schema_name: Optional[str] = None,
    cols_ordering: Optional[List[str]] = None,
) -> str:
    """ Create a insert INTO query """
    env = jinja2.Environment(undefined=jinja2.StrictUndefined)
    template = env.from_string(TEMPLATE)

    return template.render(
        query=query,
        table_name=table_name,
        schema_name=schema_name,
        cols_ordering=cols_ordering,
    )
