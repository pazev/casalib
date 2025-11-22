"""
Module with the function to operate over queries.
"""
from typing import Dict, List, Optional, Tuple

import jinja2


TEMPLATE = '''
with
input_ as (
    {{query|indent(4)}}
)
,
renaming_add_cols_ as (
    select
        *
        {%- if renaming_add_cols %}
        ,
        {%- for sql_code, alias in renaming_add_cols.items() %}
        {{ sql_code }} as {{ alias }}{%if not loop.last%},{%endif%}
        {%- endfor %}
        {%- endif %}
    from
        input_
)
,
select_exclude_ as (
    select
        {%- if not cols_final %}
        *
        {%- else %}
        {%- for col in cols_final %}
        {{col}}{%if not loop.last%},{%endif%}
        {%- endfor %}
        {%- endif %}
    from
        renaming_add_cols_
)
select * from select_exclude_
'''


def make_op_query_(
    query: str,
    query_cols: List[str],
    rename: Optional[List[Tuple[str, str]]] = None,
    add_cols: Optional[List[Tuple[str, str]]] = None,
    select: Optional[List[str]] = None,
    exclude: Optional[List[str]] = None,
) -> str:
    """
    Generate a query to operate over a query
    """
    # pylint: disable=too-many-arguments

    # Now, we need to set all info.
    #   1. All columns in rename must be in the query_cols
    #   2. No column in add_cols may have the same name of
    #       columns in the query or in renaming
    #   3. All columns created must
    #
    rename_dict = {
        col_ren: col
        for col, col_ren in (rename or [])
    }
    add_cols_dict = {
        alias: sql_code
        for sql_code, alias in (add_cols or [])
    }
    select = select or []
    exclude = exclude or []

    # Renaming columns
    check_errors_duplicates_(query_cols, rename_dict, add_cols_dict)
    renaming_add_cols, final_cols = make_final_cols_(
        query_cols, rename_dict, add_cols_dict, select, exclude
    )

    # Render the query
    env = jinja2.Environment(undefined=jinja2.StrictUndefined)
    template = env.from_string(TEMPLATE)

    return template.render(
        query=query,
        renaming_add_cols=renaming_add_cols,
        final_cols=final_cols,
    )


def make_final_cols_(
    query_cols: List[str],
    rename_dict: Dict[str, str],
    add_cols_dict: Dict[str, str],
    select: List[str],
    exclude: List[str],
) -> Tuple[Dict[str, str], List[str]]:
    """ Make final cols """
    renaming_add_cols = rename_dict | add_cols_dict
    all_cols = [*query_cols, *rename_dict, *add_cols_dict]

    if select:
        all_cols = [c for c in all_cols if c in set(select)]

    if exclude:
        all_cols = [c for c in all_cols if c not in set(exclude)]

    return renaming_add_cols, all_cols


def check_errors_duplicates_(
    query_cols: List[str],
    rename_dict: Dict[str, str],
    add_cols_dict: Dict[str, str],
) -> None:
    """
    Check if there is any duplication on the query columns.
    Raise errors if there is any error.
    """
    # Renaming columns
    missing_renaming = set(rename_dict.values()) - set(query_cols)
    if missing_renaming:
        raise ValueError(f'Missing columns: {missing_renaming}')

    # Duplicated columns
    duplicate_cols = (
        set(rename_dict) & set(add_cols_dict) |
        set(rename_dict) & set(query_cols) |
        set(add_cols_dict) & set(query_cols)
    )
    if duplicate_cols:
        raise ValueError(f'Duplicated columns: {duplicate_cols}')
