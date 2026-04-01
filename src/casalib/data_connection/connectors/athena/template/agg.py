"""
Aggregation template
"""
from collections import defaultdict
from typing import Dict, List, Optional, Tuple, Union

import jinja2


TEMPLATE = r"""
WITH
input_query_ AS (
    {{ query | indent(4) }}
)
,
cols_before_ AS (
    SELECT
        *
        {%- for sql_code, col in cols_before %}
        , {{ sql_code }} as {{ col }}
        {%- endfor %}
    FROM
        input_query_
)
,
agg_ AS (
SELECT
    {%- if groupby %}
    {%- for col in groupby %}
    {{ col }},
    {%- endfor %}
    {%- endif %}
    {%- for col, op_list in col_ops_dict.items() %}
    {%- for op_tuple in op_list %}
    {%- if op_tuple[0] == 'percentile' %}
    {%- if col in percentile_ignore_values_ %}
    approx_percentile(
        case
            when
                {{col}} not in (
                    {{percentile_ignore_values_[col] | join(', ')}}
                )
                    then {{col}}
        end,
        {{ op_tuple[1] / 100.0 }}
    ) as {{ col }}__percentile_{{op_tuple[1]}},
    {%- else %}
    approx_percentile({{col}}, {{ op_tuple[1] / 100.0 }})
        as {{ col }}__percentile_{{op_tuple[1]}},
    {%- endif %}
    {%- elif op_tuple[0] == 'count_distinct' %}
    count(distinct {{col}}) as {{ col }}__count_distinct,
    {%- elif op_tuple[0] == 'count_null' %}
    count(case when {{col}} is null then {{col}} end) as {{ col }}__count_null,
    {%- else %}
    {{ op_tuple[0] }}({{col}}) as {{ col }}__{{ op_tuple[0] }},
    {%- endif %}
    {%- endfor %}
    {%- endfor %}
    count(*) as __count__
FROM
    cols_before_
{%- if groupby %}
GROUP BY
    {%- for col in groupby %}
    {{ col }}{%if not loop.last%},{% endif %}
    {%- endfor %}
{%- endif %}
)
,
cols_after_ AS (
    SELECT
        *
        {%- for sql_code, col in cols_after %}
        , {{ sql_code }} as {{ col }}
        {%- endfor %}
    FROM
        agg_
)
select * from cols_after_
"""


def make_sql_agg_query_(
    query: str,
    groupby: Optional[List[str]] = None,
    count_: Optional[List[str]] = None,
    count_null_: Optional[List[str]] = None,
    count_distinct_: Optional[List[str]] = None,
    sum_: Optional[List[str]] = None,
    mean_: Optional[List[str]] = None,
    min_: Optional[List[str]] = None,
    max_: Optional[List[str]] = None,
    percentile_: Optional[Dict[int, List[str]]] = None,
    percentile_ignore_values_: Optional[Dict[str, List[float]]] = None,
    cols_before: Optional[List[Union[str, Tuple[str, str]]]] = None,
    cols_after: Optional[List[Union[str, Tuple[str, str]]]] = None,
) -> str:
    """
    Create the SQL to calculate the aggregation.

    Run aggregations for the specified columns.

    By default, always return the rows count for the group.

    The percentile param is a dict where the key is the
    percentile to be calculated and the value is a list of
    columns to be used.
    """
    # pylint: disable=too-many-locals,too-many-arguments

    cols_before_ = cols_before or []
    cols_before_ = [
        elem_adj
        for elem in cols_before_
        for elem_adj in [elem if isinstance(elem, tuple) else (elem, elem)]
    ]

    cols_after_ = cols_after or []
    cols_after_ = [
        elem_adj
        for elem in cols_after_
        for elem_adj in [elem if isinstance(elem, tuple) else (elem, elem)]
    ]

    col_ops_dict: Dict[
        str,
        List[Tuple[str, Optional[int]]]
    ] = defaultdict(list)

    # Functions without parameters
    no_param_function_ = {
        'count': count_,
        'count_distinct': count_distinct_,
        'count_null': count_null_,
        'sum': sum_,
        'avg': mean_,
        'min': min_,
        'max': max_,
    }

    for func, list_vars in no_param_function_.items():
        for var in (list_vars or []):
            col_ops_dict[var].append((func, None))

    # Percentile - We have to unpack the dictionary
    percentile_ = percentile_ or {}
    for perc, cols in percentile_.items():
        for var in cols:
            col_ops_dict[var].append(('percentile', perc))

    percentile_ignore_values_ = percentile_ignore_values_ or {}

    # Generating template
    env = jinja2.Environment(undefined=jinja2.StrictUndefined)
    template = env.from_string(TEMPLATE)

    query_final = template.render(
        query=query,
        col_ops_dict=col_ops_dict,
        groupby=groupby,
        cols_before=cols_before_,
        cols_after=cols_after_,
        percentile_ignore_values_=percentile_ignore_values_,
    )

    return query_final


def make_get_duplicates_(
    query: str,
    keys: List[str],
    samples: Optional[int] = None
) -> str:
    """ Make query to find duplicates """
    query_ = make_sql_agg_query_(
        query=query,
        groupby=keys,
    )

    query_ += ' where __count__ > 1'
    if samples is not None and samples >= 0:
        query_ += f' limit {samples}'

    return query_
