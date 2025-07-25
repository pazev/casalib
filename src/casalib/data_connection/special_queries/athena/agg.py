"""
Aggregation template
"""
from collections import defaultdict
from typing import Dict, List, Optional, Tuple
import jinja2


TEMPLATE_STR = """
WITH
input_query_ AS (
    {{ input_query | indent(4) }}
)

SELECT
    {%- if groupby %}
    {%- for col in groupby %}
    {{ col }},
    {%- endfor %}
    {%- endif %}
    {%- for col, op_list in col_ops_dict.items() %}
    {%- for op_tuple in op_list %}
    {%- if op_tuple[0] == 'percentile' %}
    approx_percentile({{col}}, {{ op_tuple[1] / 100.0 }}) as {{ col }}__percentile_{{op_tuple[1]}},
    {%- elif op_tuple[0] == 'count_distinct' %}
    count(distinct {{col}}) as {{ col }}__count_distinct,
    {%- else %}
    {{ op_tuple[0] }}({{col}}) as {{ col }}__{{ op_tuple[0] }},
    {%- endif %}
    {%- endfor %}
    {%- endfor %}
    count(*) as __count__
FROM
    input_query_
{%- if groupby %}
GROUP BY
    {%- for col in groupby %}
    {{ col }}{%if not loop.last%},{% endif %}
    {%- endfor %}
{%- endif %}
"""
def make_agg(
    input_query: str,
    groupby: Optional[List[str]] = None,
    count_: Optional[List[str]] = None,
    count_distinct_: Optional[List[str]] = None,
    sum_: Optional[List[str]] = None,
    mean_: Optional[List[str]] = None,
    min_: Optional[List[str]] = None,
    max_: Optional[List[str]] = None,
    percentile_: Optional[Dict[int, List[str]]] = None,
) -> str:
    """
    Generate an aggregation query using the information passed.
    """
    # pylint: disable=too-many-arguments,too-many-locals

    col_ops_dict: Dict[
        str,
        List[Tuple[str, Optional[int]]]
    ] = defaultdict(list)

    # Functions without parameters
    no_param_function_ = {
        'count': count_,
        'count_distinct': count_distinct_,
        'sum': sum_,
        'mean': mean_,
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

    env = jinja2.Environment()
    template = env.from_string(TEMPLATE_STR)
    query_final = template.render(
        input_query=input_query,
        col_ops_dict=col_ops_dict,
        groupby=groupby
    )

    return query_final
