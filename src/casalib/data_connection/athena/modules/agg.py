""" Module to run an aggregation query from a function """
# pylint: disable=too-many-arguments
from collections import defaultdict
from typing import Callable, Dict, List, Optional, Tuple

import pandas as pd

from ..templates import templates_dict



def make_agg_sql_(
    query: str,
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
    Create the SQL to calculate the aggregation.

    Run aggregations for the specified columns.

    By default, always return the rows count for the group.

    The percentile param is a dict where the key is the
    percentile to be calculated and the value is a list of
    columns to be used.
    """
    # pylint: disable=too-many-locals
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

    query_final = templates_dict['agg'].render(
        query=query,
        col_ops_dict=col_ops_dict,
        groupby=groupby
    )

    return query_final


def agg_query(
    query_function: Callable[[str], pd.DataFrame],
    query: str,
    groupby: Optional[List[str]] = None,
    count_: Optional[List[str]] = None,
    count_distinct_: Optional[List[str]] = None,
    sum_: Optional[List[str]] = None,
    mean_: Optional[List[str]] = None,
    min_: Optional[List[str]] = None,
    max_: Optional[List[str]] = None,
    percentile_: Optional[Dict[int, List[str]]] = None,
) -> pd.DataFrame:
    """
    Run aggregations for the specified columns.

    By default, always return the rows count for the group.

    The percentile param is a dict where the key is the
    percentile to be calculated and the value is a list of
    columns to be used.
    """
    query_final = make_agg_sql_(
        query=query,
        groupby=groupby,
        count_=count_,
        count_distinct_=count_distinct_,
        sum_=sum_,
        mean_=mean_,
        min_=min_,
        max_=max_,
        percentile_=percentile_,
    )

    dff = query_function(query_final)

    return dff
