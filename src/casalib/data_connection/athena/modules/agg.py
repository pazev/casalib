""" Module to run an aggregation query from a function """
from collections import defaultdict
from typing import Callable, Dict, List, Optional

from ..templates import templates_dict

import pandas as pd


def make_agg_sql_(
    query: str,
    groupby: Optional[List[str]] = None,
    count: Optional[List[str]] = None,
    count_distinct: Optional[List[str]] = None,
    sum: Optional[List[str]] = None,
    mean: Optional[List[str]] = None,
    min: Optional[List[str]] = None,
    max: Optional[List[str]] = None,
    percentile: Dict[int, List[str]] = None,
) -> str:
    """
    Create the SQL to calculate the aggregation.

    Run aggregations for the specified columns.

    By default, always return the rows count for the group.

    The percentile param is a dict where the key is the
    percentile to be calculated and the value is a list of
    columns to be used.
    """
    col_ops_dict = defaultdict(list)

    # Functions without parameters
    no_param_function_ = {
        'count': count,
        'count_distinct': count_distinct,
        'sum': sum,
        'mean': mean,
        'min': min,
        'max': max,
    }

    for func, list_vars in no_param_function_.items():
        for var in (list_vars or []):
            col_ops_dict[var].append((func,))

    # Percentile - We have to unpack the dictionary
    percentile = percentile or {}
    for perc, cols in percentile.items():
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
    count: Optional[List[str]] = None,
    count_distinct: Optional[List[str]] = None,
    sum: Optional[List[str]] = None,
    mean: Optional[List[str]] = None,
    min: Optional[List[str]] = None,
    max: Optional[List[str]] = None,
    percentile: Dict[int, List[str]] = None,
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
        count=count,
        count_distinct=count_distinct,
        sum=sum,
        mean=mean,
        min=min,
        max=max,
        percentile=percentile,
    )

    dff = query_function(query_final)

    return dff
