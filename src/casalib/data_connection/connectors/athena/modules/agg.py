""" Module to run an aggregation query from a function """
# pylint: disable=too-many-arguments
from typing import Callable, Dict, List, Optional

import pandas as pd

from ..template import AthenaTemplates


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
    query_final = AthenaTemplates.agg_query(
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
