from dataclasses import dataclass
from typing import Dict, List, Optional, Tuple, Union

import pandas as pd

from .base_connection import BaseConnectionAbstract
from .make_queries import MakeQueryAbstract


@dataclass
class Querier:
    conn_: BaseConnectionAbstract
    make_queries_: MakeQueryAbstract

    def __call__(self, query: str) -> pd.DataFrame:
        """
        Run a query into the database connection.
        """
        return self.conn_.query(query=query)

    # Queries
    def agg_query(
        self,
        query: str,
        groupby: Optional[List[str]] = None,
        count_: Optional[List[str]] = None,
        count_distinct_: Optional[List[str]] = None,
        sum_: Optional[List[str]] = None,
        mean_: Optional[List[str]] = None,
        min_: Optional[List[str]] = None,
        max_: Optional[List[str]] = None,
        percentile_: Optional[Dict[int, List[str]]] = None,
        cols_before: Optional[List[Union[str, Tuple[str, str]]]] = None,
        cols_after: Optional[List[Union[str, Tuple[str, str]]]] = None,
        sort: bool = False
    ) -> pd.DataFrame:
        """ Realiza uma agregação na query indicada """
        # pylint: disable=too-many-arguments
        groupby_: List[str] = groupby or []

        query_to_exec = self.make_queries_.agg_query(
            query=query,
            groupby=groupby,
            count_=count_,
            count_distinct_=count_distinct_,
            sum_=sum_,
            mean_=mean_,
            min_=min_,
            max_=max_,
            percentile_=percentile_,
            cols_before=cols_before,
            cols_after=cols_after,
        )[0]

        result = self.conn_.query(
            query=query_to_exec
        )

        if bool(groupby_) and sort:
            return result.sort_values(groupby_)

        return result

    def get_duplicates(
        self,
        query: str,
        keys: List[str],
        sample: Optional[int] = None
    ) -> pd.DataFrame:
        """ Query to find duplicates in the given query """
        query_ = self.make_queries_.agg_query(
            query=query, groupby=keys
        )[0]

        query_ = f'''{query_} where __count__ > 1'''
        if sample is not None and sample > 0:
            query_ += f' limit {sample}'

        return self.conn_.query(query_)
