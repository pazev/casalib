"""
Module that defines the Querier object; several special
methods to run.
"""

from dataclasses import dataclass
from typing import Dict, List, Optional, Tuple, Union

import pandas as pd

from .base_connection import BaseConnectionAbstract
from .make_queries import MakeQueryAbstract


@dataclass
class Querier:
    """ Querier class, with special queries """
    conn_: BaseConnectionAbstract
    make_queries_: MakeQueryAbstract

    def __call__(self, query: str) -> pd.DataFrame:
        """
        Run a query into the database connection.
        """
        return self.conn_.query_method_(query=query)

    # Queries
    def agg(
        self,
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
        sort: bool = False
    ) -> pd.DataFrame:
        """ Performs an aggregation on the indicated query """
        # pylint: disable=too-many-arguments,too-many-locals
        groupby_: List[str] = groupby or []

        query_to_exec = self.make_queries_.agg(
            query=query,
            groupby=groupby,
            count_=count_,
            count_null_=count_null_,
            count_distinct_=count_distinct_,
            sum_=sum_,
            mean_=mean_,
            min_=min_,
            max_=max_,
            percentile_=percentile_,
            percentile_ignore_values_=percentile_ignore_values_,
            cols_before=cols_before,
            cols_after=cols_after,
        )[0]

        result = self.conn_.query_method_(
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
        query_ = self.make_queries_.agg(
            query=query, groupby=keys
        )[0]

        query_ = f'''{query_} where __count__ > 1'''
        if sample is not None and sample > 0:
            query_ += f' limit {sample}'

        return self.conn_.query_method_(query_)

    def left_join(
        self,
        root_query: str,
        other_queries: List[str],
        join_cols: List[str],
        cols_to_add_suffix: Optional[List[str]] = None,
        cols_after: Optional[List[Tuple[str, str]]] = None,
        select_cols: Optional[List[str]] = None,
        samples: Optional[int] = None,
    ) -> pd.DataFrame:
        """
        Return a DataFrame, result of a left join query
        """
        # pylint: disable=too-many-arguments
        query_ = self.make_queries_.left_join(
            root_query=root_query,
            other_queries=other_queries,
            join_cols=join_cols,
            cols_to_add_suffix=cols_to_add_suffix,
            cols_after=cols_after,
            select_cols=select_cols,
            samples=samples,
        )[0]

        return self.conn_.query_method_(query_)

    def op(
        self,
        query: str,
        rename: Optional[List[Tuple[str, str]]] = None,
        add_cols: Optional[List[Tuple[str, str]]] = None,
        select: Optional[List[str]] = None,
        exclude: Optional[List[str]] = None,
    ) -> pd.DataFrame:
        """
        Return a DataFrame with the results of query,
        that rename, select, exclude and add new columns.
        """
        # pylint: disable=too-many-arguments
        query = self.make_queries_.op(
            query=query,
            rename=rename,
            add_cols=add_cols,
            select=select,
            exclude=exclude
        )[0]
        return self.conn_.query_method_(query=query)
