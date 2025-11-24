"""
Module that generate queries for operations
"""
# pylint: disable=too-many-arguments

from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Tuple, Union


@dataclass
class TemplateAbstract(ABC):
    """ Class to create queries when requested """
    @staticmethod
    @abstractmethod
    def split_schema_name(
        table_name: str,
        schema_name: Optional[str] = None,
    ) -> Tuple[str, str]:
        """ Function to normalize the table name """

    @staticmethod
    @abstractmethod
    def process_table_name(
        table_name: str,
        schema_name: Optional[str] = None,
    ) -> str:
        """ Process table_name, adding the default schema if
            needed.
        """

    @staticmethod
    @abstractmethod
    def agg_query(
        query: str,
        groupby: Optional[List[str]] = None,
        count_: Optional[List[str]] = None,
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
        """ Make aggregation over a query """

    @staticmethod
    @abstractmethod
    def list_duplicates(
        query: str, keys: List[str], samples: Optional[int] = None
    ) -> str:
        """ Return query to list all duplicates in a query """

    @staticmethod
    @abstractmethod
    def create_schema(
        table_name: str,
        columns_types: Dict[str, str],
        partition_cols_types: Dict[str, str],
        schema_name: Optional[str] = None,
        location: Optional[str] = None,
    ) -> str:
        """ Generate a CREATE TABLE with schema query """

    @staticmethod
    @abstractmethod
    def create_ctas(
        query: str,
        table_name: str,
        partition_cols: Optional[List[str]] = None,
        schema_name: Optional[str] = None,
        location: Optional[str] = None,
    ) -> str:
        """ Generate a CREATE TABLE AS (CTAS) query
        """

    @staticmethod
    @abstractmethod
    def insert_table(
        query: str,
        table_name: str,
        schema_name: Optional[str] = None,
    ) -> str:
        """ Generate a INSERT INTO query
        """

    @staticmethod
    @abstractmethod
    def last_partition(
        table_name: str,
        cross_columns_: List[str],
        max_column_: str,
        **filters: List[Any]
    ) -> str:
        """ Generate a query to retrieve the last partition
            of a table.
        """

    @staticmethod
    @abstractmethod
    def left_join(
        root_query_cols: Tuple[str, List[Union[str, Tuple[str, str]]]],
        other_queries_cols: List[Tuple[str, List[Union[str, Tuple[str, str]]]]],
        join_cols: List[str],
        cols_to_add_suffix: Optional[List[str]] = None,
        cols_after: Optional[List[Tuple[str, str]]] = None,
        select_cols: Optional[List[str]] = None,
        samples: Optional[int] = None,
    ) -> str:
        """
        Generate a query to LEFT JOIN several queries to a
        root one
        """

    @staticmethod
    @abstractmethod
    def op(
        query: str,
        query_cols: List[str],
        rename: Optional[List[Tuple[str, str]]] = None,
        add_cols: Optional[List[Tuple[str, str]]] = None,
        select: Optional[List[str]] = None,
        exclude: Optional[List[str]] = None,
    ) -> str:
        """
        Generate a new query, that rename, select, exclude
        and add new columns.
        """
