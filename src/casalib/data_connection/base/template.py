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
        cols_before: Optional[List[Union[str, Tuple[str, str]]]] = None,
        cols_after: Optional[List[Union[str, Tuple[str, str]]]] = None,
    ) -> str:
        """ Make aggregation over a query """

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
