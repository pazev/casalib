"""
Module that generate queries for operations
"""
# pylint: disable=too-many-arguments

from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import Any, Dict, List, Optional


@dataclass
class TemplateAbstract(ABC):
    """ Class to create queries when requested """
    @abstractmethod
    @staticmethod
    def split_schema_name(
        table_name: str,
        schema_name: Optional[str] = None,
    ) -> str:
        """ Function to normalize the table name """

    @abstractmethod
    @staticmethod
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
    ) -> str:
        """ Make aggregation over a query """

    @abstractmethod
    @staticmethod
    def create_schema(
        table_name: str,
        columns_types: Dict[str, str],
        partition_cols_types: Dict[str, str],
        schema_name: Optional[str] = None,
        location: Optional[str] = None,
    ) -> str:
        """ Generate a CREATE TABLE with schema query """

    @abstractmethod
    @staticmethod
    def create_ctas(
        query: str,
        table_name: str,
        partition_cols: Optional[List[str]] = None,
        schema_name: Optional[str] = None,
        location: Optional[str] = None,
    ) -> str:
        """ Generate a CREATE TABLE AS (CTAS) query
        """

    @abstractmethod
    @staticmethod
    def insert_table(
        query: str,
        table_name: str,
        schema_name: Optional[str] = None,
        location: Optional[str] = None,
    ) -> str:
        """ Generate a INSERT INTO query
        """

    @abstractmethod
    @staticmethod
    def last_partition(
        table_name: str,
        cross_columns_: List[str],
        max_column_: str,
        **filters: List[Any]
    ) -> List[str]:
        """ Generate a query to retrieve the last partition
            of a table.
        """
