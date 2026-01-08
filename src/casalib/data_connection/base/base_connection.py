"""
Module defines the base connection class, that will be used
to perform the tasks with the database.

All advanced features will be implemented on
ConnectionAbstract.
"""
# pylint: disable=too-many-arguments
from abc import ABC, abstractmethod
from typing import Dict, List, Optional, Tuple, Union

import pandas as pd

from ._metadata import Metadata


class BaseConnectionAbstract(ABC):
    """
    Abstract connection class containing the minimum methods
    required to function.
    """
    @abstractmethod
    def query_method_(self, query: str) -> pd.DataFrame:
        """
        Returns the query result as a DataFrame.
        """

    @abstractmethod
    def table(
        self,
        table_name: str,
        samples: Union[int, None] = 100
    ) -> pd.DataFrame:
        """
        Returns a sample of the table. The default is 100
        records, but this number can be changed in the
        `samples` parameter. If `samples` is negative
        or None, it returns the entire table.
        """

    @abstractmethod
    def metadata(
        self,
        query: Optional[str] = None,
        table_name: Optional[str] = None,
    ) -> Metadata:
        """
        Returns the metadata for the table or query. Only
        one of the two should be set.
        """

    @abstractmethod
    def drop(self, table_name: str) -> "BaseConnectionAbstract":
        """
        Drops a table.
        """

    @abstractmethod
    def create_insert(
        self,
        query: str,
        table_name: str,
        partition_cols: Optional[List[str]] = None,
    ) -> "BaseConnectionAbstract":
        """
        Creates a table if it does not exist and inserts data.

        Reorders columns if necessary.
        """

    @abstractmethod
    def create_ctas(
        self,
        query: str,
        table_name: str,
        partition_cols: Optional[List[str]] = None,
    ) -> "BaseConnectionAbstract":
        """
        Creates a table using the CREATE TABLE AS command.
        """

    @abstractmethod
    def list_partitions(
        self,
        table_name: str,
    ) -> Dict[Tuple[str, ...], str]:
        """
        Lists the partitions.
        """

    @abstractmethod
    def drop_partitions(
        self,
        table_name: str,
        partitions_to_drop: List[Tuple[str, ...]],
    ) -> "BaseConnectionAbstract":
        """
        Drops the specified partitions in the table.
        """

    @abstractmethod
    def send_pandas(
        self,
        dff: pd.DataFrame,
        table_name: str,
        partition_cols: Optional[List[str]] = None,
    ) -> Metadata:
        """
        Sends a pandas DataFrame to the database.
        """

    @abstractmethod
    def get_input_tables(
        self,
        query: str
    ) -> List[str]:
        """ Get the required tables for the given query """
