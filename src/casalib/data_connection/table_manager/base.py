""" Module defines an object to manage tables """
from abc import ABC, abstractmethod
from typing import (
    Any,
    Callable,
    Dict,
    List,
    Optional,
    Tuple
)

import pandas as pd

from ..base import ConnectionAbstract, Metadata


class TableManagerAbstract(ABC):
    """ Methods that must be available to manage tables """
    @abstractmethod
    def set_conn_maker(
        self,
        conn_maker: Callable[[], ConnectionAbstract]
    ) -> "TableManagerAbstract":
        """ Set the connection maker """

    @abstractmethod
    def get_conn(self) -> ConnectionAbstract:
        """ Get a connection """

    @abstractmethod
    def get_table_name(self) -> str:
        """ Returns the table name """

    @abstractmethod
    def get_partition_cols(self) -> Optional[List[str]]:
        """ Returns the configured parittion cols """

    @abstractmethod
    def list_partitions(self) -> Dict[Tuple[str, ...], str]:
        """ List partitions """

    @abstractmethod
    def list_partitions_filter(
        self,
        *filters: str
    ) -> List[Tuple[str, ...]]:
        """
        Filter the partitions list using the filter passed.
        """

    @abstractmethod
    def list_partitions_filter_pd(
        self,
        *filters: str
    ) -> pd.DataFrame:
        """
        Filter the partitions list using the filter passed.
        Returns the result as pd.DataFrame.
        """

    @abstractmethod
    def sample(self, samples: int = 100) -> pd.DataFrame:
        """
        Select some sample from the table.
        """

    @abstractmethod
    def metadata(self) -> Metadata:
        """ Returns the table metadata """

    @abstractmethod
    def last_partition_query(
        self,
        cross_columns_: Optional[List[str]] = None,
        max_column_: Optional[str] = None,
        **filters: List[Any],
    ) -> List[str]:
        """ Return last partition """

    @abstractmethod
    def drop(self) -> "TableManagerAbstract":
        """ Drop the table """

    @abstractmethod
    def drop_partitions(
        self,
        partitions_to_drop: List[Tuple[str, ...]]
    ) -> "TableManagerAbstract":
        """ Drop partitions """

    @abstractmethod
    def drop_partitions_filter(
        self,
        *filters: str
    ) -> "TableManagerAbstract":
        """
        Drop partitions using the fnmatch filter passed.
        """

    @abstractmethod
    def get_table_input(self) -> List[str]:
        """ Return the input tables """

    @abstractmethod
    def input_vars(self) -> List[str]:
        """
        List the variables necessary to run the TableManager
        """

    @abstractmethod
    def run(
        self, **kwargs
    ) -> "TableManagerAbstract":
        """ Run the procedure that generate the table """
