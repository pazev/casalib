"""
Table Manager Abstract: class with template, using
TableHelper to implement several aspects of TableManager
"""
from abc import abstractmethod
from dataclasses import dataclass
from typing import (
    Any,
    Callable,
    Dict,
    List,
    Optional,
    Type,
    Tuple
)

import pandas as pd

from ...base import ConnectionAbstract, Metadata
from .base_table_manager import BaseTableManagerAbstract
from ._helpers import TableHelper


@dataclass(kw_only=True)
class TableManagerAbstract(BaseTableManagerAbstract):
    """ Methods that must be available to manage tables """
    table_name: str
    connection_type: Type[ConnectionAbstract]
    partition_cols: Optional[List[str]] = None

    def __post_init__(self) -> None:
        """ Initialization """
        self.helper = TableHelper(self.table_name)

    def get_helper(self) -> TableHelper:
        """ Return a TableHelper """
        return self.helper

    def get_conn_type(self) -> Type[ConnectionAbstract]:
        """ Get connection type """
        return self.connection_type

    def set_conn_maker(
        self,
        conn_maker: Callable[[], ConnectionAbstract]
    ) -> "TableManagerAbstract":
        """ Set the connection maker """
        self.get_helper().set_conn_maker(conn_maker)
        return self

    def get_conn(self) -> ConnectionAbstract:
        """ Get a connection """
        return self.get_helper().get_conn()

    def get_table_name(self) -> str:
        """ Returns the table name """
        return self.get_helper().get_table_name()

    def get_partition_cols(self) -> Optional[List[str]]:
        """ Returns the configured parittion cols """
        return self.partition_cols or []

    def list_partitions(self) -> Dict[Tuple[str, ...], str]:
        """ List partitions """
        return self.get_helper().list_partitions()

    def list_partitions_filter(
        self,
        *filters: str
    ) -> List[Tuple[str, ...]]:
        """
        Filter the partitions list using the filter passed.
        """
        return (
            self
            .get_helper()
            .list_partitions_filter(*filters)
        )

    def list_partitions_filter_pd(
        self,
        *filters: str
    ) -> pd.DataFrame:
        """
        Filter the partitions list using the filter passed.
        Returns the result as pd.DataFrame.
        """
        return (
            self
            .get_helper()
            .list_partitions_filter_pd(*filters)
        )

    def sample(self, samples: int = 100) -> pd.DataFrame:
        """
        Select some sample from the table.
        """
        return self.get_helper().sample(samples)

    def metadata(self) -> Metadata:
        """ Returns the table metadata """
        return self.get_helper().metadata()

    def last_partition_query(
        self,
        cross_columns_: Optional[List[str]] = None,
        max_column_: Optional[str] = None,
        **filters: List[Any],
    ) -> List[str]:
        """ Return last partition """
        return self.get_helper().last_partition_query(
            self.get_conn_type(), cross_columns_,
            max_column_, **filters
        )

    def drop(self) -> "TableManagerAbstract":
        """ Drop the table """
        self.get_helper().drop()
        return self

    def drop_partitions(
        self,
        partitions_to_drop: List[Tuple[str, ...]]
    ) -> "TableManagerAbstract":
        """ Drop partitions """
        self.helper.drop_partitions(partitions_to_drop)
        return self

    def drop_partitions_filter(
        self,
        *filters: str
    ) -> "TableManagerAbstract":
        """
        Drop partitions using the fnmatch filter passed.
        """
        self.helper.drop_partitions_filter(*filters)
        return self

    def drop_partitions_filter_pd(
        self,
        *filters: str
    ) -> "TableManagerAbstract":
        """
        Drop partitions using the fnmatch filter passed.
        """
        self.helper.drop_partitions_filter(*filters)
        return self

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
        self, **kwargs: Any
    ) -> "TableManagerAbstract":
        """ Run the procedure that generate the table """
