""" Module adds a simple TableManager, to manage some
    operations as drop, drop_partition, list_partition.
"""
from dataclasses import dataclass
from typing import (
    Any, Callable, Dict, List, Optional, Tuple
)

import pandas as pd

from ..base import ConnectionAbstract, Metadata

from .base import TableManagerAbstract
from .helpers import TableHelper


@dataclass
class OtherOwnerTableManager(TableManagerAbstract):
    """ TableManager to deal with table and partition
        operations as list_partitions, sample, metadata,
        last_partition_query.

        It doesn't implement the drop, drop_partitions and
        drop_partitions_filter.
    """
    table_name: str

    def __post_init__(self):
        """ Post-init """
        self.helper = TableHelper(
            table_name=self.table_name
        )

    def set_conn_maker(
        self,
        conn_maker: Callable[[], ConnectionAbstract]
    ) -> "OtherOwnerTableManager":
        """ Set the connection maker """
        self.helper.set_conn_maker(conn_maker)
        return self

    def get_conn(self) -> ConnectionAbstract:
        """ Return the connection """
        return self.helper.get_conn()

    def get_table_name(self) -> str:
        """ Returns the table manager """
        return self.helper.get_table_name()

    def get_partition_cols(self) -> Optional[List[str]]:
        """ Returns the configured parittion cols """
        return None

    def list_partitions(self) -> Dict[Tuple[str, ...], str]:
        """ List partitions """
        return self.helper.list_partitions()

    def list_partitions_filter(
        self,
        *filters: str
    ) -> List[Tuple[str, ...]]:
        """
        Filter the partitions list using the filter passed.
        """
        filtered_partitions = (
            self.helper.list_partitions_filter(*filters)
        )
        return filtered_partitions

    def list_partitions_filter_pd(
        self,
        *filters: str
    ) -> pd.DataFrame:
        """
        Filter the partitions list using the filter passed.
        Return the result as pandas DataFrame
        """
        filtered_partitions = (
            self.helper.list_partitions_filter_pd(*filters)
        )
        return filtered_partitions

    def sample(self, samples: int = 100) -> pd.DataFrame:
        """
        Select some sample from the table.
        """
        return self.helper.sample(samples)

    def metadata(self) -> Metadata:
        """ Returns the table metadata """
        return self.helper.metadata()

    def last_partition_query(
        self,
        cross_columns_: Optional[List[str]] = None,
        max_column_: Optional[str] = None,
        **filters: List[Any],
    ) -> List[str]:
        """ Return queries to filter the last partition """
        return self.helper.last_partition_query(
            cross_columns_=cross_columns_,
            max_column_=max_column_,
            **filters
        )

    def drop(self) -> "OtherOwnerTableManager":
        """ Drop the table """
        raise NotImplementedError(
            "You cannot drop a table you don't own."
        )

    def drop_partitions(
            self,
            partitions_to_drop: List[Tuple[str, ...]]
        ) -> "OtherOwnerTableManager":
        """ Drop partitions """
        raise NotImplementedError(
            "You cannot drop partitions from a table you "
            "don't own."
        )

    def drop_partitions_filter(
        self,
        *filters: str
    ) -> "OtherOwnerTableManager":
        """
        Drop partitions using the fnmatch filter passed.
        """
        raise NotImplementedError(
            "You cannot drop partitions from a table you "
            "don't own."
        )

    def run(self, **kwargs) -> "OtherOwnerTableManager":
        """ Run the procedure that generate the table """
        return self

    def get_table_input(self) -> List[str]:
        """ Return the input tables """
        return []

    def input_vars(self) -> List[str]:
        """
        List the variables necessary to run the TableManager
        """
        return []
