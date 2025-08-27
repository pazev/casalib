"""
Helpers to implement the methods required by base
classes
"""
from dataclasses import dataclass, field
from typing import (
    Any,
    Callable,
    Dict,
    List,
    Optional,
    Type,
    Tuple,
)

import pandas as pd

from ...base import ConnectionAbstract, Metadata


@dataclass
class TableHelper():
    """ Query Manager """
    table_name: str
    conn_maker: Optional[
        Callable[[], ConnectionAbstract]
    ] = field(default=None, init=False, repr=False)

    def set_conn_maker(
        self,
        conn_maker: Callable[[], ConnectionAbstract]
    ) -> "TableHelper":
        """ Set the connection maker """
        self.conn_maker = conn_maker
        return self

    def get_conn(self) -> ConnectionAbstract:
        """ Get the conn maker """
        if self.conn_maker is None:
            raise AttributeError(
                'The conn_maker attribute was not '
                'initialized'
            )
        return self.conn_maker()

    def get_table_name(self) -> str:
        """ Return the table name """
        return self.get_conn().utils.schema_tablename(
            table_name=self.table_name
        )

    def list_partitions(self) -> Dict[Tuple[str, ...], str]:
        """ List partitions """
        return self.get_conn().list_partitions(
            table_name=self.table_name
        )

    def list_partitions_filter(
        self,
        *filters: str
    ) -> List[Tuple[str, ...]]:
        """
        Filter the partitions list using the filter passed.
        """
        return self.get_conn().list_partitions_filter(
            self.table_name, *filters
        )

    def list_partitions_filter_pd(
        self,
        *filters: str
    ) -> pd.DataFrame:
        """
        Filter the partitions list using the filter passed.
        Return result as DataFrame.
        """
        conn = self.get_conn()

        return conn.list_partitions_filter_pd(
            self.table_name, *filters
        )

    def sample(self, samples: int = 100) -> pd.DataFrame:
        """
        Select some sample from the table.
        """
        return self.get_conn().table(
            table_name=self.table_name,
            samples=samples
        )

    def metadata(self) -> Metadata:
        """ Returns the table metadata """
        return self.get_conn().metadata(
            table_name=self.table_name
        )

    def last_partition_query(
        self,
        connection_type: Type[ConnectionAbstract],
        cross_columns_: Optional[List[str]] = None,
        max_column_: Optional[str] = None,
        **filters: List[Any],
    ) -> List[str]:
        """ Return last partition """
        meta = self.get_conn().metadata(
            table_name=self.table_name
        )

        cross_columns_f = (
            cross_columns_ or list(meta.partition_cols)
        )
        max_column_f = max_column_ or cross_columns_f[-1]

        query = connection_type.template().last_partition(
            table_name=self.table_name,
            cross_columns_=cross_columns_f,
            max_column_=max_column_f,
            **filters
        )

        return [query]

    def drop(self) -> "TableHelper":
        """ Drop the table """
        self.get_conn().drop(self.table_name)
        return self

    def drop_partitions(
        self,
        partitions_to_drop: List[Tuple[str, ...]]
    ) -> "TableHelper":
        """ Drop partitions """
        self.get_conn().drop_partitions(
            table_name=self.table_name,
            partitions_to_drop=partitions_to_drop
        )
        return self

    def drop_partitions_filter(
        self,
        *filters: str
    ) -> "TableHelper":
        """
        Drop partitions using the fnmatch filter passed.
        """
        self.get_conn().drop_partitions_filter(
            self.table_name, *filters
        )
        return self

    def drop_partitions_filter_pd(
        self,
        *filters: str
    ) -> "TableHelper":
        """
        Drop partitions using the fnmatch filter passed.
        """
        self.get_conn().drop_partitions_filter(
            self.table_name, *filters
        )
        return self
