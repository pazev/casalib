""" Module defines an object to manage tables """
from abc import ABC, abstractmethod
from dataclasses import dataclass
from fnmatch import fnmatch
from typing import Callable, Dict, List, Optional, Tuple

import pandas as pd

from ..base import ConnectionAbstract, Metadata


@dataclass
class TableHelper():
    """ Query Manager """
    table_name: str
    conn_maker: Optional[
        Callable[[], ConnectionAbstract]
    ] = None

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

    def drop(self) -> "TableHelper":
        """ Drop the table """
        conn = self.get_conn()
        conn.drop(self.table_name)

        return self

    def drop_partitions(
        self,
        partitions_to_drop: List[Tuple[str, ...]]
    ) -> "TableHelper":
        """ Drop partitions """
        conn = self.get_conn()
        conn.drop_partitions(
            table_name=self.table_name,
            partitions_to_drop=partitions_to_drop
        )
        return self

    def list_partitions(self) -> Dict[Tuple[str, ...], str]:
        """ List partitions """
        conn = self.get_conn()
        return conn.list_partitions(self.table_name)

    def list_partitions_filter(
        self,
        *filters: str
    ) -> List[Tuple[str, ...]]:
        """
        Filter the partitions list using the filter passed.
        """
        conn = self.get_conn()

        filtered_partitions = [
            part
            for part, _ in (
                conn
                .list_partitions(self.table_name)
                .items()
            )
            for filter_ in [
                all(map(
                    lambda part_patt_: fnmatch(*part_patt_),
                    zip(part, filters)
                ))
            ]
            if filter_
        ]

        return filtered_partitions

    def drop_partitions_filter(
        self,
        *filters: str
    ) -> "TableHelper":
        """
        Drop partitions using the fnmatch filter passed.
        """
        conn = self.get_conn()

        filtered_partitions = self.list_partitions_filter(
            *filters
        )

        conn.drop_partitions(
            self.table_name,
            filtered_partitions
        )

        return self

    def sample(self, samples: int = 100) -> pd.DataFrame:
        """
        Select some sample from the table.
        """
        conn = self.get_conn()
        return conn.table(
            table_name=self.table_name,
            samples=samples
        )

    def metadata(self) -> Metadata:
        """ Returns the table metadata """
        conn = self.get_conn()
        return conn.metadata(table_name=self.table_name)
