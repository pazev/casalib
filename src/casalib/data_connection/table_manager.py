from dataclasses import dataclass
from fnmatch import fnmatch
from typing import Callable, Dict, List, Optional, Tuple

import jinja2
import pandas as pd

from .base import ConnectionAbstract, Metadata


@dataclass
class TableManager():
    """ Query Manager """
    query_template: str
    table_name: str
    conn_maker: Optional[Callable[[], ConnectionAbstract]] = None
    partition_cols: Optional[List[str]] = None

    def set_conn_maker(
        self,
        conn_maker: Callable[[], ConnectionAbstract]
    ) -> "TableManager":
        """ Set the connection maker """
        self.conn_maker = conn_maker
        return self

    def make_query_(self, **params) -> str:
        """ Make the query that will be executed """
        env = jinja2.Environment()
        template = env.from_string(self.query_template)
        query = template.render(**params)
        return query

    def drop(self) -> ConnectionAbstract:
        """ Drop the table """
        conn = self.conn_maker()
        return conn.drop(self.table_name)

    def create_insert(self, **params) -> ConnectionAbstract:
        """ Create/insert the query on table """
        conn = self.conn_maker()
        return conn.create_insert(
            query=self.make_query_(**params),
            table_name=self.table_name,
            partition_cols=self.partition_cols,
        )

    def drop_partitions(
        self,
        partitions_to_drop: List[Tuple[str]]
    ) -> ConnectionAbstract:
        """ Drop partitions """
        conn = self.conn_maker()
        return conn.drop_partitions(
            table_name=self.table_name,
            partitions_to_drop=partitions_to_drop
        )

    def list_partitions(self) -> Dict[Tuple[str], str]:
        """ List partitions """
        conn = self.conn_maker()
        return conn.list_partitions(self.table_name)

    def list_partitions_filter(self, *filters: List[str]):
        """
        Filter the partitions list using the filter passed.
        """
        conn = self.conn_maker()

        filtered_partitions = [
            part
            for part, path in conn.list_partitions(self.table_name).items()
            for filter_ in [all(map(lambda part_patt_: fnmatch(*part_patt_), zip(part, filters)))]
            if filter_
        ]

        return filtered_partitions

    def drop_partitions_filter(self, *filters: List[str]):
        """
        Filter the partitions list using the fnmatch filter passed.
        """
        conn = self.conn_maker()

        filtered_partitions = [
            part
            for part, path in conn.list_partitions(self.table_name).items()
            for filter_ in [all(map(lambda part_patt_: fnmatch(*part_patt_), zip(part, filters)))]
            if filter_
        ]

        return conn.drop_partitions(self.table_name, filtered_partitions)

    def sample(self, samples: int = 100) -> pd.DataFrame:
        """
        Select some sample from the table.
        """
        conn = self.conn_maker()
        return conn.table(table_name=self.table_name, samples=samples)

    def metadata(self) -> Metadata:
        """ Returns the table metadata """
        conn = self.conn_maker()
        return conn.metadata(table_name=self.table_name)
