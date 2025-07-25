""" Module defines an object to manage tables """
from dataclasses import dataclass
from fnmatch import fnmatch
from typing import Callable, Dict, List, Optional, Tuple

import jinja2
from jinja2 import meta
import pandas as pd

from ..base import ConnectionAbstract, Metadata


@dataclass
class TableManager():
    """ Query Manager """
    query_template: str
    table_name: str
    conn_maker: Optional[
        Callable[[], ConnectionAbstract]
    ] = None
    partition_cols: Optional[List[str]] = None

    def set_conn_maker(
        self,
        conn_maker: Callable[[], ConnectionAbstract]
    ) -> "TableManager":
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

    def make_query_(self, **params) -> str:
        """ Make the query that will be executed """
        env = jinja2.Environment(
            undefined=jinja2.StrictUndefined
        )
        template = env.from_string(self.query_template)
        query = template.render(**params)
        return query

    def drop(self) -> "TableManager":
        """ Drop the table """
        conn = self.get_conn()
        conn.drop(self.table_name)

        return self

    def create_insert(self, **params) -> "TableManager":
        """ Create/insert the query on table """
        conn = self.get_conn()
        conn.create_insert(
            query=self.make_query_(**params),
            table_name=self.table_name,
            partition_cols=self.partition_cols,
        )
        return self

    def drop_partitions(
        self,
        partitions_to_drop: List[Tuple[str, ...]]
    ) -> "TableManager":
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
    ) -> "TableManager":
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

    def input_vars(self) -> List[str]:
        """ List the variables in the template """
        env = jinja2.Environment()
        parsed_content = env.parse(self.query_template)
        return list(
            meta.find_undeclared_variables(parsed_content)
        )
