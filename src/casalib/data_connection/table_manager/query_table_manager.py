""" Module defines an object to manage tables and its
    creation
"""
from dataclasses import dataclass, field
from typing import (
    Any, Callable, Dict, List, Optional, Tuple
)

import jinja2
from jinja2 import meta
import pandas as pd

from .base import (
    TableManagerAbstract,
)
from .helpers import TableHelper
from ..base import ConnectionAbstract, Metadata


@dataclass
class QueryTableManager(TableManagerAbstract):
    """ Query Manager """
    table_name: str
    query_template: str = field(repr=False)
    partition_cols: Optional[List[str]] = None

    def __post_init__(self):
        """ Post-init """
        self.helper = TableHelper(
            table_name=self.table_name,
        )

    def set_conn_maker(
        self,
        conn_maker: Callable[[], ConnectionAbstract]
    ) -> "QueryTableManager":
        """ Set the connection maker """
        self.helper.set_conn_maker(conn_maker)
        return self

    def get_conn(self) -> ConnectionAbstract:
        """ Get a connection """
        return self.helper.get_conn()

    def get_table_name(self) -> str:
        """ Return the table_name """
        return self.helper.get_table_name()

    def get_partition_cols(self) -> Optional[List[str]]:
        """ Returns the configured parittion cols """
        return self.partition_cols

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
        **filters: List[Any]
    ) -> List[str]:
        """ Return last partition query for the table """
        return self.helper.last_partition_query(
            cross_columns_=cross_columns_,
            max_column_=max_column_,
            **filters
        )

    def drop(self) -> "QueryTableManager":
        """ Drop the table """
        self.helper.drop()
        return self

    def drop_partitions(
        self,
        partitions_to_drop: List[Tuple[str, ...]]
    ) -> "QueryTableManager":
        """ Drop partitions """
        self.helper.drop_partitions(partitions_to_drop)
        return self

    def drop_partitions_filter(
        self,
        *filters: str
    ) -> "QueryTableManager":
        """
        Drop partitions using the fnmatch filter passed.
        """
        self.helper.drop_partitions_filter(*filters)
        return self

    def get_table_input(
        self, **params
    ) -> List[str]:
        """ Return the list of table inputs """
        query = self.make_query(**params)
        input_tables = (
            self.helper.get_conn().get_input_tables(query)
        )
        return input_tables

    def input_vars(self) -> List[str]:
        """ List the variables in the template """
        env = jinja2.Environment()
        parsed_content = env.parse(self.query_template)
        return list(
            meta.find_undeclared_variables(parsed_content)
        )

    # Methods required for the run
    def make_query(self, **params) -> str:
        """ Make the query that will be executed """
        env = jinja2.Environment(
            undefined=jinja2.StrictUndefined
        )
        template = env.from_string(self.query_template)
        query = template.render(**params)
        return query

    def create_insert_(self, **params) -> "QueryTableManager":
        """ Create/insert the query on table """
        conn = self.helper.get_conn()
        conn.create_insert(
            query=self.make_query(**params),
            table_name=self.table_name,
            partition_cols=self.partition_cols,
        )
        return self

    def run(self, **params) -> "QueryTableManager":
        """ Run the TableManager """
        return self.create_insert_(**params)
