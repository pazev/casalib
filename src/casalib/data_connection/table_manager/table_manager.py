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
    TableHelper,
    TableManagerAbstract,
    ExecutableTableManagerAbstract,
)
from ..base import ConnectionAbstract, Metadata, TableSchema


@dataclass
class TableManager(
    TableManagerAbstract,
    ExecutableTableManagerAbstract
):
    """ Query Manager """
    table_name: str
    query_template: str = field(repr=False)
    partition_cols: Optional[List[str]] = None

    def __post_init__(self):
        """ Post-init """
        self.helper = TableHelper(
            table_name=self.table_name
        )

    def set_conn_maker(
        self,
        conn_maker: Callable[[], ConnectionAbstract]
    ) -> "TableManager":
        """ Set the connection maker """
        self.helper.set_conn_maker(conn_maker)
        return self

    def drop(self) -> "TableManager":
        """ Drop the table """
        self.helper.drop()
        return self

    def drop_partitions(
        self,
        partitions_to_drop: List[Tuple[str, ...]]
    ) -> "TableManager":
        """ Drop partitions """
        self.helper.drop_partitions(partitions_to_drop)
        return self

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

    def drop_partitions_filter(
        self,
        *filters: str
    ) -> "TableManager":
        """
        Drop partitions using the fnmatch filter passed.
        """
        self.helper.drop_partitions_filter(*filters)
        return self

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

    # Class that implement the ExecutableTableManagerAbstract
    def make_query_(self, **params) -> str:
        """ Make the query that will be executed """
        env = jinja2.Environment(
            undefined=jinja2.StrictUndefined
        )
        template = env.from_string(self.query_template)
        query = template.render(**params)
        return query

    def create_insert(self, **params) -> "TableManager":
        """ Create/insert the query on table """
        conn = self.helper.get_conn()
        conn.create_insert(
            query=self.make_query_(**params),
            table_name=self.table_name,
            partition_cols=self.partition_cols,
        )
        return self

    def run(self, **params) -> "TableManager":
        """ Run the TableManager """
        return self.create_insert(**params)

    def get_table_input(
        self, **params
    ) -> List[TableSchema]:
        """ Return the list of table inputs """
        query = self.make_query_(**params)
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
