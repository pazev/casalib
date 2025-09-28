""" Module defines an object to manage tables and its
    creation
"""
from dataclasses import dataclass
from typing import List

import jinja2
from jinja2 import meta

from .base import TableManagerAbstract


@dataclass(kw_only=True)
class QueryTableManager(TableManagerAbstract):
    """ Query Manager """
    query_template: str

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

    def discover_params_(self, **params) -> List[Dict[str, Any]]:
        """ Discover parameters for execution """
        return [params]

    def run(self, **params) -> "QueryTableManager":
        """ Run the TableManager """
        params_list = self.discover_params_(**params)

        for params in params_list:
            last = self.create_insert_(**params)

        return last
