"""
Query templating module
"""
from typing import Any, Dict, List, Optional

from ....base.template import TemplateAbstract
from ._load_templates import templates_dict


class AthenaTemplates(TemplateAbstract):
    """ Class that generate SQL queries for Athena """
    @staticmethod
    def split_schema_name(
        table_name: str,
        schema_name: Optional[str] = None,
    ) -> str:
        """ Function to normalize the table name """
        schema, table = [
            schema_name,
            *table_name.split('.')
        ][-2:]
        return '.'.join(filter(None, [schema, table]))

    @staticmethod
    def agg_query(
        query: str,
        groupby: Optional[List[str]] = None,
        count_: Optional[List[str]] = None,
        count_distinct_: Optional[List[str]] = None,
        sum_: Optional[List[str]] = None,
        mean_: Optional[List[str]] = None,
        min_: Optional[List[str]] = None,
        max_: Optional[List[str]] = None,
        percentile_: Optional[Dict[int, List[str]]] = None,
    ) -> str:
        """ Make aggregation over a query """
        # pylint: disable=too-many-arguments
        return templates_dict['agg'].render(
            query=query,
            groupby=groupby,
            count_=count_,
            count_distinct_=count_distinct_,
            sum_=sum_,

        )


    @staticmethod
    def create_schema(
        table_name: str,
        columns_types: Dict[str, str],
        partition_cols_types: Dict[str, str],
        schema_name: Optional[str] = None,
        location: str = None,
    ) -> str:
        """ Generate a CREATE TABLE with schema query """


    @staticmethod
    def create_ctas(
        query: str,
        table_name: str,
        partition_cols: Optional[List[str]] = None,
        schema_name: Optional[str] = None,
        location: Optional[str] = None,
    ) -> str:
        """ Generate a CREATE TABLE AS (CTAS) query
        """

    @staticmethod
    def insert_table(
        query: str,
        table_name: str,
        schema_name: Optional[str] = None,
        location: Optional[str] = None,
    ) -> str:
        """ Generate a INSERT INTO query
        """

    @staticmethod
    def last_partition(
        table_name: str,
        cross_columns_: List[str],
        max_column_: str,
        **filters: List[Any]
    ) -> List[str]:
        """ Generate a query to retrieve the last partition
            of a table.
        """
