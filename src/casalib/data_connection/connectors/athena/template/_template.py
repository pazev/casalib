"""
Query templating module
"""
from typing import Any, Dict, List, Optional, Tuple, Union

from ....base.template import TemplateAbstract

from .agg import make_sql_agg_query_, make_get_duplicates_
from .create_ctas import make_sql_create_ctas_
from .create_schema import make_sql_create_schema_
from .insert_table import make_sql_insert_
from .last_partition import make_sql_last_partition_


class AthenaTemplates(TemplateAbstract):
    """ Class that generate SQL queries for Athena """
    @staticmethod
    def split_schema_name(
        table_name: str,
        schema_name: Optional[str] = None,
        raise_error_: bool = True,
    ) -> Tuple[str, str]:
        """ Function to normalize the table name """
        schema, table = [
            schema_name,
            *table_name.split('.')
        ][-2:]

        if schema is None:
            if raise_error_:
                raise ValueError(
                    "Please, add schema_name to "
                    "`table_name` or set `schema_name`."
                )

        return (str(schema), str(table))

    @staticmethod
    def process_table_name(
        table_name: str,
        schema_name: Optional[str] = None,
    ) -> str:
        """ Process table_name, adding the default schema if
            needed.
        """
        schema, table = AthenaTemplates.split_schema_name(
            table_name, schema_name
        )
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
        cols_before: Optional[List[Union[str, Tuple[str, str]]]] = None,
        cols_after: Optional[List[Union[str, Tuple[str, str]]]] = None,
    ) -> str:
        """ Make aggregation over a query """
        # pylint: disable=too-many-arguments
        return make_sql_agg_query_(
            query=query,
            groupby=groupby,
            count_=count_,
            count_distinct_=count_distinct_,
            sum_=sum_,
            mean_=mean_,
            min_=min_,
            max_=max_,
            percentile_=percentile_,
            cols_before=cols_before,
            cols_after=cols_after,
        )

    @staticmethod
    def get_duplicates(
        query: str,
        keys: List[str],
        samples: Optional[int] = None
    ) -> str:
        """ Find duplicates in a query """
        return make_get_duplicates_(
            query=query,
            keys=keys,
            samples=samples,
        )

    @staticmethod
    def last_partition(
        table_name: str,
        cross_columns_: List[str],
        max_column_: str,
        **filters: List[Any]
    ) -> str:
        """ Generate a query to retrieve the last partition
            of a table.
        """
        return make_sql_last_partition_(
            table_name=table_name,
            cross_columns_=cross_columns_,
            max_column_=max_column_,
            **filters
        )

    @staticmethod
    def create_schema(
        table_name: str,
        columns_types: Dict[str, str],
        partition_cols_types: Optional[Dict[str, str]] = None,
        schema_name: Optional[str] = None,
        location: Optional[str] = None,
    ) -> str:
        """ Generate a CREATE TABLE with schema query """
        schema_name_, table_name = (
            AthenaTemplates
            .split_schema_name(table_name, schema_name)
        )

        partition_cols_types_ = partition_cols_types or {}

        return make_sql_create_schema_(
            table_name=table_name,
            columns_types=columns_types,
            partition_cols_types=partition_cols_types_,
            schema_name=schema_name_,
            location=location,
        )

    @staticmethod
    def create_ctas(
        query: str,
        table_name: str,
        partition_cols: Optional[List[str]] = None,
        schema_name: Optional[str] = None,
        location: Optional[str] = None,
        cols_ordering: Optional[List[str]] = None,
    ) -> str:
        """ Generate a CREATE TABLE AS (CTAS) query
        """
        # pylint: disable=too-many-arguments
        schema_name_, table_name = (
            AthenaTemplates
            .split_schema_name(table_name, schema_name)
        )

        partition_cols_ = partition_cols or []
        cols_ordering_ = cols_ordering or []

        return make_sql_create_ctas_(
            query=query,
            table_name=table_name,
            partition_cols=partition_cols_,
            schema_name=schema_name_,
            location=location,
            cols_ordering=cols_ordering_,
        )

    @staticmethod
    def insert_table(
        query: str,
        table_name: str,
        schema_name: Optional[str] = None,
        cols_ordering: Optional[List[str]] = None,
    ) -> str:
        """ Generate a INSERT INTO query
        """
        schema_name, table_name = (
            AthenaTemplates
            .split_schema_name(table_name, schema_name)
        )

        return make_sql_insert_(
            query=query,
            table_name=table_name,
            schema_name=schema_name,
            cols_ordering=cols_ordering,
        )
