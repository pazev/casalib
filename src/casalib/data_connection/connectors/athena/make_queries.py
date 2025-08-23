"""
Module defines an object that generate queries to perform
some operations
"""
from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Type

from .template import AthenaTemplates

from ...base import MakeQueryAbstract, Metadata
from .base_connection import AthenaBaseConnection

from .modules.create_insert import (
    make_create_schema_query_,
    make_create_ctas_query_,
    make_insert_query_
)

from .modules.util import split_table_name


@dataclass
class MakeQuery(MakeQueryAbstract):
    """ Class to create queries when requested """
    conn: AthenaBaseConnection

    @property
    def get_connection_(self) -> AthenaBaseConnection:
        """ Get the connection """
        return self.conn

    @property
    def get_template_(self) -> Type[AthenaTemplates]:
        """ Get the query template collection """
        return AthenaTemplates

    def create_insert(
        self,
        query: str,
        table_name: str,
        partition_cols: Optional[List[str]] = None,
    ) -> List[str]:
        """ Cria uma tabela se não existir e insere dados.
            Realiza reordenação de colunas se necessário.
        """
        output_queries = []

        partition_cols = partition_cols or []

        schema_name, table_name = split_table_name(
            table_name=table_name,
            default_schema_name=self.conn.schema_name
        )

        query_meta = self.conn.metadata(query=query)

        not_found_part_cols = (
            set(partition_cols) - set(query_meta.columns)
        )

        if not_found_part_cols:
            raise ValueError(
                f'The columns {not_found_part_cols} were '
                'not found in query.'
            )

        # Get table metadata
        # pylint: disable=broad-exception-caught

        try:
            metadata = self.conn.metadata(
                table_name=table_name
            )
        except Exception as exc:
            if 'EntityNotFound' not in exc.args[0]:
                raise exc

            columns_types = {
                col: type_
                for col, type_ in query_meta.columns.items()
                if col not in partition_cols
            }

            partition_cols_types = {
                col: type_
                for col in partition_cols
                for type_ in [query_meta.columns[col]]
            }

            metadata = Metadata(
                connection_type='athena.AthenaConnection',
                columns=columns_types,
                partition_cols=partition_cols_types,
                location=None,
                table_name=None,
                query=None,
                orig_info=None
            )

            create_query = make_create_schema_query_(
                schema_name=schema_name,
                table_name=table_name,
                columns_types=metadata.columns,
                partition_columns_types=(
                    metadata.partition_cols
                ),
                s3_output=self.conn.s3_staging_dir,
            )

            output_queries.append(create_query)

        # Check if all columns requested by the table
        # are in the query
        not_found_table_cols = (
            (
                set(metadata.columns)
                |
                set(metadata.partition_cols)
            )
            -
            (
                set(query_meta.columns)
                |
                set(query_meta.partition_cols)
            )
        )

        if not_found_table_cols:
            raise ValueError(
                f'The columns {not_found_table_cols} not '
                'found in query.'
            )

        # Generate the insert query
        insert_query = make_insert_query_(
            schema_name=schema_name,
            table_name=table_name,
            columns_types=(
                metadata.columns | metadata.partition_cols
            ),
            query=query,
        )

        output_queries.append(insert_query)

        return output_queries

    def create_ctas(
        self,
        query: str,
        table_name: str,
        partition_cols: Optional[List[str]] = None,
    ) -> List[str]:
        """ Cria uma tabela com comando CREATE TABLE AS
        """
        # pylint: disable=too-many-arguments

        output_queries = []

        partition_cols = partition_cols or []

        query_meta = self.conn.metadata(query=query)

        schema_name, table_name = split_table_name(
            table_name=table_name,
            default_schema_name=self.conn.schema_name
        )

        query_ctas = make_create_ctas_query_(
            schema_name=schema_name,
            table_name=table_name,
            query=query,
            columns_types={
                col: type_
                for col, type_ in query_meta.columns.items()
                if col not in partition_cols
            },
            partition_columns_types={
                col: query_meta.columns[col]
                for col in partition_cols
            },
            s3_output=self.conn.s3_staging_dir,
        )

        output_queries.append(query_ctas)

        return output_queries

    def agg_query(
        self,
        query: str,
        groupby: Optional[List[str]] = None,
        count_: Optional[List[str]] = None,
        count_distinct_: Optional[List[str]] = None,
        sum_: Optional[List[str]] = None,
        mean_: Optional[List[str]] = None,
        min_: Optional[List[str]] = None,
        max_: Optional[List[str]] = None,
        percentile_: Optional[Dict[int, List[str]]] = None,
    ) -> List[str]:
        """ Realiza uma agregação na query indicada """
        # pylint: disable=too-many-arguments

        return [
            self.get_template_.agg_query(
                query=query,
                groupby=groupby,
                count_=count_,
                count_distinct_=count_distinct_,
                sum_=sum_,
                mean_=mean_,
                min_=min_,
                max_=max_,
                percentile_=percentile_,
            )
        ]

    def last_partition(
        self,
        table_name: str,
        cross_columns_: List[str],
        max_column_: str,
        **filters: List[Any]
    ) -> List[str]:
        """ Create a query to retrieve last partition """
        queries = [
            self.get_template_.last_partition(
                table_name=table_name,
                cross_columns_=cross_columns_,
                max_column_=max_column_,
                **filters
            )
        ]
        return queries
