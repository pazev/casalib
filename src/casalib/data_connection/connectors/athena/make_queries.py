"""
Module defines an object that generate queries to perform
some operations
"""
from dataclasses import dataclass
from typing import (
    Any, Dict, List, Optional, Tuple, Type, Union
)

from .template import AthenaTemplates

from ...base import MakeQueryAbstract, Metadata
from .base_connection import AthenaBaseConnection


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
        # pylint: disable=too-many-locals
        output_queries = []

        partition_cols = partition_cols or []

        schema_name, table_name = (
            AthenaTemplates.split_schema_name(
                table_name=table_name,
                schema_name=self.conn.schema_name
            )
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

            location = f'{self.conn.s3_staging_dir}/{schema_name}.{table_name}'

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

            # create_query = make_create_schema_query_(
            create_query = AthenaTemplates.create_schema(
                table_name=table_name,
                columns_types=metadata.columns,
                partition_cols_types=metadata.partition_cols,
                schema_name=schema_name,
                location=location,
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
        insert_query = AthenaTemplates.insert_table(
            query=query,
            table_name=table_name,
            schema_name=schema_name,
            cols_ordering=list(
                metadata.columns | metadata.partition_cols
            ),
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

        schema_name, table_name = (
            AthenaTemplates.split_schema_name(
                table_name=table_name,
                schema_name=self.conn.schema_name
            )
        )

        location = (
            f'{self.conn.s3_staging_dir}/{schema_name}.{table_name}'
        )

        query_ctas = AthenaTemplates.create_ctas(
            query=query,
            table_name=table_name,
            partition_cols=list(partition_cols),
            schema_name=schema_name,
            location=location,
            cols_ordering=list(
                query_meta.columns | query_meta.partition_cols
            ),
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
        percentile_ignore_values_: Optional[Dict[str, List[float]]] = None,
        cols_before: Optional[List[Union[str, Tuple[str, str]]]] = None,
        cols_after: Optional[List[Union[str, Tuple[str, str]]]] = None,
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
                percentile_ignore_values_=percentile_ignore_values_,
                cols_before=cols_before,
                cols_after=cols_after
            )
        ]

    def get_duplicates(
        self,
        query: str,
        keys: List[str],
        samples: Optional[int] = None
    ) -> List[str]:
        """ Query to find duplicates in the given query """
        return [
            self.get_template_.get_duplicates(
                query=query, keys=keys, samples=samples
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

    def left_join(
        self,
        root_query: str,
        other_queries: List[str],
        join_cols: List[str],
        cols_to_add_suffix: Optional[List[str]] = None,
        cols_after: Optional[List[Tuple[str, str]]] = None,
        select_cols: Optional[List[str]] = None,
        samples: Optional[int] = None,
    ) -> List[str]:
        """
        Generate a LEFT JOIN query, to join several queries
        to a root one.
        """
        # pylint: disable=too-many-arguments
        cols_to_add_suffix = cols_to_add_suffix or []
        cols_after = cols_after or []

        conn = self.get_connection_

        # Capture the columns
        queries_list = [root_query, *other_queries]
        cols_list = [
            [
                (col, col)
                for col in conn.metadata(query).columns
            ]
            for query in queries_list
        ]

        root_query_cols, *other_queries_cols = (
            list(zip(queries_list, cols_list))
        )

        query = self.get_template_.left_join(
            root_query_cols=root_query_cols,
            other_queries_cols=other_queries_cols,
            join_cols=join_cols,
            cols_to_add_suffix=cols_to_add_suffix,
            cols_after=cols_after,
            select_cols=select_cols,
            samples=samples,
        )

        return [query]

    def op(
        self,
        query: str,
        rename: Optional[List[Tuple[str, str]]] = None,
        add_cols: Optional[List[Tuple[str, str]]] = None,
        select: Optional[List[str]] = None,
        exclude: Optional[List[str]] = None,
    ) -> List[str]:
        """
        Generate a new query, that rename, select, exclude
        and add new columns.
        """
        # pylint: disable=too-many-arguments
        query_cols = list(
            self.get_connection_.metadata(query=query).columns
        )

        query = self.get_template_.op(
            query=query,
            query_cols=query_cols,
            rename=rename,
            add_cols=add_cols,
            select=select,
            exclude=exclude,
        )

        return [query]
