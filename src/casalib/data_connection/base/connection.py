"""
This module contains the final ConnectionAbstract class.

The Connection class combines the functionalities of
BaseConnectionAbstract, MakeQueries, Utils and Templates to
provide some advanced features, like advanced partition
filtering and automatically query generation and more.
"""
from abc import abstractmethod
from fnmatch import fnmatch
from typing import Dict, List, Optional, Tuple, Type, Union

import pandas as pd

from ._metadata import Metadata
from .base_connection import BaseConnectionAbstract
from .make_queries import MakeQueryAbstract
from .querier import Querier
from .template import TemplateAbstract
from .utils import ConnectionUtilsAbstract


class ConnectionAbstract(BaseConnectionAbstract):
    """ Add the MakeQuery property to the class """
    # Properties
    # ==========
    @staticmethod
    @abstractmethod
    def template() -> Type[TemplateAbstract]:
        """
        Return the class with methods to generate queries.
        Given it is a staticmethod, we don't have connection
        info while generating the query.
        """

    @property
    @abstractmethod
    def queries(self) -> MakeQueryAbstract:
        """
        Returns an object capable of generating the desired
        query. Use connection data into the generation
        procedure.
        """

    @property
    @abstractmethod
    def query(self) -> Querier:
        """
        Returns an object to run the query
        """

    @property
    @abstractmethod
    def utils(self) -> ConnectionUtilsAbstract:
        """ Connection utils """

    @property
    @abstractmethod
    def get_connection_(self) -> BaseConnectionAbstract:
        """ Return the connection abstract necessary to
            perform the tasks
        """

    def query_method_(self, query: str) -> pd.DataFrame:
        """ Return the results of a query as a dataframe """
        return self.get_connection_.query_method_(query)

    def table(
        self,
        table_name: str,
        samples: Union[int, None] = 100
    ) -> pd.DataFrame:
        """ Returns a sample of the table. The default is 100
            records, but this number can be changed in the
            `samples` parameter. If `samples` receives a
            negative number or None, returns the entire
            table.
        """
        return self.get_connection_.table(
            table_name=table_name, samples=samples,
        )

    def metadata(
        self,
        query: Optional[str] = None,
        table_name: Optional[str] = None,
    ) -> Metadata:
        """ Returns the metadata of the table or query. Only
            one of the two should be set.
        """
        return self.get_connection_.metadata(
            query=query, table_name=table_name,
        )

    def drop(self, table_name: str) -> "ConnectionAbstract":
        """ Drops a table """
        self.get_connection_.drop(
            table_name=table_name,
        )
        return self

    def create_insert(
        self,
        query: str,
        table_name: str,
        partition_cols: Optional[List[str]] = None,
    ) -> "ConnectionAbstract":
        """ Creates a table if it doesn't exist and inserts data.
            Performs column reordering if necessary.
        """
        self.get_connection_.create_insert(
            query=query, table_name=table_name,
            partition_cols=partition_cols,
        )
        return self

    def create_ctas(
        self,
        query: str,
        table_name: str,
        partition_cols: Optional[List[str]] = None,
    ) -> "ConnectionAbstract":
        """ Creates a table with CREATE TABLE AS command
        """
        self.get_connection_.create_ctas(
            query=query, table_name=table_name,
            partition_cols=partition_cols,
        )
        return self

    def list_partitions(
        self,
        table_name: str,
    ) -> Dict[Tuple[str, ...], str]:
        """ Lists the partitions """
        return self.get_connection_.list_partitions(
            table_name=table_name,
        )

    def drop_partitions(
        self,
        table_name: str,
        partitions_to_drop: List[Tuple[str, ...]],
    ) -> "ConnectionAbstract":
        """ Drops the indicated partitions in the table """
        self.get_connection_.drop_partitions(
            table_name=table_name,
            partitions_to_drop=partitions_to_drop,
        )
        return self

    def send_pandas(
        self,
        dff: pd.DataFrame,
        table_name: str,
        partition_cols: Optional[List[str]] = None,
    ) -> Metadata:
        """ Sends a pandas DataFrame to the database """
        return self.get_connection_.send_pandas(
            dff=dff, table_name=table_name,
            partition_cols=partition_cols,
        )

    def get_input_tables(
        self,
        query: str
    ) -> List[str]:
        """ Get the required tables for the given query """
        return (
            self
            .get_connection_
            .get_input_tables(query)
        )

    # Upgraded version of partition list
    def list_partitions_filter(
        self, table_name: str, *filters: str,
    ) -> List[Tuple[str, ...]]:
        """
        List the partitions, allowing filtering using
        fnmatch
        """
        filtered_partitions = [
            part
            for part, _ in (
                self
                .get_connection_
                .list_partitions(table_name)
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

    def list_partitions_filter_pd(
        self, table_name: str, *filters: str,
    ) -> pd.DataFrame:
        """ Return the list of partitions as pd.DataFrame
        """
        metadata = self.metadata(table_name=table_name)

        list_partitions = self.list_partitions_filter(
            table_name, *filters
        )

        return pd.DataFrame(
            list_partitions,
            columns=list(metadata.partition_cols)
        ).sort_values(list(metadata.partition_cols))

    def drop_partitions_filter(
        self, table_name: str, *filters: str,
    ) -> "ConnectionAbstract":
        """
        Drop partitions using the fnmatch filter passed.
        """
        filtered_partitions = self.list_partitions_filter(
            table_name, *filters
        )
        self.get_connection_.drop_partitions(
            table_name, filtered_partitions
        )
        return self

    def drop_partitions_filter_pd(
        self, table_name: str, *filters: str,
    ) -> "ConnectionAbstract":
        """
        Drop partitions using the fnmatch filter passed.
        Sugar syntax to dtop_partitions_filter.
        """
        return self.drop_partitions_filter(
            table_name,
            *filters
        )

    def tsel(self, table_name: str) -> str:
        """ Query to create a simple SELECT * FROM table """
        return f'select * from {table_name}'
