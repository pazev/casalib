"""
Module defines the Connection, that will be used.
"""
from abc import abstractmethod
from fnmatch import fnmatch
from typing import Dict, List, Optional, Tuple, Union

import pandas as pd

from .base_connection import BaseConnectionAbstract
from .make_queries import MakeQueryAbstract
from .metadata import Metadata
from .utils import ConnectionUtilsAbstract


class ConnectionAbstract(BaseConnectionAbstract):
    """ Add the MakeQuery property to the class """
    @property
    @abstractmethod
    def queries(self) -> MakeQueryAbstract:
        """ Returns an object capable of generating the
            desired query
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

    def query(self, query: str) -> pd.DataFrame:
        """ Retorna o resultado da query como um
            DataFrame
        """
        return self.get_connection_.query(query=query,)

    def table(
        self,
        table_name: str,
        samples: Union[int, None] = 100
    ) -> pd.DataFrame:
        """ Retorna uma amostra da tabela. O padrão são 100
            registros, mas este número pode ser alterado no
            parâmetro `samples`. Caso `samples` receba um
            número negativo ou None retorna a tabela
            inteira.
        """
        return self.get_connection_.table(
            table_name=table_name, samples=samples,
        )

    def metadata(
        self,
        query: Optional[str] = None,
        table_name: Optional[str] = None,
    ) -> Metadata:
        """ Retorna o metadados da tabela ou query. Somente
            um dos dois deve ser setado.
        """
        return self.get_connection_.metadata(
            query=query, table_name=table_name,
        )

    def drop(self, table_name: str) -> "ConnectionAbstract":
        """ Dropa uma tabela """
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
        """ Cria uma tabela se não existir e insere dados.
            Realiza reordenação de colunas se necessário.
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
        """ Cria uma tabela com comando CREATE TABLE AS
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
        """ Lista as partições """
        return self.get_connection_.list_partitions(
            table_name=table_name,
        )

    def drop_partitions(
        self,
        table_name: str,
        partitions_to_drop: List[Tuple[str, ...]],
    ) -> "ConnectionAbstract":
        """ Dropa as partições indicadas na tabela """
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
        """ Envia um pandas DataFrame para o banco """
        return self.get_connection_.send_pandas(
            dff=dff, table_name=table_name,
            partition_cols=partition_cols,
        )

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
    ) -> pd.DataFrame:
        """ Realiza uma agregação na query indicada """
        # pylint: disable=too-many-arguments
        return self.get_connection_.agg_query(
            query=query, groupby=groupby,
            count_=count_, count_distinct_=count_distinct_,
            sum_=sum_, mean_=mean_, min_=min_,
            max_=max_, percentile_=percentile_,
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

    def list_partition_filter(
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

    def list_partition_filter_pd(
        self, table_name: str, *filters: str,
    ) -> pd.DataFrame:
        """ Return the list of partitions as pd.DataFrame
        """
        metadata = self.metadata(table_name=table_name)

        list_partitions = self.list_partition_filter(
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
        filtered_partitions = self.list_partition_filter(
            table_name, *filters
        )
        self.get_connection_.drop_partitions(
            table_name, filtered_partitions
        )
        return self
