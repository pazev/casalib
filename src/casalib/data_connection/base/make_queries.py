"""
Module that generate queries for operations
"""
# pylint: disable=too-many-arguments

from abc import ABC, abstractmethod
from typing import Any, Dict, List, Optional, Type

from .base_connection import BaseConnectionAbstract
from .template import TemplateAbstract


class MakeQueryAbstract(ABC):
    """ Class to create queries when requested """
    @property
    @abstractmethod
    def get_connection_(self) -> BaseConnectionAbstract:
        """ Get the connection """

    @property
    @abstractmethod
    def get_template_(self) -> Type[TemplateAbstract]:
        """ Get the query template collection """

    @abstractmethod
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

    @abstractmethod
    def create_insert(
        self,
        query: str,
        table_name: str,
        partition_cols: Optional[List[str]] = None,
    ) -> List[str]:
        """ Cria uma tabela se não existir e insere dados.
            Realiza reordenação de colunas se necessário.
        """

    @abstractmethod
    def create_ctas(
        self,
        query: str,
        table_name: str,
        partition_cols: Optional[List[str]] = None,
    ) -> List[str]:
        """ Cria uma tabela com comando CREATE TABLE AS
        """

    @abstractmethod
    def last_partition(
        self,
        table_name: str,
        cross_columns_: List[str],
        max_column_: str,
        **filters: List[Any]
    ) -> List[str]:
        """ Create a query to retrieve last partition """
