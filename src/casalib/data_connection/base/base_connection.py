"""
Module defines the base connection class, that will be used
to perform the tasks with the database.

All advanced features will be implemented on
ConnectionAbstract.
"""
# pylint: disable=too-many-arguments
from abc import ABC, abstractmethod
from typing import Dict, List, Optional, Tuple, Union

import pandas as pd

from ._metadata import Metadata


class BaseConnectionAbstract(ABC):
    """ Classe abstrata de conexão, contendo os métodos
        mínimos para funcionar.
    """
    @abstractmethod
    def query_method_(self, query: str) -> pd.DataFrame:
        """ Retorna o resultado da query como um
            DataFrame
        """

    @abstractmethod
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

    @abstractmethod
    def metadata(
        self,
        query: Optional[str] = None,
        table_name: Optional[str] = None,
    ) -> Metadata:
        """ Retorna o metadados da tabela ou query. Somente
            um dos dois deve ser setado.
        """

    @abstractmethod
    def drop(self, table_name: str) -> "BaseConnectionAbstract":
        """ Dropa uma tabela """

    @abstractmethod
    def create_insert(
        self,
        query: str,
        table_name: str,
        partition_cols: Optional[List[str]] = None,
    ) -> "BaseConnectionAbstract":
        """ Cria uma tabela se não existir e insere dados.
            Realiza reordenação de colunas se necessário.
        """

    @abstractmethod
    def create_ctas(
        self,
        query: str,
        table_name: str,
        partition_cols: Optional[List[str]] = None,
    ) -> "BaseConnectionAbstract":
        """ Cria uma tabela com comando CREATE TABLE AS
        """

    @abstractmethod
    def list_partitions(
        self,
        table_name: str,
    ) -> Dict[Tuple[str, ...], str]:
        """ Lista as partições """

    @abstractmethod
    def drop_partitions(
        self,
        table_name: str,
        partitions_to_drop: List[Tuple[str, ...]],
    ) -> "BaseConnectionAbstract":
        """ Dropa as partições indicadas na tabela """

    @abstractmethod
    def send_pandas(
        self,
        dff: pd.DataFrame,
        table_name: str,
        partition_cols: Optional[List[str]] = None,
    ) -> Metadata:
        """ Envia um pandas DataFrame para o banco """

    @abstractmethod
    def get_input_tables(
        self,
        query: str
    ) -> List[str]:
        """ Get the required tables for the given query """
