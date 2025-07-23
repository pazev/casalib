"""
Módulo contendo as classes bases para operação de bancos de
dados.
"""

from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional, Tuple, Union

import pandas as pd


@dataclass
class Metadata:
    """ Objeto para extrair dados de uma tabela no banco """
    connection_type: str
    columns: Dict[str, str]
    partition_cols: Dict[str, str]
    location: Union[str, None]
    table_name: Optional[str] = field(default=None, repr=False)
    query: Optional[str] = field(default=None, repr=False)
    orig_info: Optional[Any] = field(default=None, repr=False)


class ConnectionAbstract(ABC):
    """ Classe abstrata de conexão, contendo os métodos
        mínimos para funcionar.
    """
    @abstractmethod
    def query(self, query: str) -> pd.DataFrame:
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
        self, query: str, table_name: str
    ) -> Dict[str, Dict]:
        """ Retorna o metadados da tabela ou query. Somente
            um dos dois deve ser setado.
        """

    @abstractmethod
    def drop(self, table_name: str) -> "ConnectionAbstract":
        """ Dropa uma tabela """

    @abstractmethod
    def create_insert(
        self,
        query: str,
        table_name: str,
        partition_cols: Optional[List[str]] = None,
    ) -> "ConnectionAbstract":
        """ Cria uma tabela se não existir e insere dados.
            Realiza reordenação de colunas se necessário.
        """

    @abstractmethod
    def create_ctas(
        self,
        query: str,
        table_name: str,
        partition_cols: Optional[List[str]] = None,
    ) -> "ConnectionAbstract":
        """ Cria uma tabela com comando CREATE TABLE AS
        """

    @abstractmethod
    def list_partitions(
        self,
        table_name: str,
    ) -> Dict[Tuple[str], str]:
        """ Lista as partições """

    @abstractmethod
    def drop_partitions(
        self,
        table_name: str,
        partitions_to_drop: List[Tuple[str]],
    ) -> "ConnectionAbstract":
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
    def agg_query(
        self,
        query: str,
        groupby: Optional[List[str]] = None,
        count: Optional[List[str]] = None,
        count_distinct: Optional[List[str]] = None,
        sum: Optional[List[str]] = None,
        mean: Optional[List[str]] = None,
        min: Optional[List[str]] = None,
        max: Optional[List[str]] = None,
        percentile: Dict[int, List[str]] = None,
    ) -> pd.DataFrame:
        """ Realiza uma agregação na query indicada """

    @abstractmethod
    def get_input_tables(self, query: str) -> List[str]:
        """ Get the required tables for the given query """
