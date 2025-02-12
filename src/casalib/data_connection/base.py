from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from typing import Any, Dict, Optional, Union

import pandas as pd


@dataclass
class Metadata:
    connection_type: str
    columns: Dict[str, str]
    partition_cols: Dict[str, str]
    location: Union[str, None]
    table_name: Optional[str] = field(default=None, repr=False)
    query: Optional[str] = field(default=None, repr=False)
    orig_info: Optional[Any] = field(default=None, repr=False)


class ConnectionAbstract(ABC):
    """ Classe abstrata de conexão, contendo os métodos mínimos
        para funcionar.
    """
    @abstractmethod
    def query(self, query: str) -> pd.DataFrame:
        """ Retorna o resultado da query como um DataFrame """

    @abstractmethod
    def table(self, tablename: str) -> pd.DataFrame:
        """ Retorna todos os registros da tabela """

    @abstractmethod
    def sample(
        self, tablename: str, samples: int = 100
    ) -> pd.DataFrame:
        """ Retorna uma amostra da tabela com o tamanho
            especificado
        """

    @abstractmethod
    def metadata(
        self, query: str, tablename: str
    ) -> Dict[str, Dict]:
        """ Retorna o metadados da tabela ou query. Somente
            um dos dois deve ser setado.
        """
