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
    def table(
        self, table_name: str, samples: Union[int, None] = 100
    ) -> pd.DataFrame:
        """ Retorna uma amostra da tabela. O padrão são 100
            registros, mas este número pode ser alterado no
            parâmetro `samples`. Caso `samples` receba um número
            negativo ou None retorna a tabela inteira.
        """

    @abstractmethod
    def metadata(
        self, query: str, tablename: str
    ) -> Dict[str, Dict]:
        """ Retorna o metadados da tabela ou query. Somente
            um dos dois deve ser setado.
        """
