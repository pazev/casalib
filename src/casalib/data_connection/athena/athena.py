from dataclasses import dataclass
from typing import Optional, Union

import boto3
import pandas as pd

from ..base import ConnectionAbstract, Metadata

from .metadata import get_table_metadata, get_query_metadata
from .querying import run_query_get_pandas, run_table_get_pandas


@dataclass
class Boto3SessionMaker:
    aws_access_key_id: Optional[str] = None
    aws_secret_access_key: Optional[str] = None
    aws_session_token: Optional[str] = None
    region_name: Optional[str] = None

    def make(self) -> boto3.Session:
        """ Cria a sessão boto3 """
        par = {}

        if self.aws_access_key_id is not None:
            par['aws_access_key_id'] = self.aws_access_key_id

        if self.aws_secret_access_key is not None:
            par['aws_secret_access_key'] = (
                self.aws_secret_access_key
            )

        if self.aws_session_token is not None:
            par['aws_session_token'] = self.aws_session_token

        if self.region_name is not None:
            par['region_name'] = self.region_name

        return boto3.Session(**par)


@dataclass
class AthenaConnection(ConnectionAbstract):
    """ Classe para conexão no Athena. """
    schema_name: str
    workgroup: str
    s3_staging_dir: str
    data_catalog: str
    boto3_session_maker: Boto3SessionMaker
    table_prefix: str = ''

    def query(self, query: str) -> pd.DataFrame:
        """ Retorna o resultado da query como um DataFrame """
        return run_query_get_pandas(
            boto3_session=self.boto3_session_maker.make(),
            data_catalog=self.data_catalog,
            default_schema_name=self.schema_name,
            workgroup=self.workgroup,
            s3_output=self.s3_staging_dir,
            table_prefix=self.table_prefix,
            query=query
        )

    def table(
        self, table_name: str, samples: Union[int, None] = 100
    ) -> pd.DataFrame:
        """ Retorna uma amostra da tabela. O padrão são 100
            registros, mas este número pode ser alterado no
            parâmetro `samples`. Caso `samples` receba um número
            negativo ou None retorna a tabela inteira.
        """
        return run_table_get_pandas(
            boto3_session=self.boto3_session_maker.make(),
            data_catalog=self.data_catalog,
            default_schema_name=self.schema_name,
            workgroup=self.workgroup,
            s3_output=self.s3_staging_dir,
            table_prefix=self.table_prefix,
            table_name=table_name,
            samples=samples,
        )

    def metadata(
        self, query: Optional[str] = None,
        table_name: Optional[str] = None
    ) -> Metadata:
        """ Retorna o metadados da tabela ou query. Somente um
            dos dois deve ser setado.
        """
        # Controle de entrada
        if (query or table_name) is None:
            raise ValueError(
                "Ou `query` ou `tablename` precisa ser setado."
            )

        if (query is not None) and (table_name is not None):
            raise ValueError(
                "Ou `query` ou `tablename` precisa ser setado."
            )

        # Cria a sessão
        param = {
            'boto3_session': self.boto3_session_maker.make(),
            'data_catalog': self.data_catalog,
            'default_schema_name': self.schema_name,
            'workgroup': self.workgroup,
        }

        # Captura o metadado
        if table_name is not None:
            return get_table_metadata(
                table_name=table_name, **param
            )

        return get_query_metadata(
            query=query, **param
        )
