"""
This module contains the final Connection class, which
encapsulates the implementation of data_connection for AWS
Athena.

The Boto3SessionMaker class manages the creation of
connections to AWS Athena whenever needed.

The AthenaConnection class combines the functionalities of
AthenaBaseConnection, MakeQueries, Utils and Templates to
provide some advanced features, like advanced partition
filtering and automatically query generation and more.
"""
# pylint: disable=too-many-instance-attributes
from dataclasses import dataclass
from typing import Type

from .make_queries import MakeQuery
from .template import AthenaTemplates
from .utils import AthenaConnectionUtils
from .base_connection import Boto3SessionMaker, AthenaBaseConnection
from ...base import (
    BaseConnectionAbstract,
    ConnectionAbstract,
    Querier
)


@dataclass
class AthenaConnection(ConnectionAbstract):
    """ Classe para conexão no Athena. """
    schema_name: str
    workgroup: str
    s3_staging_dir: str
    data_catalog: str
    boto3_session_maker: Boto3SessionMaker
    table_prefix: str = ''

    def __post_init__(self) -> None:
        """ Create basic modules to use """
        self.conn_ = AthenaBaseConnection(
            schema_name=self.schema_name,
            workgroup=self.workgroup,
            s3_staging_dir=self.s3_staging_dir,
            data_catalog=self.data_catalog,
            boto3_session_maker=self.boto3_session_maker,
            table_prefix=self.table_prefix,
        )

        self.make_query_ = MakeQuery(self.conn_)
        self.utils_ = AthenaConnectionUtils(self.conn_)
        self.query_ = Querier(self.conn_, self.make_query_)

    @staticmethod
    def template() -> Type[AthenaTemplates]:
        """ Return the class with methods to generate
            queries without base connection
        """
        return AthenaTemplates

    @property
    def queries(self) -> MakeQuery:
        """ Returns an object capable of generating the
            desired query
        """
        return self.make_query_

    @property
    def query(self) -> Querier:
        """
        Returns an object to run the query
        """
        return self.query_

    @property
    def utils(self) -> AthenaConnectionUtils:
        """ Reeturns an object to implement some
            utilities
        """
        return self.utils_

    @property
    def get_connection_(self) -> BaseConnectionAbstract:
        """ Return the connection abstract necessary to
            perform the tasks
        """
        return self.conn_
