"""
Módulo contém a classe final, que encapsula a implementação
do data_connection para o AWS Athena junto com a
funcionalidade de make_queries.

A class Boto3SessionMaker gerencia a criação da conexão com
o AWS Athena sempre que necessário.

Já a classe AthenaConnection junta as funcionalidades de
AthenaBaseConnection com MakeQueries. A amarração se dá
via design pattern template, na classe abstrata.
"""
# pylint: disable=too-many-instance-attributes
from dataclasses import dataclass, field

from .make_query import MakeQuery
from .utils import AthenaConnectionUtils
from .base import Boto3SessionMaker, AthenaBaseConnection
from ...base import (
    BaseConnectionAbstract,
    ConnectionAbstract,
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
    conn_: AthenaBaseConnection = field(
        init=False,
        repr=False
    )
    make_query_: MakeQuery = field(
        init=False,
        repr=False
    )

    def __post_init__(self):
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

    @property
    def queries(self) -> MakeQuery:
        """ Returns an object capable of generating the
            desired query
        """
        return self.make_query_

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
