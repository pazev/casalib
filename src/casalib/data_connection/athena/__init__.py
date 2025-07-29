"""
Módulo faz a implementação do data_connection para o
Amazon Athena.
"""
from .athena import AthenaConnection, Boto3SessionMaker
from .make_query import MakeQuery