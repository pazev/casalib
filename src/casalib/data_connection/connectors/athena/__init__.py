"""
Módulo faz a implementação do data_connection para o
Amazon Athena.
"""
from .base_connection import Boto3SessionMaker
from .connection import AthenaConnection


__all__ = [
    'Boto3SessionMaker',
    'AthenaConnection',
]
