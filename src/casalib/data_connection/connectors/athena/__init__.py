"""
Module implements the data_connection for Amazon Athena.
"""
from .base_connection import Boto3SessionMaker
from .connection import AthenaConnection


__all__ = [
    'Boto3SessionMaker',
    'AthenaConnection',
]
