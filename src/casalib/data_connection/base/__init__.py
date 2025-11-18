"""
Base abstractions for the data connection functionalities
"""
from ._metadata import Metadata
from .base_connection import BaseConnectionAbstract
from .connection import ConnectionAbstract
from .make_queries import MakeQueryAbstract
from .querier import Querier
from .utils import ConnectionUtilsAbstract
