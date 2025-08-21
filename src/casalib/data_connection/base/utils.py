"""
Module implements some base utilitie for the connection
"""
from abc import ABC, abstractmethod


class ConnectionUtilsAbstract(ABC):
    """ Utils for abstract method """
    # pylint: disable=too-few-public-methods
    @abstractmethod
    def schema_tablename(self, table_name: str) -> str:
        """ Generate the table_name with schema to be used
            in the connection
        """
