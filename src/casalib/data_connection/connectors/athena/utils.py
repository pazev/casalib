"""
Module implements a utility class, to do some special
operations not directly related to quering.
"""
from dataclasses import dataclass

from ...base import ConnectionUtilsAbstract
from .base import AthenaBaseConnection

from .modules.util import split_table_name


@dataclass
class AthenaConnectionUtils(ConnectionUtilsAbstract):
    """ Utilities class to Athena connection """
    conn: AthenaBaseConnection

    def schema_tablename(self, table_name: str) -> str:
        """ Generate the table_name with schema to be used
            in the connection
        """
        schema_name, table_name = split_table_name(
            table_name=table_name,
            default_schema_name=self.conn.schema_name
        )

        return f'{schema_name}.{table_name}'
