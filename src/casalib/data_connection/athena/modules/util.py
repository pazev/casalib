""" Utils to work with Athena tables """
import re
from typing import Tuple


def split_table_name(
    table_name: str,
    default_schema_name: str
) -> Tuple[str, str]:
    """ Split table name into schema and table names. """
    table_name_adj = re.sub('[`]', '', table_name)
    default_schema_name_adj = re.sub(
        r'[`]',
        '',
        default_schema_name
    )

    schema_name, table_name = [
        default_schema_name_adj,
        *table_name_adj.split('.')
    ][-2:]

    return schema_name, table_name
