""" Utils to work with Athena tables """
# pylint: disable=too-many-arguments
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


def treat_column_type(
    column_type: str
) -> str:
    """ Adjust the column type """
    if 'array' in column_type:
        column_type = re.sub(
            r'array\((.*)\)',
            r'array<\1>',
            column_type
        )

    if 'real' in column_type:
        return column_type.replace('real', 'double')

    if 'varchar' in column_type:
        return re.sub(
            r'varchar(\(\d+\))?',
            r'string',
            column_type
        )

    if 'timestamp' in column_type:
        return 'timestamp'

    return column_type
