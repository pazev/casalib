""" Module to collect the input tables from a query """
# pylint: disable=too-many-arguments
from itertools import chain
import re
from typing import List, Optional

import boto3

from .boto3_querying import run_query


def process_line(
    line: str, default_schema_name: str
) -> Optional[List[str]]:
    """ Process a line, retrieving the table """
    line = (
        line
        .replace('$iceberg-aws', '')
        .replace('.', ':')
    )

    match = re.search(
        r'table\s+=\s+([\w\d\.\:]+)',
        line
    )

    if not match:
        return None

    *_, schema_name, table_name = match.group(1).split(':')

    return [
        ".".join([
            default_schema_name,
            schema_name,
            table_name
        ][-2:])
    ]


def get_input_tables(
    query: str,
    boto3_session: boto3.Session,
    data_catalog: str,
    default_schema_name: str,
    workgroup: str,
) -> List[str]:
    """ Get the Input tables for a query """
    query_execution_id = run_query(
        query=f'explain {query}',
        schema_name=default_schema_name,
        data_catalog=data_catalog,
        workgroup=workgroup,
        boto3_session=boto3_session
    )

    results = query_execution_id.get_query_results(
        boto3_session
    )

    # Extract all table names
    tables = [
        list_tables

        for res in results
        for row in res['ResultSet']['Rows']
        for text in [row['Data'][0].get('VarCharValue', '')]
        for list_tables in [
            process_line(text, default_schema_name)
        ]
        if list_tables
    ]

    tables_flatten = chain.from_iterable(tables)

    return list(tables_flatten)
