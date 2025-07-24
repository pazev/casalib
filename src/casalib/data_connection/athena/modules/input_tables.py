""" Module to collect the input tables from a query """
from collections import namedtuple
import re
from typing import List

import boto3

from .boto3_querying import run_query


TableInput = namedtuple(
    'TableInput',
    ['schema_name', 'table_name']
)


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
    table_names = set()

    for res in results:
        for row in res['ResultSet']['Rows']:
            text = row['Data'][0].get('VarCharValue', '')
            match = re.search(
                r'table\s+=\s+([\w\d\.\:]+)',
                text
            )
            if match:
                table_names.add(match.group(1))

    table_names = [
        TableInput(schema, table_name)
        for tab in table_names
        for *_, schema, table_name in [tab.split(':')]
    ]

    return table_names
