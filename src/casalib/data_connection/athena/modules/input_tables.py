import re
from typing import List

import boto3

from .boto3_querying import run_query


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

    results = query_execution_id.get_query_results()

    import pdb; pdb.set_trace()

    # Extract all table names
    table_names = set()

    for row in results['ResultSet']['Rows']:
        text = row['Data'][0].get('VarCharValue', '')
        match = re.search(r'Table:\s*([\w\.]+)', text)
        if match:
            table_names.add(match.group(1))

    return list(table_names)
