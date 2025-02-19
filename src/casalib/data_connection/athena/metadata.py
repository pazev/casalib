import boto3

from .boto3_querying import run_query
from ..base import Metadata


def get_table_metadata(
    boto3_session: boto3.Session,
    data_catalog: str,
    default_schema_name: str,
    workgroup: str,
    table_name: str
):
    """ Captura o metadado de uma tabela Athena usando a boto3
    """
    athena = boto3_session.client('athena')

    schema, table_name = [
        default_schema_name,
        *table_name.split('.')
    ][-2:]

    # Capture metadata in Athena
    metadata_dict = athena.get_table_metadata(
        CatalogName=data_catalog,
        DatabaseName=schema,
        TableName=table_name
    )['TableMetadata']

    columns = {
        c['Name']: c['Type']
        for c in metadata_dict['Columns']
    }
    partition_keys = {
        c['Name']: c['Type']
        for c in metadata_dict['PartitionKeys']
    }

    # Cria o obieto Metadata
    metadata_obj = Metadata(
        connection_type='athena.AthenaConnection',
        columns=columns,
        partition_cols=partition_keys,
        location=metadata_dict['Parameters']['location'],
        table_name=f'{schema}.{table_name}',
        orig_info=metadata_dict,
    )

    return metadata_obj


def get_query_metadata(
    boto3_session: boto3.Session,
    data_catalog: str,
    default_schema_name: str,
    workgroup: str,
    query: str
):
    """ Captura o metadado da query """
    import json

    # Captura o JSON explain
    query_explain = f'explain (format json)\n{query}'

    query_obj = run_query(
        query=query_explain,
        schema_name=default_schema_name,
        data_catalog=data_catalog,
        workgroup=workgroup,
        boto3_session=boto3_session
    )

    result_set_list = query_obj.get_query_results(boto3_session)

    result_list = [
        line['Data'][0]['VarCharValue']

        for result_set in result_set_list
        for line in result_set['ResultSet']['Rows']
    ]

    result_json = json.loads("\n".join(result_list[1:]))

    # Extrai os dados
    columns = (
        result_json['0']['descriptor']['columnNames'][1:-1]
        .split(', ')
    )

    output_types = map(
        lambda x: x['type'],
        result_json['0']['outputs']
    )

    types = dict(zip(columns, output_types))

    # Cria objeto de Metadados
    metadata_obj = Metadata(
        connection_type='athena.AthenaConnection',
        columns=types,
        partition_cols=[],
        location=None,
        query=query,
    )

    return metadata_obj
