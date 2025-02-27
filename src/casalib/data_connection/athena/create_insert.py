from pathlib import Path
from typing import Dict, List, Union

import awswrangler as wr
import boto3

from ..base import Metadata
from .boto3_querying import run_query
from .metadata import get_table_metadata, get_query_metadata
from .templates import templates_dict


def make_create_schema_query_(
    schema_name: str,
    table_name: str,
    columns_types: Dict[str, str],
    partition_columns_types: Dict[str, str],
    s3_output: str,
):
    template = templates_dict['create_table']

    query = template.render(
        schema_name=schema_name,
        table_name=table_name,
        columns_types=columns_types,
        partition_columns_types=partition_columns_types,
        s3_output=s3_output
    )

    return query


def create_schema(
    boto3_session: boto3.Session,
    workgroup: str,
    data_catalog: str,
    schema_name: str,
    table_name: str,
    columns_types: Dict[str, str],
    partition_columns_types: Dict[str, str],
    s3_output: str,
) -> Metadata:
    """ Cria tabela com o schema passado """
    query_create = make_create_schema_query_(
        schema_name=schema_name,
        table_name=table_name,
        columns_types=columns_types,
        partition_columns_types=partition_columns_types,
        s3_output=f'{s3_output}/{table_name}'
    )

    query_exec = run_query(
        query=query_create,
        schema_name=schema_name,
        data_catalog=data_catalog,
        workgroup=workgroup,
        boto3_session=boto3_session
    )

    query_exec.wait(boto3_session)

    return get_table_metadata(
        boto3_session=boto3_session,
        data_catalog=data_catalog,
        default_schema_name=schema_name,
        workgroup=workgroup,
        table_name=table_name,
    )


def make_insert_query_(
    schema_name: str,
    table_name: str,
    columns_types: Dict[str, str],
    query: str,
):
    template = templates_dict['insert_table']

    query_insert = template.render(
        schema_name=schema_name,
        table_name=table_name,
        columns_types=columns_types,
        query=query,
    )

    return query_insert


def insert(
    boto3_session: boto3.Session,
    data_catalog: str,
    default_schema_name: str,
    workgroup: str,
    s3_output: str,
    schema_name: str,
    table_name: str,
    columns_types: Union[List[str], None],
    partition_columns_types: Union[List[str], None],
    query: str,
) -> Metadata:
    query_insert = make_insert_query_(
        schema_name=schema_name,
        table_name=table_name,
        columns_types=columns_types | partition_columns_types,
        query=query
    )

    query_exec = run_query(
        query=query_insert,
        schema_name=schema_name,
        data_catalog=data_catalog,
        workgroup=workgroup,
        boto3_session=boto3_session
    )

    query_exec.wait(boto3_session)

    return get_table_metadata(
        boto3_session=boto3_session,
        data_catalog=data_catalog,
        default_schema_name=schema_name,
        workgroup=workgroup,
        table_name=table_name,
    )


def create_insert(
    boto3_session: boto3.Session,
    data_catalog: str,
    default_schema_name: str,
    workgroup: str,
    s3_output: str,
    table_name: str,
    partition_cols: Union[List[str], None],
    query: str,
):
    """ Cria uma tabela se não existir, e insere dados na mesma.
    """
    partition_cols = partition_cols or []

    # Trata nome da tabela
    schema_name, table_name = [
        default_schema_name,
        *table_name.split('.')
    ][-2:]

    # Captura metadata da query
    query_meta = get_query_metadata(
        boto3_session=boto3_session,
        data_catalog=data_catalog,
        default_schema_name=default_schema_name,
        workgroup=workgroup,
        query=query,
    )

    # Check inicial - colunas de partição estão na query?
    not_found_part_cols = (
        set(partition_cols) - set(query_meta.columns)
    )

    if not_found_part_cols:
        raise ValueError(
            f'As colunas {not_found_part_cols} não foram '
            'encontradas na query.'
        )

    # Captura metadado da tabela, cria se necessário
    metadata = None

    try:
        metadata = get_table_metadata(
            boto3_session=boto3_session,
            data_catalog=data_catalog,
            default_schema_name=default_schema_name,
            workgroup=workgroup,
            table_name=table_name
        )
    except Exception as exception:
        if 'EntityNotFound' not in exception.args[0]:
            raise exception

        metadata = create_schema(
            boto3_session=boto3_session,
            workgroup=workgroup,
            data_catalog=data_catalog,
            schema_name=schema_name,
            table_name=table_name,
            columns_types={
                col: type_adj
                for col, type_ in query_meta.columns.items()
                for type_adj in [
                    'string' if 'varchar' in type_ else
                    'double' if type_ == 'real' else
                    type_
                ]
                if col not in partition_cols
            },
            partition_columns_types={
                col: type_adj
                for col in partition_cols
                for type_ in [query_meta.columns[col]]
                for type_adj in [
                    'string' if 'varchar' in type_ else
                    'double' if type_ == 'real' else
                    type_
                ]
            },
            s3_output=s3_output,
        )

    # Checa se todas as colunas solicitadas pela tabela estão na
    # query
    not_found_table_cols = (
        (set(metadata.columns) | set(metadata.partition_cols)) -
        (set(query_meta.columns) | set(query_meta.partition_cols))
    )

    if not_found_table_cols:
        raise ValueError(
            f'As colunas {not_found_part_cols} não foram '
            'encontradas na query.'
        )

    # Insere os dados
    res = insert(
        boto3_session=boto3_session,
        data_catalog=data_catalog,
        default_schema_name=default_schema_name,
        workgroup=workgroup,
        s3_output=s3_output,
        schema_name=schema_name,
        table_name=table_name,
        columns_types=metadata.columns,
        partition_columns_types=metadata.partition_cols,
        query=query,
    )

    return metadata


def create(
    boto3_session: boto3.Session,
    data_catalog: str,
    default_schema_name: str,
    workgroup: str,
    s3_output: str,
    table_name: str,
    partition_cols: Union[List[str], None],
    query: str,
):
    partition_cols = partition_cols or []

    # Trata nome da tabela
    schema_name, table_name = [
        default_schema_name,
        *table_name.split('.')
    ][-2:]

    s3_output = f'{s3_output}/{table_name}'

    params = {
        'sql': query,
        'database': schema_name,
        'ctas_table': table_name,
        'ctas_database': schema_name,
        'workgroup': workgroup,
        'boto3_session': boto3_session,
        's3_output': s3_output,
        'partitioning_info': partition_cols,
        'wait': True,
    }

    res = wr.athena.create_ctas_table(**params)

    return res
