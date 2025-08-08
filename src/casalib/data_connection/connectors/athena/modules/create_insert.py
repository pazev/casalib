"""
Módulo implementa funções de criação e inserção de dados em
tabelas no AWS Athena.
"""
# pylint: disable=too-many-arguments
from typing import Dict, List, Union

import boto3

from ....base import Metadata
from .boto3_querying import run_query
from .metadata import get_table_metadata, get_query_metadata
from ..templates import templates_dict
from .util import split_table_name, treat_column_type


def make_create_schema_query_(
    schema_name: str,
    table_name: str,
    columns_types: Dict[str, str],
    partition_columns_types: Dict[str, str],
    s3_output: str,
):
    """ Make the query to create a table passing the
        schema
    """
    columns_types = {
        col: treat_column_type(type_)
        for col, type_ in columns_types.items()
    }
    partition_columns_types = {
        col: treat_column_type(type_)
        for col, type_ in partition_columns_types.items()
    }

    template = templates_dict['create_table']

    query = template.render(
        schema_name=schema_name,
        table_name=table_name,
        columns_types=columns_types,
        partition_columns_types=partition_columns_types,
        s3_output=f'{s3_output}/{schema_name}.{table_name}'
    )

    return query


def create_schema(
    boto3_session: boto3.Session,
    workgroup: str,
    data_catalog: str,
    default_schema_name: str,
    table_name: str,
    columns_types: Dict[str, str],
    partition_columns_types: Dict[str, str],
    s3_output: str,
) -> Metadata:
    """ Cria tabela com o schema passado """
    schema_name, table_name = split_table_name(
        table_name=table_name,
        default_schema_name=default_schema_name,
    )

    query_create = make_create_schema_query_(
        schema_name=schema_name,
        table_name=table_name,
        columns_types=columns_types,
        partition_columns_types=partition_columns_types,
        s3_output=s3_output,
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


def make_create_ctas_query_(
    schema_name: str,
    table_name: str,
    query: str,
    columns_types: Dict[str, str],
    partition_columns_types: Union[Dict[str, str], None],
    s3_output: str
):
    """ Make the query to create a table passing the
        schema
    """
    template = templates_dict['create_table_ctas']

    query = template.render(
        schema_name=schema_name,
        table_name=table_name,
        query=query,
        columns_types=columns_types,
        partition_columns_types=partition_columns_types,
        s3_output=f'{s3_output}/{schema_name}.{table_name}'
    )

    return query


def create_ctas(
    boto3_session: boto3.Session,
    workgroup: str,
    data_catalog: str,
    default_schema_name: str,
    table_name: str,
    query: str,
    partition_cols: Union[List[str], None],
    s3_output: str
) -> Metadata:
    """ Cria tabela com o método CREATE TABLE AS """
    partition_cols = partition_cols or []

    schema_name, table_name = split_table_name(
        table_name=table_name,
        default_schema_name=default_schema_name
    )

    # Captura metadata da query
    query_meta = get_query_metadata(
        boto3_session=boto3_session,
        data_catalog=data_catalog,
        default_schema_name=default_schema_name,
        workgroup=workgroup,
        query=query,
    )

    columns_types = {
        col: type_
        for col, type_ in query_meta.columns.items()
        if col not in partition_cols
    }

    partition_columns_types = {
        col: query_meta.columns[col]
        for col in partition_cols
    }

    query_create = make_create_ctas_query_(
        schema_name=schema_name,
        table_name=table_name,
        query=query,
        columns_types=columns_types,
        partition_columns_types=partition_columns_types,
        s3_output=s3_output,
    )

    query_exec = run_query(
        query=query_create,
        schema_name=schema_name,
        data_catalog=data_catalog,
        workgroup=workgroup,
        boto3_session=boto3_session,
    )

    query_exec.wait(boto3_session)

    return get_table_metadata(
        boto3_session=boto3_session,
        data_catalog=data_catalog,
        default_schema_name=default_schema_name,
        workgroup=workgroup,
        table_name=table_name,
    )


def make_insert_query_(
    schema_name: str,
    table_name: str,
    columns_types: Dict[str, str],
    query: str,
):
    """ Make the INSERT INTO query """
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
    workgroup: str,
    data_catalog: str,
    default_schema_name: str,
    table_name: str,
    columns_types: Dict[str, str],
    partition_columns_types: Dict[str, str],
    query: str,
    s3_output: str,
) -> Metadata:
    """ Run the create insert into query """
    # pylint: disable=unused-argument
    schema_name, table_name = split_table_name(
        table_name=table_name,
        default_schema_name=default_schema_name
    )

    query_insert = make_insert_query_(
        schema_name=schema_name,
        table_name=table_name,
        columns_types=(
            columns_types | partition_columns_types
        ),
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
    workgroup: str,
    data_catalog: str,
    default_schema_name: str,
    table_name: str,
    query: str,
    partition_cols: Union[List[str], None],
    s3_output: str,
):
    """ Cria uma tabela se não existir, e insere dados na
        mesma.
    """
    # pylint: disable=broad-exception-caught

    partition_cols = partition_cols or []

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
            default_schema_name=default_schema_name,
            table_name=table_name,
            columns_types={
                col: type_
                for col, type_ in query_meta.columns.items()
                if col not in partition_cols
            },
            partition_columns_types={
                col: type_
                for col in partition_cols
                for type_ in [query_meta.columns[col]]
            },
            s3_output=s3_output,
        )

    # Checa se todas as colunas solicitadas pela tabela
    # estão na query
    not_found_table_cols = (
        (
            set(metadata.columns)
            |
            set(metadata.partition_cols)
        )
        -
        (
            set(query_meta.columns)
            |
            set(query_meta.partition_cols)
        )
    )

    if not_found_table_cols:
        raise ValueError(
            f'As colunas {not_found_table_cols} não foram '
            'encontradas na query.'
        )

    # Insere os dados
    insert(
        boto3_session=boto3_session,
        workgroup=workgroup,
        data_catalog=data_catalog,
        default_schema_name=default_schema_name,
        table_name=table_name,
        columns_types=metadata.columns,
        partition_columns_types=metadata.partition_cols,
        s3_output=s3_output,
        query=query,
    )

    return metadata
