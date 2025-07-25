"""
Módulo contém a implementação do drop de tabelas no Athena.

No Athena, o simples drop ainda mantém os dados presentes no
S3. Assim, precisamos utilizar do boto3 para listar os
arquivos e apagar os arquivos físicos presentes no S3.
"""
# pylint: disable=too-many-arguments
import re

import boto3

from .boto3_querying import run_query
from .metadata import get_table_metadata
from .s3_ops import (
    list_files_prefix,
    delete_objects,
    get_bucket_prefix
)


def drop_table(
    boto3_session: boto3.Session,
    data_catalog: str,
    default_schema_name: str,
    workgroup: str,
    table_name: str,
    ignore_if_not_exist: bool = True
) -> None:
    """ Drop a table in Athena """
    # Get table metadata
    try:
        metadata = get_table_metadata(
            boto3_session=boto3_session,
            data_catalog=data_catalog,
            default_schema_name=default_schema_name,
            workgroup=workgroup,
            table_name=table_name,
        )
    except Exception as exc:
        if 'EntityNotFound' not in exc.args[0]:
            raise exc

        if ignore_if_not_exist:
            return

        raise exc

    bucket, prefix = get_bucket_prefix(
        str(metadata.location)
    )

    # List files in bucket / prefix
    files_list = list_files_prefix(
        boto3_session=boto3_session,
        bucket=bucket,
        prefix=prefix,
    )

    delete_objects(
        boto3_session=boto3_session,
        files_list=files_list
    )

    # Drop athena table from catalog
    *_, schema_name, table_name = (
        str(metadata.table_name).split('.')
    )

    drop_query = run_query(
        query=(
            'drop table if exists '
            f'{schema_name}.{table_name}'
        ),
        schema_name=schema_name,
        data_catalog=data_catalog,
        workgroup=workgroup,
        boto3_session=boto3_session,
    )

    drop_query.get_query_results(
        boto3_session=boto3_session
    )
