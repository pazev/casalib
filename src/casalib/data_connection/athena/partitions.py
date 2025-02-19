from typing import List, Tuple

import awswrangler as wr
import boto3

from .s3_ops import (
    list_files_prefix,
    delete_objects,
    get_bucket_prefix
)


def list_partitions(
    boto3_session: boto3.Session,
    default_schema_name: str,
    table_name: str,
):
    """ Lista partições da tabela """
    schema_name, table_name = [
        default_schema_name,
        *table_name.split('.')
    ]

    partitions = wr.catalog.get_partitions(
        database=schema_name,
        table=table_name,
        boto3_session=boto3_session,
    )

    partitions_final = {
        tuple(part): path
        for path, part in partitions.items()
    }

    return partitions_final


def drop_partitions(
    boto3_session: boto3.Session,
    default_schema_name: str,
    table_name: str,
    partitions_to_drop: List[Tuple[str]]
):
    """ Dropa as partições indicadas na tabela """
    # Quebra nome da tabela
    schema_name, table_name = [
        default_schema_name,
        *table_name.split('.')
    ]

    # Lista as partições
    partitions = list_partitions(
        boto3_session=boto3_session,
        default_schema_name=schema_name,
        table_name=table_name
    )

    # Apaga os arquivos
    for part_spec in partitions_to_drop:
        path = partitions[tuple(part_spec)]
        bucket, prefix = get_bucket_prefix(path)

        files_to_delete = list_files_prefix(
            boto3_session=boto3_session,
            bucket=bucket,
            prefix=prefix
        )

        delete_objects(boto3_session, files_to_delete)

    # Apaga do metadados
    wr.catalog.delete_partitions(
        table=table_name,
        database=schema_name,
        partitions_values=partitions_to_drop,
        boto3_session=boto3_session,
    )
