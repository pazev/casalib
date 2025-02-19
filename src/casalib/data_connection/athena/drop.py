import re

import boto3

from .boto3_querying import run_query
from .metadata import get_table_metadata
from .s3_ops import list_files_prefix, delete_objects


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

    bucket, prefix = re.match(r's3:\/\/(.+?)\/(.*)\/?$',
                              metadata.location
                              ).groups()

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
    schema_name, table_name = metadata.table_name.split('.')

    drop_query = run_query(
        query=f'drop table if exists {schema_name}.{table_name}',
        schema_name=schema_name,
        data_catalog=data_catalog,
        workgroup=workgroup,
        boto3_session=boto3_session,
    )

    drop_query.get_query_results(boto3_session=boto3_session)

    return drop_query, files_list, metadata
