"""
Module implements the functionality of sending a pandas
dataframe to the database.
"""
# pylint: disable=too-many-arguments
from typing import List, Union

import awswrangler as wr
import boto3
import pandas as pd

from .metadata import get_table_metadata
from ....base import Metadata


def create_table_pandas_dataframe(
    boto3_session: boto3.Session,
    data_catalog: str,
    workgroup: str,
    default_schema_name: str,
    table_name: str,
    s3_output: str,
    dff: pd.DataFrame,
    partition_cols: Union[List[str], None],
) -> Metadata:
    """ Sends a pandas DataFrame to the indicated
        location
    """
    # pylint: disable=broad-exception-caught

    location = None
    partition_cols_tab = None

    try:
        metadata = get_table_metadata(
            boto3_session=boto3_session,
            data_catalog=data_catalog,
            default_schema_name=default_schema_name,
            workgroup=workgroup,
            table_name=table_name,
        )

        location = metadata.location
        partition_cols_tab = list(metadata.partition_cols)
    except Exception as exception:
        error_flag = True
        if 'EntityNotFound' in exception.args[0]:
            error_flag = False

        if 'MetadataException' in exception.args[0]:
            error_flag = False

        if error_flag:
            raise exception

    partition_cols = (
        partition_cols or partition_cols_tab or []
    )

    if (
        partition_cols_tab and
        (partition_cols != partition_cols_tab)
    ):
        raise ValueError(
            "partition_cols is invalid; table already "
            "exists. Please leave the partition_cols "
            "argument empty."
        )

    # Sends the file
    schema_name, table_name = [
        default_schema_name,
        *table_name.split('.')
    ][-2:]

    s3_output = (
        location or
        f'{s3_output}/{schema_name}.{table_name}'
    )

    wr.s3.to_parquet(
        df=dff,
        path=s3_output,
        dataset=True,
        database=schema_name,
        table=table_name,
        partition_cols=partition_cols,
        boto3_session=boto3_session,
    )

    # Captures the metadata
    res = get_table_metadata(
        boto3_session=boto3_session,
        data_catalog=data_catalog,
        default_schema_name=schema_name,
        workgroup=workgroup,
        table_name=table_name,
    )

    return res
