"""
Módulo implementa a funcionalidade de enviar um dataframe
pandas para o banco de dados.
"""
from typing import List, Union

import awswrangler as wr
import boto3
import pandas as pd

from .metadata import get_table_metadata, Metadata


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
    """ Envia um pandas DataFrame para a localização
        indicada
    """
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
    except Exception as exc:
        pass

    partition_cols = partition_cols or partition_cols_tab or []

    if partition_cols_tab and (partition_cols != partition_cols_tab):
        raise ValueError(
            "partition_cols is invalid; table already "
            "exists. Please leave the partition_cols "
            "argument empty."
        )

    # Envia o arquivo
    schema_name, table_name = [
        default_schema_name,
        *table_name.split('.')
    ][-2:]

    s3_output = (
        location or f'{s3_output}/{schema_name}.{table_name}'
    )

    params = {
        'df': dff,
        'path': s3_output,
        'dataset': True,
        'database': schema_name,
        'table': table_name,
        'partition_cols': partition_cols,
        'boto3_session': boto3_session,
    }

    wr.s3.to_parquet(**params)

    # Captura o metadado
    res = get_table_metadata(
        boto3_session=boto3_session,
        data_catalog=data_catalog,
        default_schema_name=schema_name,
        workgroup=workgroup,
        table_name=table_name,
    )

    return res
