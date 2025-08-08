"""
Functions to create the Public and Source using a Connection
"""
from typing import List, Optional

from casalib.data_connection.base import (
    ConnectionAbstract,
)
from .base import Source, Public


def make_conn_public(
    query: str,
    event_ymd_column: str,
    conn: ConnectionAbstract
) -> Public:
    """ Create an Public from Athena Connection """
    metadata = conn.metadata(query=query)
    public = Public(
        event_ymd_column=event_ymd_column,
        columns=list(
            metadata.columns | metadata.partition_cols
        ),
        metadata={'query': query}
    )
    return public


def make_conn_source(
    table_name: str,
    keys: List[str],
    info_ymd_column: str,
    ingestion_column: str,
    conn: ConnectionAbstract,
    remove_columns: Optional[List[str]] = None,
    prefix: str = '',
) -> Source:
    """ Create a Source from Athena Connection """
    # pylint: disable=too-many-arguments

    metadata = conn.metadata(table_name=table_name)

    columns_dict = (
        metadata.columns | metadata.partition_cols
    )

    columns = [
        col
        for col in columns_dict
        if col not in (remove_columns or [])
    ]

    source = Source(
        keys=keys,
        info_ymd_column=info_ymd_column,
        ingestion_column=ingestion_column,
        columns=columns,
        metadata={'table_name': table_name},
        prefix=prefix
    )
    return source
