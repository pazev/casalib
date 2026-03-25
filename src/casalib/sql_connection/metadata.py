"""Metadata dataclasses for tables and queries."""
from dataclasses import dataclass
from typing import Dict, List, Optional


@dataclass
class TableInfo:
    """Metadata describing a physical table.

    Attributes:
        table_name: Fully qualified table name.
        schema: Schema (database/dataset) the table belongs to.
        table: Bare table name without the schema prefix.
        partition_cols: Mapping of partition column name to its type.
        custom_metadata: Arbitrary key/value pairs attached to the table.
    """

    table_name: str
    schema: str
    table: str
    partition_cols: Dict[str, str]
    custom_metadata: Dict[str, str]


@dataclass
class QueryInfo:
    """Metadata describing a query result.

    Attributes:
        input_tables: Tables referenced by the query.
        partition_cols: Mapping of partition column name to its type.
        custom_metadata: Arbitrary key/value pairs attached to the query.
    """

    input_tables: List[str]
    partition_cols: Dict[str, str]
    custom_metadata: Dict[str, str]


@dataclass
class Metadata:
    """Combined metadata for a query or table.

    Attributes:
        cols: Mapping of column name to its type.
        table: Table-specific metadata, if the source is a physical table.
        query_info: Query-specific metadata, if the source is a query.
    """

    cols: Dict[str, str]

    table: Optional[TableInfo] = None
    query_info: Optional[QueryInfo] = None
