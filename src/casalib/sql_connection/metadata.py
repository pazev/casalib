''' Metadata object '''
from dataclasses import dataclass, field
from typing import Dict, List, Optional


@dataclass
class TableInfo:
    table_name: str
    schema: str
    table: str
    partition_cols: Dict[str, str]
    custom_metadata: Dict[str, str]


@dataclass
class QueryInfo:
    input_tables: List[str]
    partition_cols: Dict[str, str]
    custom_metadata: Dict[str, str]


@dataclass
class Metadata:
    cols: Dict[str, str]

    table: Optional[TableInfo] = None
    query_info: Optional[QueryInfo] = None
