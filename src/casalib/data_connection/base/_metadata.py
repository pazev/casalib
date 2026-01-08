"""
Define the Metadata object, that capture information about
a table
"""
from dataclasses import dataclass, field
from typing import Any, Dict, Union, Optional


@dataclass
class Metadata:
    """
    Object to extract data from a table in the database
    """
    connection_type: str
    columns: Dict[str, str]
    partition_cols: Dict[str, str]
    location: Union[str, None]
    table_name: Optional[str] = (
        field(default=None, repr=False)
    )
    query: Optional[str] = field(default=None, repr=False)
    orig_info: Optional[Any] = (
        field(default=None, repr=False)
    )
