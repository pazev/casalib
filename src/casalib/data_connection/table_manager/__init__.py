""" Module defines an object to manage tables """
from .other_owner_table_manager import (
    OtherOwnerTableManager
)
from .manageable_table_manager import ManageableTableManager
from .query_table_manager import QueryTableManager
from .pandas_table_manager import PandasTableManager

from .collection import TableManagerCollection
