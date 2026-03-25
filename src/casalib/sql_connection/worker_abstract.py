"""Worker abstract class."""
from abc import ABC, abstractmethod
from typing import List, Optional, Tuple

import pandas as pd

from .metadata import Metadata


class WorkerAbstract(ABC):
    """Abstract base class for database workers.

    Concrete implementations wrap a specific database client and handle
    all I/O operations: running queries, creating tables, and managing
    partitions.
    """

    @abstractmethod
    def run_query(self, query: str) -> pd.DataFrame:
        """Run a SQL query against the database.

        If the query produces rows, return them as a DataFrame. If the
        query produces no rows (e.g. CREATE, DROP, INSERT), return an
        empty DataFrame. Raise on error.

        Args:
            query: SQL query string to execute.

        Returns:
            A DataFrame with the query results, or an empty DataFrame
            for non-SELECT statements.
        """

    @abstractmethod
    def create_insert(
        self,
        query: str,
        table_name: str,
        partition_cols: Optional[List[str]] = None,
    ) -> str:
        """Create a table from a query or insert into an existing one.

        If the table does not exist, create it with the schema inferred
        from the query and insert the data. If the table already exists,
        reorder the query columns to match and insert. ``partition_cols``
        is ignored when the table already exists.

        Args:
            query: SELECT query whose result set is written to the table.
            table_name: Destination table name.
            partition_cols: Columns to use as partition keys on creation.

        Returns:
            The resolved table name after the operation.
        """

    @abstractmethod
    def create_ctas(
        self,
        query: str,
        table_name: str,
        partition_cols: Optional[List[str]] = None,
    ) -> str:
        """Create a table using CREATE TABLE AS SELECT.

        Raises:
            Exception: If the table already exists.

        Args:
            query: SELECT query to use as the table definition.
            table_name: Name for the new table.
            partition_cols: Columns to use as partition keys.

        Returns:
            The resolved table name after creation.
        """

    @abstractmethod
    def get_query_metadata(self, query: str) -> Metadata:
        """Return metadata for the result set of a query.

        Args:
            query: SQL query string to inspect.

        Returns:
            Metadata describing the columns and types of the result set.
        """

    @abstractmethod
    def get_table_metadata(self, table_name: str) -> Metadata:
        """Return metadata for a physical table.

        Args:
            table_name: Fully qualified table name.

        Returns:
            Metadata describing the table's columns, types, and
            partition information.
        """

    @abstractmethod
    def drop(self, table_name: str) -> None:
        """Drop a table.

        Args:
            table_name: Fully qualified table name.

        Raises:
            Exception: If the table does not exist.
        """

    @abstractmethod
    def list_partitions(
        self,
        table_name: str,
        *filters: str,
    ) -> List[Tuple[str, ...]]:
        """List partitions for a table, optionally filtered.

        Each returned tuple contains one value per partition column.
        When ``filters`` are provided, each filter is an fnmatch pattern
        applied to the corresponding partition column in order.

        Args:
            table_name: Fully qualified table name.
            *filters: Optional fnmatch patterns, one per partition column.

        Returns:
            A list of tuples representing the matching partitions.

        Raises:
            Exception: If the table does not exist or is not partitioned.
        """

    @abstractmethod
    def drop_partitions(
        self,
        table_name: str,
        *filters: str,
    ) -> None:
        """Drop partitions from a table matching the given filters.

        Each filter is an fnmatch pattern applied to the corresponding
        partition column in order.

        Args:
            table_name: Fully qualified table name.
            *filters: fnmatch patterns, one per partition column.

        Raises:
            Exception: If the table does not exist or is not partitioned.
        """
