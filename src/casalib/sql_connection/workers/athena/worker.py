"""AwsAthenaWorker — WorkerAbstract for Athena.

Delegates all operations to _operations.
"""
from dataclasses import dataclass, field
from typing import Any, List, Optional, Tuple

import boto3
import pandas as pd

from ...metadata import Metadata
from ...worker_abstract import WorkerAbstract
from . import _operations as ops


@dataclass
class AwsAthenaWorker(WorkerAbstract):
    """WorkerAbstract implementation for Athena.

    All logic lives in _operations; this class
    holds configuration and wires arguments.

    Attributes:
        database: Default Glue/Athena database.
        s3_output: S3 URI for query results
            and new table data.
        region: AWS region name.
        workgroup: Athena workgroup name.
        poll_interval: Seconds between status
            polls in run_query.
    """

    database: str
    s3_output: str
    region: str
    workgroup: str = "primary"
    poll_interval: float = 0.5
    _client: Any = field(
        default=None, init=False, repr=False
    )

    @property
    def client(self) -> Any:
        """Lazy boto3 Athena client."""
        if self._client is None:
            self._client = boto3.client(
                "athena",
                region_name=self.region,
            )
        return self._client

    def run_query(
        self, query: str
    ) -> pd.DataFrame:
        """Run a query via boto3 Athena API.

        Args:
            query: SQL query string to execute.

        Returns:
            A DataFrame with the query results,
            or an empty DataFrame for non-SELECT
            statements.

        Raises:
            RuntimeError: If the query fails or
                is cancelled.
        """
        return ops.run_query(
            client=self.client,
            query=query,
            database=self.database,
            s3_output=self.s3_output,
            workgroup=self.workgroup,
            poll_interval=self.poll_interval,
        )

    def create_insert(
        self,
        query: str,
        table_name: str,
        partition_cols: Optional[
            List[str]
        ] = None,
    ) -> str:
        """Create a table or insert into an
        existing one.

        Args:
            query: SELECT query to materialise.
            table_name: Destination table name
                (schema.table format).
            partition_cols: Partition keys used
                only when creating a new table.

        Returns:
            The resolved table name.
        """
        return ops.create_insert(
            query=query,
            table_name=table_name,
            s3_output=self.s3_output,
            workgroup=self.workgroup,
            partition_cols=partition_cols,
        )

    def create_ctas(
        self,
        query: str,
        table_name: str,
        partition_cols: Optional[
            List[str]
        ] = None,
    ) -> str:
        """Create a table from query using CTAS.

        Args:
            query: SELECT query to materialise.
            table_name: Name for the new table
                (schema.table format).
            partition_cols: Partition keys for
                the new table.

        Returns:
            The resolved table name.

        Raises:
            Exception: If the table already
                exists.
        """
        return ops.create_ctas(
            query=query,
            table_name=table_name,
            s3_output=self.s3_output,
            partition_cols=partition_cols,
        )

    def get_query_metadata(
        self, query: str
    ) -> Metadata:
        """Return metadata for a query result
        set.

        Args:
            query: SQL query to inspect.

        Returns:
            Metadata with column types.
        """
        return ops.get_query_metadata(
            query=query,
            database=self.database,
        )

    def get_table_metadata(
        self, table_name: str
    ) -> Metadata:
        """Return metadata for a physical table.

        Args:
            table_name: Fully qualified name
                (schema.table).

        Returns:
            Metadata with column and partition
            information.
        """
        return ops.get_table_metadata(
            table_name=table_name,
            region=self.region,
        )

    def drop(self, table_name: str) -> None:
        """Drop a table.

        Args:
            table_name: Fully qualified name
                (schema.table).
        """
        ops.drop(table_name)

    def list_partitions(
        self,
        table_name: str,
        *filters: str,
    ) -> List[Tuple[str, ...]]:
        """List partitions, optionally filtered
        via fnmatch.

        Args:
            table_name: Fully qualified name
                (schema.table).
            *filters: Optional fnmatch patterns,
                one per partition column.

        Returns:
            List of tuples of partition values.
        """
        return ops.list_partitions(
            table_name, *filters
        )

    def drop_partitions(
        self,
        table_name: str,
        *filters: str,
    ) -> None:
        """Drop partitions, optionally filtered
        via fnmatch.

        Args:
            table_name: Fully qualified name
                (schema.table).
            *filters: fnmatch patterns, one per
                partition column.
        """
        ops.drop_partitions(table_name, *filters)
