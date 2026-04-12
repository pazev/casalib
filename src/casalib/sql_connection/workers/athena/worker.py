"""AwsAthenaWorker — WorkerAbstract for Athena.

Delegates all operations to _operations.
"""
from dataclasses import dataclass, field
from typing import List, Optional, Tuple

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
        catalog: Athena data catalog name.
        session_: Optional pre-built boto3
            Session for dependency injection.
            When ``None`` (default) a session
            is created lazily using ``region``.
    """

    database: str
    s3_output: str
    region: str
    workgroup: str = "primary"
    poll_interval: float = 0.5
    catalog: str = "AwsDataCatalog"
    session_: Optional[boto3.Session] = field(
        default=None, repr=False
    )

    @property
    def session(self) -> boto3.Session:
        """Return the boto3 Session.

        Uses the injected ``session_`` if
        provided; otherwise creates one lazily
        from ``region`` and caches it.
        """
        if self.session_ is None:
            self.session_ = boto3.Session(
                region_name=self.region,
            )
        return self.session_

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
            session=self.session,
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

        When the table exists, validates the
        query schema against the table schema
        and reorders columns to match before
        inserting.

        Args:
            query: SELECT query to materialise.
            table_name: Destination table name
                (schema.table format).
            partition_cols: Partition keys used
                only when creating a new table.

        Returns:
            The resolved table name.

        Raises:
            ValueError: A required column is
                absent from the query output.
            TypeError: A column type in the
                query is incompatible with the
                table definition.
        """
        return ops.create_insert(
            session=self.session,
            query=query,
            table_name=table_name,
            s3_output=self.s3_output,
            workgroup=self.workgroup,
            database=self.database,
            catalog=self.catalog,
            poll_interval=self.poll_interval,
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
        set with native Athena types.

        Args:
            query: SQL query to inspect.

        Returns:
            Metadata with native Athena column
            types.
        """
        return ops.get_query_metadata(
            session=self.session,
            query=query,
            database=self.database,
            s3_output=self.s3_output,
            workgroup=self.workgroup,
            poll_interval=self.poll_interval,
            catalog=self.catalog,
        )

    def get_table_metadata(
        self, table_name: str
    ) -> Metadata:
        """Return metadata for a physical table
        with native Athena types.

        Args:
            table_name: Fully qualified name
                (schema.table).

        Returns:
            Metadata with native Athena column
            and partition information.
        """
        return ops.get_table_metadata(
            session=self.session,
            table_name=table_name,
            catalog=self.catalog,
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
