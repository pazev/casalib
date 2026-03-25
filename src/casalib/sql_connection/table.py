"""Table class."""
from dataclasses import dataclass
from typing import List, Optional, Tuple, Type

import pandas as pd

from .metadata import Metadata
from .query import Query
from .sql_abstract import SqlDialectAbstract
from .worker_abstract import WorkerAbstract


@dataclass
class Table:
    """A named table with an associated dialect and optional worker.

    Attributes:
        table_name: Fully qualified table name.
        dialect: Dialect class used for SQL generation.
    """

    table_name: str
    dialect: Type[SqlDialectAbstract]

    @property
    def worker(self) -> WorkerAbstract:
        """Return the database worker.

        Returns:
            The configured WorkerAbstract instance.

        Raises:
            RuntimeError: If the worker has not been set.
        """
        worker = getattr(self, 'worker_', None)

        if worker:
            return worker

        raise RuntimeError('`Worker` not set. Please check')

    def set_worker(self, worker: Optional[WorkerAbstract]) -> "Table":
        """Set the database worker.

        Args:
            worker: Worker instance to attach, or None.

        Returns:
            Self, for chaining.
        """
        self.worker_ = worker
        return self

    def drop(self) -> "Table":
        """Drop the table from the database.

        Returns:
            Self, for chaining.
        """
        self.worker.drop(self.table_name)
        return self

    def metadata(self) -> Metadata:
        """Return metadata for this table.

        Returns:
            Metadata describing the table's columns and partition information.
        """
        return self.worker.get_table_metadata(self.table_name)

    def list_partitions(self, *filters: str) -> pd.DataFrame:
        """List partitions as a DataFrame, optionally filtered.

        Each filter is an fnmatch pattern applied to the corresponding
        partition column in order.

        Args:
            *filters: Optional fnmatch patterns, one per partition column.

        Returns:
            A DataFrame with partition column names as columns and one row
            per matching partition.
        """
        meta = self.metadata()
        partition_cols = list(meta.table.partition_cols.keys())
        partitions = self.worker.list_partitions(self.table_name, *filters)
        return pd.DataFrame(partitions, columns=partition_cols)

    def drop_partitions(self, *filters: str) -> None:
        """Drop partitions from the table, optionally filtered.

        Each filter is an fnmatch pattern applied to the corresponding
        partition column in order.

        Args:
            *filters: Optional fnmatch patterns, one per partition column.
        """
        self.worker.drop_partitions(self.table_name, *filters)

    @property
    def query(self) -> Query:
        """Return a Query that selects all data from this table.

        Returns:
            A Query wrapping ``SELECT * FROM <table_name>``.
        """
        q = Query.from_table_name(self.table_name, self.dialect)
        q.set_worker(self.worker)
        return q

    def collect(self) -> pd.DataFrame:
        """Collect all data from the table as a DataFrame.

        Returns:
            A DataFrame with all rows and columns from the table.
        """
        return self.query.collect()
