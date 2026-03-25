"""Query class."""
from dataclasses import dataclass
from typing import List, Optional, Type

import pandas as pd

from .metadata import Metadata
from .sql_abstract import SqlDialectAbstract
from .worker_abstract import WorkerAbstract


class _QueryBuilder:
    """Wraps a dialect instance and wires worker/dialect onto returned Queries.

    Any ``Query`` returned by a dialect method call is reconstructed with
    the type-based dialect and the worker already set, so callers do not
    need to configure them manually.

    Attributes:
        _instance: Instantiated dialect used to generate SQL.
        _dialect: Dialect type to attach to returned Query objects.
        _worker: Worker to attach to returned Query objects.
    """

    def __init__(
        self,
        dialect_instance: SqlDialectAbstract,
        dialect: Type[SqlDialectAbstract],
        worker: Optional[WorkerAbstract],
    ) -> None:
        self._instance = dialect_instance
        self._dialect = dialect
        self._worker = worker

    def __getattr__(self, name: str):
        method = getattr(self._instance, name)

        if not callable(method):
            return method

        def _wrapper(*args, **kwargs):
            result = method(*args, **kwargs)

            if isinstance(result, Query):
                return Query(
                    query=result.query,
                    dialect=self._dialect,
                ).set_worker(self._worker)

            return result

        return _wrapper


@dataclass
class Query:
    """A stored SQL string paired with a dialect type and an optional worker.

    Attributes:
        query: SQL string.
        dialect: Dialect class used to generate and transform SQL.
    """

    query: str
    dialect: Type[SqlDialectAbstract]

    @classmethod
    def from_table_name(
        cls,
        table_name: str,
        dialect: Type[SqlDialectAbstract],
    ) -> "Query":
        """Create a Query that selects all data from a table.

        Args:
            table_name: Fully qualified table name.
            dialect: Dialect class to use for SQL generation.

        Returns:
            A Query wrapping ``SELECT * FROM <table_name>``.
        """
        return cls(
            query=dialect(input_query=table_name).select(table_name).query,
            dialect=dialect,
        )

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

    def set_worker(self, worker: Optional[WorkerAbstract]) -> "Query":
        """Set the database worker.

        Args:
            worker: Worker instance to attach, or None.

        Returns:
            Self, for chaining.
        """
        self.worker_ = worker
        return self

    @property
    def q(self) -> _QueryBuilder:
        """Return a dialect builder initialised with the current query string.

        Any Query returned by the builder's methods will have the worker
        and dialect already set.

        Returns:
            A _QueryBuilder wrapping the instantiated dialect.
        """
        return _QueryBuilder(
            dialect_instance=self.dialect(input_query=self.query),
            dialect=self.dialect,
            worker=getattr(self, 'worker_', None),
        )

    def collect(self) -> pd.DataFrame:
        """Execute the query and return results as a DataFrame.

        Returns:
            A DataFrame with the query results.
        """
        return self.worker.run_query(self.query)

    def create_insert(
        self,
        table_name: str,
        partition_cols: Optional[List[str]] = None,
    ) -> "Query":
        """Create the destination table if absent, then insert query results.

        Args:
            table_name: Destination table name.
            partition_cols: Partition keys to use on table creation.

        Returns:
            Self, for chaining.
        """
        self.worker.create_insert(
            query=self.query,
            table_name=table_name,
            partition_cols=partition_cols,
        )
        return self

    def create_ctas(
        self,
        table_name: str,
        partition_cols: Optional[List[str]] = None,
    ) -> "Query":
        """Create a table from this query using CREATE TABLE AS SELECT.

        Args:
            table_name: Name for the new table.
            partition_cols: Partition keys for the new table.

        Returns:
            Self, for chaining.
        """
        self.worker.create_ctas(
            query=self.query,
            table_name=table_name,
            partition_cols=partition_cols,
        )
        return self

    def metadata(self) -> Metadata:
        """Return metadata for the query result set.

        Returns:
            Metadata describing the columns and types of the result set.
        """
        return self.worker.get_query_metadata(self.query)
