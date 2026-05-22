"""Connection class."""
from dataclasses import dataclass, field
from typing import Optional, Type

from .query import AsyncQuery, Query
from .sql_dialect_abstract import SqlDialectAbstract
from .table import AsyncTable, Table
from .worker_abstract import (
    AsyncWorkerAbstract,
    WorkerAbstract,
)


@dataclass
class Connection:
    """Entry point for building Query and Table
    objects.

    Attributes:
        dialect: Dialect class used for SQL
            generation.
    """

    dialect: Type[SqlDialectAbstract]
    worker_: Optional[WorkerAbstract] = field(
        default=None, init=False, repr=False
    )

    @property
    def worker(self) -> Optional[WorkerAbstract]:
        """Return the database worker, or None
        if not set.

        Returns:
            The configured WorkerAbstract
            instance, or None.
        """
        return self.worker_

    def set_worker(
        self, worker: WorkerAbstract
    ) -> "Connection":
        """Set the database worker.

        Args:
            worker: Worker instance to attach.

        Returns:
            Self, for chaining.
        """
        self.worker_ = worker
        return self

    def query(self, query: str) -> Query:
        """Create a Query from a raw SQL string.

        Args:
            query: SQL query string.

        Returns:
            A Query with the dialect and worker
            already set.
        """
        return Query(
            query=query, dialect=self.dialect
        ).set_worker(self.worker)

    def table(self, table_name: str) -> Table:
        """Create a Table object for the given
        table name.

        Args:
            table_name: Fully qualified table
                name.

        Returns:
            A Table with the dialect and worker
            already set.
        """
        return Table(
            table_name=table_name,
            dialect=self.dialect,
        ).set_worker(self.worker)


@dataclass
class AsyncConnection:
    """Async counterpart of Connection.

    Attributes:
        dialect: Dialect class for SQL
            generation.
    """

    dialect: Type[SqlDialectAbstract]
    worker_: Optional[
        AsyncWorkerAbstract
    ] = field(
        default=None, init=False, repr=False
    )

    @property
    def worker(
        self,
    ) -> Optional[AsyncWorkerAbstract]:
        """Return the async worker, or None.

        Returns:
            The configured worker, or None.
        """
        return self.worker_

    def set_worker(
        self, worker: AsyncWorkerAbstract
    ) -> "AsyncConnection":
        """Set the async worker.

        Args:
            worker: Worker to attach.

        Returns:
            Self, for chaining.
        """
        self.worker_ = worker
        return self

    def query(self, query: str) -> AsyncQuery:
        """Create an AsyncQuery from raw SQL.

        Args:
            query: SQL query string.

        Returns:
            An AsyncQuery with dialect and
            worker already set.
        """
        return AsyncQuery(
            query=query, dialect=self.dialect
        ).set_worker(self.worker_)

    def table(
        self, table_name: str
    ) -> AsyncTable:
        """Create an AsyncTable for a table.

        Args:
            table_name: Fully qualified table
                name.

        Returns:
            An AsyncTable with dialect and
            worker already set.
        """
        return AsyncTable(
            table_name=table_name,
            dialect=self.dialect,
        ).set_worker(self.worker_)
