"""Connection class."""
from dataclasses import dataclass
from typing import Optional, Type

from .query import Query
from .sql_abstract import SqlDialectAbstract
from .table import Table
from .worker_abstract import WorkerAbstract


@dataclass
class Connection:
    """Entry point for building Query and Table objects.

    Attributes:
        dialect: Dialect class used for SQL generation.
    """

    dialect: Type[SqlDialectAbstract]

    @property
    def worker(self) -> Optional[WorkerAbstract]:
        """Return the database worker, or None if not set.

        Returns:
            The configured WorkerAbstract instance, or None.
        """
        return getattr(self, 'worker_', None)

    def set_worker(self, worker: WorkerAbstract) -> "Connection":
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
            A Query with the dialect and worker already set.
        """
        return Query(query=query, dialect=self.dialect).set_worker(self.worker)

    def table(self, table_name: str) -> Table:
        """Create a Table object for the given table name.

        Args:
            table_name: Fully qualified table name.

        Returns:
            A Table with the dialect and worker already set.
        """
        return Table(table_name=table_name, dialect=self.dialect).set_worker(self.worker)
