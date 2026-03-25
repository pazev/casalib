'''
Connection class
'''
from dataclasses import dataclass
from typing import Optional

from .query import Query
from .sql_abstract import SqlDialectAbstract
from .table import Table
from .worker_abstract import WorkerAbstract


@dataclass
class Connection:
    dialect: SqlDialectAbstract

    @property
    def worker(self) -> Optional[WorkerAbstract]:
        return getattr(self, 'worker_', None)

    def set_worker(self, worker: WorkerAbstract) -> "Connection":
        self.worker_ = worker
        return self

    def query(self, query: str) -> Query:
        return Query(query=query, dialect=self.dialect).set_worker(self.worker)

    def table(self, table_name: str) -> Table:
        return Table(table_name=table_name, dialect=self.dialect).set_worker(self.worker)
