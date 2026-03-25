import fnmatch
from dataclasses import dataclass

import pandas as pd

from .metadata import Metadata
from .query import Query
from .sql_abstract import SqlDialectAbstract
from .worker_abstract import WorkerAbstract


@dataclass
class Table:
    table_name: str
    dialect: SqlDialectAbstract

    @property
    def worker(self) -> WorkerAbstract:
        worker = getattr(self, 'worker_', None)

        if worker:
            return worker

        raise RuntimeError('`Worker` not set. Please check')

    def set_worker(self, worker: WorkerAbstract) -> "Table":
        self.worker_ = worker
        return self

    def drop(self) -> "Table":
        self.worker.drop(self.table_name)
        return self

    def metadata(self) -> Metadata:
        return self.worker.get_table_metadata(self.table_name)

    def list_partitions(self, *filters: str) -> pd.DataFrame:
        '''
        List all the partitions as a DataFrame. Will filter
        each partition column using the corresponding fnmatch
        filter passed as positional argument.
        '''
        meta = self.metadata()
        partitions = self.worker.list_partitions(self.table_name)

        if not filters or meta.table is None:
            return partitions

        mask = pd.Series(True, index=partitions.index)
        for col, pattern in zip(meta.table.partition_cols.keys(), filters):
            mask &= partitions[col].apply(lambda v, p=pattern: fnmatch.fnmatch(str(v), p))

        return partitions[mask]

    def drop_partitions(self, *filters: str) -> None:
        '''
        Drop the partitions from table. Will filter
        each partition column using the corresponding fnmatch
        filter passed as positional argument.
        '''
        partitions = self.list_partitions(*filters)
        self.worker.drop_partitions(self.table_name, partitions)

    @property
    def query(self) -> Query:
        '''
        Create a Query object that selects all data from the table.
        '''
        q = Query(f"SELECT * FROM {self.table_name}", self.dialect)
        q.set_worker(self.worker)
        return q

    def collect(self) -> pd.DataFrame:
        '''
        Collect the table as a DataFrame.
        '''
        return self.query.collect()
