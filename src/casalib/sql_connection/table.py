from dataclasses import dataclass
from typing import List, Optional, Tuple, Type

import pandas as pd

from .metadata import Metadata
from .query import Query
from .sql_abstract import SqlDialectAbstract
from .worker_abstract import WorkerAbstract


@dataclass
class Table:
    table_name: str
    dialect: Type[SqlDialectAbstract]

    @property
    def worker(self) -> WorkerAbstract:
        worker = getattr(self, 'worker_', None)

        if worker:
            return worker

        raise RuntimeError('`Worker` not set. Please check')

    def set_worker(self, worker: Optional[WorkerAbstract]) -> "Table":
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
        partition_cols = list(meta.table.partition_cols.keys())
        partitions = self.worker.list_partitions(self.table_name, *filters)
        return pd.DataFrame(partitions, columns=partition_cols)

    def drop_partitions(self, *filters: str) -> None:
        '''
        Drop the partitions from table. Will filter
        each partition column using the corresponding fnmatch
        filter passed as positional argument.
        '''
        self.worker.drop_partitions(self.table_name, *filters)

    @property
    def query(self) -> Query:
        '''
        Create a Query object that selects all data from the table.
        '''
        q = Query.from_table_name(self.table_name, self.dialect)
        q.set_worker(self.worker)
        return q

    def collect(self) -> pd.DataFrame:
        '''
        Collect the table as a DataFrame.
        '''
        return self.query.collect()
