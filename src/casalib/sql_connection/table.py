from dataclasses import dataclass

import pandas as pd

from .metadata import Metadata
from .query import Query
from .worker_abstract import WorkerAbstract

@dataclass
class Table:
    table_name: str

    @property
    def worker(self) -> WorkerAbstract:
        worker = getattr(self, 'worker_', None)

        if worker:
            return worker

        raise RuntimeError('`Worker` not set. Please check')

    def set_worker(self, worker: WorkerAbstract) -> "Query":
        self.worker_ = worker

    def drop(self) -> "Table":
        self.worker.drop(self.table_name)
        return self

    def metadata(self) -> Metadata:
        return self.worker.get_table_metadata(self.table_name)

    def list_partitions(self, *filters: str) -> pd.DataFrame:
        '''
        List all the partitions as a DataFrame. Will filter
        the dataframe using the fnmatch filters with the
        information passed.
        '''

    def drop_partitions(self, *filters: str) -> pd.DataFrame:
        '''
        Drop the partitions from table. Will filter
        the dataframe using the fnmatch filters with the
        information passed.
        '''

    def query(self) -> Query:
        '''
        Create a query object, to query the table
        '''
        return Query()

    def collect(self) -> pd.DataFrame:
        '''
        Collect the table
        '''
        query = self.query.collect()