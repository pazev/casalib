"""
`Query` class

In this class, we have methods to work with a stored query.
"""
from dataclasses import dataclass
from typing import List, Optional

import pandas as pd

from .metadata import Metadata
from .sql_abstract import SqlDialectAbstract
from .worker_abstract import WorkerAbstract


@dataclass
class Query:
    query: str
    dialect: SqlDialectAbstract

    def set_worker(self, worker: WorkerAbstract) -> "Query":
        self.worker_ = worker

    def get_worker(self) -> WorkerAbstract:
        worker = getattr(self, 'worker_', None)

        if worker:
            return worker

        raise RuntimeError('`Worker` not set. Please check')

    def run(self) -> pd.DataFrame:
        worker = self.get_worker()
        return worker.run_query(self.query)

    def create_insert(
        self,
        table_name: str,
        partition_cols: Optional[List[str]] = None,
    ) -> "Query":
        '''
        Create the table if it doesn't exist. Insert
        if it exist.
        '''
        worker = self.get_worker()
        return worker.create_insert(
            query=self.query,
            table_name=table_name,
            partition_cols=partition_cols,
        )

    def create_ctas(
        self,
        table_name: str,
        partition_cols: Optional[List[str]] = None,
    ) -> "Query":
        '''
        Create the table via CTAS command.
        '''
        worker = self.get_worker()
        return worker.create_ctas(
            query=self.query,
            table_name=table_name,
            partition_cols=partition_cols,
        )

    def metadata(self) -> Metadata:
        '''
        Get query metadata, returning the columns and types
        for the query.
        '''
        worker = self.get_worker()
        return worker.get_query_metadata(self.query)
