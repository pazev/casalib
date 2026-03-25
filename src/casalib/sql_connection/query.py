"""
`Query` class

In this class, we have methods to work with a stored query.
"""
from dataclasses import dataclass
from typing import List, Optional, Type

import pandas as pd

from .metadata import Metadata
from .sql_abstract import SqlDialectAbstract
from .worker_abstract import WorkerAbstract


@dataclass
class Query:
    query: str
    dialect: Type[SqlDialectAbstract]

    @classmethod
    def from_table_name(
        cls,
        table_name: str,
        dialect: Type[SqlDialectAbstract],
    ) -> "Query":
        '''
        Create a Query that selects all data from `table_name`
        using the given dialect.
        '''
        return cls(
            query=dialect(input_query=table_name).select(table_name).query,
            dialect=dialect,
        )

    @property
    def worker(self) -> WorkerAbstract:
        ''' Return the database worker '''
        worker = getattr(self, 'worker_', None)

        if worker:
            return worker

        raise RuntimeError('`Worker` not set. Please check')

    def set_worker(self, worker: Optional[WorkerAbstract]) -> "Query":
        self.worker_ = worker
        return self

    def collect(self) -> pd.DataFrame:
        return self.worker.run_query(self.query)

    def create_insert(
        self,
        table_name: str,
        partition_cols: Optional[List[str]] = None,
    ) -> "Query":
        '''
        Create the table if it doesn't exist. Insert
        if it exist.
        '''
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
        '''
        Create the table via CTAS command.
        '''
        self.worker.create_ctas(
            query=self.query,
            table_name=table_name,
            partition_cols=partition_cols,
        )
        return self

    def metadata(self) -> Metadata:
        '''
        Get query metadata, returning the columns and types
        for the query.
        '''
        return self.worker.get_query_metadata(self.query)
