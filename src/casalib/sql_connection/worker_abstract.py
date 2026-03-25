'''
Worker abstract class
'''
from abc import ABC, abstractmethod
from typing import Optional, List

import pandas as pd

from .metadata import Metadata


class WorkerAbstract(ABC):
    @abstractmethod
    def run_query(
        self,
        query: str,
    ) -> pd.DataFrame:
        '''
        Method to run a query in the database.

        If the query returns data, must return a DataFrame;
        if the query doesn't return data (like CREATE,
        DROP, etc), must return an empty DataFrame.

        In case of error, must raise.
        '''


    @abstractmethod
    def create_insert(
        self,
        query: str,
        table_name: str,
        partition_cols: Optional[List[str]] = None,
    ) -> str:
        '''
        Method will create/insert data in a query. If
        the table doesn't exist, we will create it with
        schema definition and insert data.

        If the table exists, we will reorder the query
        columns and insert the data.

        `partition_cols` will be ignored if the table
        exists.
        '''

    @abstractmethod
    def create_ctas(
        self,
        query: str,
        table_name: str,
        partition_cols: Optional[List[str]] = None,
    ) -> str:
        '''
        Method to create a table using CREATE TABLE AS.

        Must raise if the table exists.
        '''

    @abstractmethod
    def get_query_metadata(
        self,
        query: str
    ) -> Metadata:
        '''
        Return the query metadata for a given query,
        informing the columns and types.
        '''