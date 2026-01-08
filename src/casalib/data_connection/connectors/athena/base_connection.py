"""
Module defines the base connetion class, that will be used
to perform the tasks with the AWS Athena.

All advanced features will be implemented on
ConnectionAbstract.

In this module, two classes are defined: Boto3SessionMaker,
that provides a function to connect with Boto3; and
AthenaBaseConnection, that implements the tasks.

By design, the functionalities are implemented on modules
folder, as functions; the class only makes the calls.
"""
from dataclasses import dataclass, field
from typing import Dict, List, Optional, Tuple, Union

import boto3
import pandas as pd

from ...base import (
    BaseConnectionAbstract,
    Metadata,
)

from .modules.create_insert import (
    create_insert,
    create_ctas,
)
from .modules.drop import (
    drop_table,
)
from .modules.input_tables import get_input_tables
from .modules.metadata import (
    get_table_metadata,
    get_query_metadata,
)
from .modules.partitions import (
    list_partitions,
    drop_partitions,
)
from .modules.querying import (
    run_query_get_pandas,
    run_table_get_pandas,
)
from .modules.send_pandas import (
    create_table_pandas_dataframe,
)


@dataclass
class Boto3SessionMaker:
    """ Class responsible for storing and managing the creation
        of connections to boto3 whenever necessary.
    """
    aws_access_key_id: Optional[str] = field(default=None, repr=False)
    aws_secret_access_key: Optional[str] = field(default=None, repr=False)
    aws_session_token: Optional[str] = field(default=None, repr=False)
    profile_name: Optional[str] = field(default=None, repr=False)
    region_name: Optional[str] = None

    def make(self) -> boto3.Session:
        """ Creates the boto3 session """
        par = {}

        if self.aws_access_key_id is not None:
            par['aws_access_key_id'] = (
                self.aws_access_key_id
            )

        if self.aws_secret_access_key is not None:
            par['aws_secret_access_key'] = (
                self.aws_secret_access_key
            )

        if self.aws_session_token is not None:
            par['aws_session_token'] = (
                self.aws_session_token
            )

        if self.profile_name is not None:
            par['profile_name'] = (
                self.profile_name
            )

        if self.region_name is not None:
            par['region_name'] = self.region_name

        return boto3.Session(**par) # type: ignore


@dataclass
class AthenaBaseConnection(BaseConnectionAbstract):
    """ Class for connection in Athena. """
    schema_name: str
    workgroup: str
    s3_staging_dir: str
    data_catalog: str
    boto3_session_maker: Boto3SessionMaker
    table_prefix: str = ''

    def query_method_(self, query: str) -> pd.DataFrame:
        """
        Returns the query result as a DataFrame
        """
        return run_query_get_pandas(
            boto3_session=self.boto3_session_maker.make(),
            data_catalog=self.data_catalog,
            default_schema_name=self.schema_name,
            workgroup=self.workgroup,
            s3_output=self.s3_staging_dir,
            table_prefix=self.table_prefix,
            query=query
        )

    def table(
        self,
        table_name: str,
        samples: Union[int, None] = 100
    ) -> pd.DataFrame:
        """ Returns a sample of the table. The default is 100
            records, but this number can be changed in the
            `samples` parameter. If `samples` receives a
            negative number or None, returns the entire
            table.
        """
        return run_table_get_pandas(
            boto3_session=self.boto3_session_maker.make(),
            data_catalog=self.data_catalog,
            default_schema_name=self.schema_name,
            workgroup=self.workgroup,
            s3_output=self.s3_staging_dir,
            table_prefix=self.table_prefix,
            table_name=table_name,
            samples=samples,
        )

    def metadata(
        self,
        query: Optional[str] = None,
        table_name: Optional[str] = None
    ) -> Metadata:
        """ Returns the metadata of the table or query.
            Only one of the two should be set.
        """
        if (query is not None) and (table_name is not None):
            raise ValueError(
                "Either `query` or `tablename` needs to be "
                "set."
            )

        # Captures the metadata
        if table_name is not None:
            return get_table_metadata(
                table_name=table_name,
                boto3_session=self.boto3_session_maker.make(),
                data_catalog=self.data_catalog,
                default_schema_name=self.schema_name,
                workgroup=self.workgroup,
            )

        if query is not None:
            return get_query_metadata(
                query=query,
                boto3_session=self.boto3_session_maker.make(),
                data_catalog=self.data_catalog,
                default_schema_name=self.schema_name,
                workgroup=self.workgroup,
            )

        raise ValueError(
            "Either `query` or `tablename` needs to be "
            "set."
        )

    def drop(self, table_name: str) -> "AthenaBaseConnection":
        """ Drops a table
        """
        drop_table(
            boto3_session=self.boto3_session_maker.make(),
            data_catalog=self.data_catalog,
            default_schema_name=self.schema_name,
            workgroup=self.workgroup,
            table_name=table_name,
        )
        return self

    def create_insert(
        self,
        query: str,
        table_name: str,
        partition_cols: Optional[List[str]] = None,
    ) -> "AthenaBaseConnection":
        """ Creates a table if it doesn't exist and inserts data.
            Performs column reordering if necessary.
        """
        create_insert(
            boto3_session=self.boto3_session_maker.make(),
            data_catalog=self.data_catalog,
            default_schema_name=self.schema_name,
            workgroup=self.workgroup,
            s3_output=self.s3_staging_dir,
            table_name=table_name,
            partition_cols=partition_cols,
            query=query,
        )
        return self

    def create_ctas(
        self,
        query: str,
        table_name: str,
        partition_cols: Optional[List[str]] = None,
    ) -> "AthenaBaseConnection":
        """ Creates a table via CREATE TABLE AS
        """
        create_ctas(
            boto3_session=self.boto3_session_maker.make(),
            data_catalog=self.data_catalog,
            default_schema_name=self.schema_name,
            workgroup=self.workgroup,
            s3_output=self.s3_staging_dir,
            table_name=table_name,
            partition_cols=partition_cols,
            query=query,
        )

        return self

    def list_partitions(
        self,
        table_name: str,
    ) -> Dict[Tuple[str, ...], str]:
        """ Lists the partitions """
        return list_partitions(
            boto3_session=self.boto3_session_maker.make(),
            default_schema_name=self.schema_name,
            table_name=table_name,
        )

    def drop_partitions(
        self,
        table_name: str,
        partitions_to_drop: List[Tuple[str, ...]],
    ) -> "AthenaBaseConnection":
        """ Drops the indicated partitions in the table """
        drop_partitions(
            boto3_session=self.boto3_session_maker.make(),
            default_schema_name=self.schema_name,
            table_name=table_name,
            partitions_to_drop=partitions_to_drop
        )
        return self

    def send_pandas(
        self,
        dff: pd.DataFrame,
        table_name: str,
        partition_cols: Optional[List[str]] = None
    ) -> Metadata:
        """ Sends a pandas DataFrame to the database """
        return create_table_pandas_dataframe(
            boto3_session=self.boto3_session_maker.make(),
            data_catalog=self.data_catalog,
            workgroup=self.workgroup,
            default_schema_name=self.schema_name,
            table_name=table_name,
            s3_output=self.s3_staging_dir,
            dff=dff,
            partition_cols=partition_cols,
        )

    def get_input_tables(
        self,
        query: str
    ) -> List[str]:
        """ Get the required tables for the given query """
        return get_input_tables(
            query=query,
            boto3_session=self.boto3_session_maker.make(),
            data_catalog=self.data_catalog,
            default_schema_name=self.schema_name,
            workgroup=self.workgroup,
        )
