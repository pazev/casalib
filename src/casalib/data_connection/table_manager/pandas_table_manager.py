"""
Module defines a TableManager that applies a pandas
pipeline to a Table
"""
from dataclasses import dataclass, field
from typing import (
    Any, Callable, Dict, List, Optional, Tuple
)

import jinja2
import pandas as pd

from .base import (
    TableManagerAbstract,
)
from .helpers import TableHelper
from ..base import ConnectionAbstract, Metadata


@dataclass
class PandasTableManager(TableManagerAbstract):
    """
    Pandas Table Manager

    Applies a Pandas pipeline, query_template_dict, to a
    retrieved table.

    We can extract several tables using different queries
    in query_template_dict.

    We have the transform_and_load_ parameter, for advanced
    use; basically, it expects a function that will receive
    the load function and the dictionary of dataframes; it
    allows to increment transform and load.
    """
    table_name: str
    required_params: List[str]

    pandas_pipeline: Optional[
        Callable[..., pd.DataFrame]
    ] = field(default=None, repr=False)

    query_template_dict: Dict[str, str] = field(
        repr=False,
        default_factory=dict,
    )
    partition_cols: Optional[List[str]] = None

    transform_and_load_: Optional[Callable[
        [
            Callable[[pd.DataFrame], None],
            Dict[str, pd.DataFrame],
            Any
        ],
        None
    ]] = field(default=None, repr=False)

    def __post_init__(self):
        """ Post-init """
        self.helper = TableHelper(
            table_name=self.table_name,
        )

        # Pandas pipeline or transform_and_load_ must be set
        if (
            self.pandas_pipeline is None and
            self.transform_and_load_ is None
        ):
            raise ValueError(
                "pandas_pipeline must be set"
            )

        if self.transform_and_load_ is None:
            self.transform_and_load_ = (
                self.standard_transform_and_load_
            )

    def set_conn_maker(
        self,
        conn_maker: Callable[[], ConnectionAbstract]
    ) -> "PandasTableManager":
        """ Set the connection maker """
        self.helper.set_conn_maker(conn_maker)
        return self

    def get_conn(self) -> ConnectionAbstract:
        """ Get a connection """
        return self.helper.get_conn()

    def get_table_name(self) -> str:
        """ Return the table_name """
        return self.helper.get_table_name()

    def get_partition_cols(self) -> Optional[List[str]]:
        """ Returns the configured parittion cols """
        return self.partition_cols

    def list_partitions(self) -> Dict[Tuple[str, ...], str]:
        """ List partitions """
        return self.helper.list_partitions()

    def list_partitions_filter(
        self,
        *filters: str
    ) -> List[Tuple[str, ...]]:
        """
        Filter the partitions list using the filter passed.
        """
        filtered_partitions = (
            self.helper.list_partitions_filter(*filters)
        )
        return filtered_partitions

    def list_partitions_filter_pd(
        self,
        *filters: str
    ) -> pd.DataFrame:
        """
        Filter the partitions list using the filter passed.
        Return the result as pandas DataFrame
        """
        filtered_partitions = (
            self.helper.list_partitions_filter_pd(*filters)
        )
        return filtered_partitions

    def sample(self, samples: int = 100) -> pd.DataFrame:
        """
        Select some sample from the table.
        """
        return self.helper.sample(samples)

    def metadata(self) -> Metadata:
        """ Returns the table metadata """
        return self.helper.metadata()

    def last_partition_query(
        self,
        cross_columns_: Optional[List[str]] = None,
        max_column_: Optional[str] = None,
        **filters: List[Any]
    ) -> List[str]:
        """ Return last partition query for the table """
        return self.helper.last_partition_query(
            cross_columns_=cross_columns_,
            max_column_=max_column_,
            **filters
        )

    def drop(self) -> "PandasTableManager":
        """ Drop the table """
        self.helper.drop()
        return self

    def drop_partitions(
        self,
        partitions_to_drop: List[Tuple[str, ...]]
    ) -> "PandasTableManager":
        """ Drop partitions """
        self.helper.drop_partitions(partitions_to_drop)
        return self

    def drop_partitions_filter(
        self,
        *filters: str
    ) -> "PandasTableManager":
        """
        Drop partitions using the fnmatch filter passed.
        """
        self.helper.drop_partitions_filter(*filters)
        return self


    # Methods required for the run
    def input_vars(self) -> List[str]:
        """ List the variables in the template """
        return self.required_params

    def missing_params_(self, **params) -> List[str]:
        """ Validate if all required variables are given """
        var_miss = list(
            set(self.input_vars()) - set(params)
        )

        if var_miss:
            raise ValueError(f"Missing params {var_miss}")

        return var_miss

    def make_query(self, **params) -> Dict[str, str]:
        """ Make the query that will be executed """
        self.missing_params_(**params)

        env = jinja2.Environment(
            undefined=jinja2.StrictUndefined
        )

        return {
            name: query

            for name, query_template_str in (
                self.query_template_dict.items()
            )
            for template in [
                env.from_string(query_template_str)
            ]
            for query in [template.render(**params)]
        }

    def get_table_input(
        self, **params
    ) -> List[str]:
        """ Return the list of table inputs """
        query_dict = self.make_query(**params)

        input_tables = list(
            set(
                tab
                for query in query_dict.values()
                for tab in (
                    self
                    .helper
                    .get_conn()
                    .get_input_tables(query)
                )
            )
        )
        return input_tables

    def extract_dataframes_(
        self, **params
    ) -> Dict[str, pd.DataFrame]:
        """ Extract the dataframes (Extract of ETL) """
        return {
            name: self.get_conn().query(query)
            for name, query in (
                self.make_query(**params).items()
            )
        }

    def load_function_(self, dff: pd.DataFrame) -> None:
        """
        Function to load the dataframe in the connection
        """
        self.get_conn().send_pandas(
            dff=dff,
            table_name=self.get_table_name(),
            partition_cols=self.get_partition_cols()
        )

    def standard_transform_and_load_(
        self,
        load_function: Callable[[pd.DataFrame], None],
        dict_dffs_: Dict[str, pd.DataFrame],
        **kwargs: Any,
    ) -> None:
        res = self.pandas_pipeline(**dict_dffs_)
        load_function(res)

    def run(self, **params) -> "PandasTableManager":
        """ Run the TableManager """
        # Validate if all required params are being passed
        self.missing_params_(**params)

        dict_dffs_ = self.extract_dataframes_(**params)

        self.transform_and_load_(
            self.load_function_,
            dict_dffs_
        )
        return self