from dataclasses import dataclass, field
from typing import Callable, Dict,  List, Optional, Tuple

import pandas as pd

from . base import TableHelper
from ..base import ConnectionAbstract, Metadata


@dataclass
class AnyTableManager:
    table_name: str
    conn_maker: Optional[
        Callable[[], ConnectionAbstract]
    ] = None
    helper: TableHelper = field(init=False)

    def __post_init__(self):
        self.helper = TableHelper(
            table_name=self.table_name,
            conn_maker=self.conn_maker
        )

    def set_conn_maker(
        self,
        conn_maker: Callable[[], ConnectionAbstract]
    ) -> "AnyTableManager":
        """ Set the connection maker """
        self.helper.set_conn_maker(conn_maker)
        return self

    def drop(self) -> "AnyTableManager":
        """ Drop the table """
        self.helper.drop()
        return self

    def drop_partitions(
        self,
        partitions_to_drop: List[Tuple[str, ...]]
    ) -> "AnyTableManager":
        """ Drop partitions """
        self.helper.drop_partitions(partitions_to_drop)
        return self

    def list_partitions(self) -> Dict[Tuple[str, ...], str]:
        """ List partitions """
        return self.helper.list_partitions(self.table_name)

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

    def drop_partitions_filter(
        self,
        *filters: str
    ) -> "AnyTableManager":
        """
        Drop partitions using the fnmatch filter passed.
        """
        self.helper.drop_partitions_filter(*filters)
        return self

    def sample(self, samples: int = 100) -> pd.DataFrame:
        """
        Select some sample from the table.
        """
        return self.helper.sample(samples)

    def metadata(self) -> Metadata:
        """ Returns the table metadata """
        return self.helper.metadata()
