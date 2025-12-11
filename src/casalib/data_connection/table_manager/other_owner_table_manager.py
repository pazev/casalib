""" Module adds a simple TableManager, to manage some
    operations as drop, drop_partition, list_partition.
"""
from typing import Any, List, Tuple

from .base import TableManagerAbstract


class OtherOwnerTableManager(TableManagerAbstract):
    """ TableManager to deal with table and partition
        operations as list_partitions, sample, metadata,
        last_partition_query.

        It doesn't implement the drop, drop_partitions and
        drop_partitions_filter.
    """
    # Deactivating dropping methods
    def drop(self) -> "OtherOwnerTableManager":
        """ Drop the table """
        raise NotImplementedError(
            "You cannot drop a table you don't own."
        )

    def drop_partitions(
            self,
            partitions_to_drop: List[Tuple[str, ...]]
        ) -> "OtherOwnerTableManager":
        """ Drop partitions """
        raise NotImplementedError(
            "You cannot drop partitions from a table you "
            "don't own."
        )

    def drop_partitions_filter(
        self,
        *filters: str
    ) -> "OtherOwnerTableManager":
        """
        Drop partitions using the fnmatch filter passed.
        """
        raise NotImplementedError(
            "You cannot drop partitions from a table you "
            "don't own."
        )

    def drop_partitions_filter_pd(
        self,
        *filters: str
    ) -> "OtherOwnerTableManager":
        """
        Drop partitions using the fnmatch filter passed.
        """
        raise NotImplementedError(
            "You cannot drop partitions from a table you "
            "don't own."
        )

    # Implementation of run: do nothing
    def run(self, **kwargs: Any) -> "OtherOwnerTableManager":
        """ Run the procedure that generate the table """
        return self

    def get_table_input(self) -> List[str]:
        """ Return the input tables """
        return []

    def input_vars(self) -> List[str]:
        """
        List the variables necessary to run the TableManager
        """
        return []
