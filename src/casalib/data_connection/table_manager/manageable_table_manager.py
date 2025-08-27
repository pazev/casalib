""" Module adds a simple TableManager, to manage some
    operations as drop, drop_partition, list_partition.
"""
from typing import List
from .base import TableManagerAbstract


class ManageableTableManager(TableManagerAbstract):
    """ TableManager to deal with table and partition
        operations as list_partitions, sample, metadata,
        last_partition_query.

        It allows to drop, drop_partitions and
        drop_partitions_filter, but has no requirements
        about how to run the table.
    """
    # Implementation of run: do nothing
    def run(self, **kwargs) -> "ManageableTableManager":
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
