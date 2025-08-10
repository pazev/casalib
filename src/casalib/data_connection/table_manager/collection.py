"""
Module defines TableManagerCollection, to manage several
TableManager at once
"""
from dataclasses import dataclass, field
from typing import Any, Callable, Dict, List, Tuple

from casalib.algorithms.graph.kahn_toposort import kahn_toposort

from ..base import ConnectionAbstract
from .base import TableManagerAbstract


@dataclass
class TableManagerCollection:
    """
    Collection to manage the table manager collection.

    Provides the KN (known names) variable to the run
    functions.
    """
    tm_collection: Dict[
        str, TableManagerAbstract
    ] = field(init=False, repr=False, default_factory=dict)
    known_names: Dict[
        Any, TableManagerAbstract
    ] = field(init=False, repr=False, default_factory=dict)

    def __getitem__(self, key: Any) -> Any:
        """ Returns the table manager """
        return self.known_names[key]

    def __setitem__(self, key: Any, val: Any) -> None:
        """ Set a known name """
        if key in self.known_names:
            raise ValueError(
                f" The key `{key}` already exists in "
                "dictionary. Please check."""
            )

        self.known_names[key] = val

    def add_known_name(
        self, key: Any, val: Any
    ) -> "TableManagerCollection":
        """ Add a known name, to be used in the collection
        """
        self[key] = val
        return self

    def as_list(self) -> List[TableManagerAbstract]:
        """ Get all TableManagers in a list """
        return list(self.tm_collection.values())

    def add_table_manager(
        self,
        tm: TableManagerAbstract,
        *other_names: Any
    ) -> "TableManagerCollection":
        """ Add a TableManager to the Collection """
        table_name = tm.get_table_name()
        all_names = [table_name, *other_names]

        shock_names = (
            set(all_names) & set(self.known_names)
        )

        if shock_names:
            raise ValueError(
                f"The names {shock_names} is/are already "
                "in the collection; please check. ({tm})"
            )

        self.tm_collection[table_name] = tm

        for name in [table_name, *other_names]:
            self[table_name] = tm
            self[name] = tm

        self.sort_()
        return self

    def set_conn_maker(
        self, conn_maker: Callable[[], ConnectionAbstract]
    ) -> "TableManagerCollection":
        """
        Set connection maker to all TableManagers in the
        collection.
        """
        for tm in self.tm_collection.values():
            tm.set_conn_maker(conn_maker)

        return self

    def input_vars(self) -> List[str]:
        """ Get input vars for each TableManager """
        vars_ = set(
            var_
            for tm in self.tm_collection.values()
            for var_ in tm.input_vars()
        )

        return list(vars_ - set(['KN']))

    def get_dependency_edges_(self) -> List[Tuple[str, str]]:
        """ Return the dependency edge """
        edges_set_ = set(
            (input_table, tm_name)

            for tm in self.tm_collection.values()
            for tm_name in [tm.get_table_name()]
            for input_table in tm.get_table_input()
        )

        return list(edges_set_)

    def sort_(self) -> "TableManagerCollection":
        """
        Sort the TableManagers in the collection.
        """
        edges = self.get_dependency_edges_()
        names = list(self.tm_collection)

        ordered = kahn_toposort(
            edges=edges,
            nodes=names
        )

        final_tm_col = {
            tm_name: self[tm_name]
            for tm_name in ordered
            if tm_name in names
        }

        self.tm_collection = final_tm_col

        return self

    def run(self, **kwargs) -> "TableManagerCollection":
        """
        Run the TableManagerCollection (all the
        TableManagers inside it).
        """
        for tm in self.tm_collection.values():
            tm.run(KN=self.known_names, **kwargs)

        return self
