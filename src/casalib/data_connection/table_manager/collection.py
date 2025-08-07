"""
Module defines TableManagerCollection, to manage several
TableManager at once
"""
from dataclasses import dataclass, field
from typing import Any, Dict, List

from .base import TableManagerAbstract


@dataclass
class TableManagerCollection:
    """
    Collection to manage the table manager collection
    """
    collection: Dict[
        str, TableManagerAbstract
    ] = field(init=False, repr=False, default_factory=dict)
    renaming_dict: Dict[
        Any, TableManagerAbstract
    ] = field(init=False, repr=False, default_factory=dict)

    def __getitem__(self, key: Any) -> TableManagerAbstract:
        """ Returns the table manager """
        return self.renaming_dict[key]

    def add_table_manager(
        self,
        tm: TableManagerAbstract,
        *other_names: Any
    ) -> "TableManagerCollection":
        """ Add a TableManager to the Collection """
        table_name = tm.get_table_name()
        all_names = [table_name, *other_names]

        shock_names = (
            set(all_names) & set(self.renaming_dict)
        )

        if shock_names:
            raise ValueError(
                f"The names {shock_names} is/are already "
                "in the collection; please check. ({tm})"
            )

        self.collection[table_name] = tm

        for name in [table_name, *other_names]:
            self.renaming_dict[table_name] = tm
            self.renaming_dict[name] = tm

        return self

    def input_vars(self) -> List[str]:
        """ Get input vars for each TableManager """
        vars_ = list(
            set(
                var_
                for tm in self.collection.values()
                for var_ in tm.input_vars()
            )
        )

        return vars_
