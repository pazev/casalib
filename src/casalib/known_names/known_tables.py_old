from dataclasses import dataclass, field
from pathlib import Path
from pprint import pformat
from typing import (
    Any, Callable, ClassVar, Dict, List, Union,
)


@dataclass
class KnownTables:
    dict_tables: Dict[str, str] = field(default_factory=dict)
    dict_functions_: ClassVar[
        Union[
            None,
            Dict[
                str,
                Callable[[str], Union[str, None]]
            ]
        ]
    ] = None

    def __post_init__(self):
        """ Post-init """
        self.dict_functions_ = self.dict_functions_ or {}

    @classmethod
    def register_func(
        cls,
        func_name: str,
        func: Callable[["KnownTables", str], Union[str, None]]
    ):
        """ Register the function """
        if cls.dict_functions_ is None:
            cls.dict_functions_ = {}

        cls.dict_functions_[func_name] = func

    def __repr__(self):
        """ Representation """
        return (
            'KnownTables(' +
            str(list(self.dict_tables)) +
            ')'
        )

    def __iter__(self):
        """ Iter """
        return iter(self.dict_tables)

    def keys(self):
        """ Return the known keys """
        return self.dict_tables.keys()

    def __setitem__(self, key: str, val: Any):
        """ Set a new property """
        if key not in self.dict_tables:
            self.dict_tables[key] = val

        if val == self.dict_tables[key]:
            return

        raise ValueError(
            f'The key `{key}` is already set with value '
            f'`{repr(self.dict_tables[key])}`. You cannot '
            f'set a new value `{repr(val)}`'
        )

    def __getitem__(self, key: Union[str, List[str]]):
        """ Get the info """
        if isinstance(key, str):
            return self.get_info_(key)

        if isinstance(key, list):
            return [self.get_info_(k) for k in key]

    def __getattr__(self, key: str):
        """
        If an attribute is requested but not found,
        check if the object dictionary of known tables
        contains the key
        """
        return self.get_info_(key)

    def get_info_(self, key: str):
        """ Return the tables in tb """
        if key in self.dict_tables:
            return self.dict_tables[key]

        # If key not known, but starts with select___, check
        # if the following table is in the dict
        res: Union[str, None] = None
        args = key.split('___')

        if len(args) > 1:
            func_name, *vars_parts = args
            func = self.dict_functions_.get(func_name)
            var = '___'.join(vars_parts)

            if func:
                res = func(self, var)

        if res is not None:
            return res

        raise ValueError(
            f'Key not found: {key}. Known keys: '
            f'{list(self.dict_tables)}'
        )

    @property
    def full(self) -> Dict[str, str]:
        """
        Return a dict with all combinations of tab and
        selected_
        """
        return self.dict_tables | {
            f'{func_name}___{key}': func(self, key)
            for func_name, func in self.dict_functions_.items()
            for key, _ in self.dict_tables.items()
        }

    def save(self, filename: Union[str, Path]):
        """
        Save the known tables to a file.
        """
        import yaml
        with open(filename, 'w') as f:
            yaml.safe_dump(self.dict_tables, f)
        return self

    def load(self, filename: Union[str, Path]) -> "KnownTables":
        """
        Load a file with the list of known tables.
        """
        import yaml
        with open(filename, 'r') as f:
            self.dict_tables = yaml.safe_load(f)
        return self


def select_query_(tb: KnownTables, tab: str) -> Union[str, None]:
    """ Return a select query """
    if tab in tb.dict_tables:
        tab_final = tb.dict_tables[tab]
        return f'''select * from {tab_final}'''
    return None


def sample_query_(tb: KnownTables, tab: str) -> Union[str, None]:
    """ Return a select query """
    import textwrap

    if tab in tb.dict_tables:
        tab_final = tb.dict_tables[tab]
        return textwrap.dedent(f'''
        with
        input_ as (
            select * from {tab_final}
            limit 100
        )
        select * from input_
        ''')
    return None


def select_max_col_(
    col: str
) -> Callable[[KnownTables, str], Union[str, None]]:
    """
    Return a function to select the maximum
    partition.
    """
    def select_gen(
        tb: KnownTables, tab: str
    ) -> Union[str, None]:
        """ Return a select query """
        import textwrap

        if tab in tb.dict_tables:
            tab_final = tb.dict_tables[tab]
            return textwrap.dedent(f'''
            with
            input_ as (
                select * from {tab_final}
            )
            ,
            part_ as (
                select * from input_
                where
                    {col} =
                    (select max({col}) from input_)
            )
            select * from part_
            ''')
        return None
    return select_gen


KnownTables.register_func('select', select_query_)
KnownTables.register_func('sample', sample_query_)
KnownTables.register_func('select_max', select_max_col_('dt_ingestao'))