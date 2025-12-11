"""
Base module, with the definition of NamesManager and
NamesManagerFunction.
"""
from dataclasses import dataclass, field
from pathlib import Path
from typing import (
    Any, ClassVar, Dict, Optional, Protocol, Union,
)


class NamesManagerFunction(Protocol):
    '''
    Class to define the prototype of functions to be used
    to create new functionalities.
    '''
    # pylint: disable=too-few-public-methods
    def __call__(self, tb: "NamesManager", tab: str):
        pass


@dataclass
class NamesManager:
    """ Names Manager """
    dict_names: Dict[str, str] = field(default_factory=dict)
    name: str = 'NamesManager'
    file: Optional[Union[str, Path]] = None
    dict_functions_: ClassVar[Dict[str, NamesManagerFunction]] = {}

    def __post_init__(self):
        """ Post-init """
        self.dict_functions_ = self.dict_functions_ or {}
        self.file = Path(self.file) if self.file else None

    @classmethod
    def register_func(cls, func_name: str, func: NamesManagerFunction):
        """ Register the function """
        if cls.dict_functions_ is None:
            cls.dict_functions_ = {}

        cls.dict_functions_[func_name] = func

    def __repr__(self):
        """ Representation """
        return (
            self.name +
            '(' +
            str(sorted(list(self.dict_names))) +
            ')'
        )

    def __iter__(self):
        """ Iter """
        return iter(self.dict_names)

    def keys(self):
        """ Return the known keys """
        return self.dict_names.keys()

    def __setitem__(self, key: str, val: Any):
        """ Set a new property """
        if self.file:
            self.load(self.file)

        if key not in self.dict_names:
            self.dict_names[key] = val

        if val != self.dict_names[key]:
            raise ValueError(
                f'The key `{key}` is already set with value '
                f'`{repr(self.dict_names[key])}`. You cannot '
                f'set a new value `{repr(val)}`'
            )

        if self.file:
            self.save(self.file)

    def __getitem__(self, key: str) -> str:
        """ Get the info """
        return self.get_info_(key)

    def __getattr__(self, key: str):
        """
        If an attribute is requested but not found,
        check if the object dictionary of known tables
        contains the key
        """
        return self.get_info_(key)

    def get_info_(self, key: str):
        """ Return the tables in tb """
        if self.file:
            self.load(self.file)

        if key in self.dict_names:
            return self.dict_names[key]

        # If key not known, but starts with select___, check
        # if the following table is in the dict
        res: Union[str, None] = None
        args = key.split('___')

        dict_functions = self.dict_functions_ or {}

        if len(args) > 1:
            func_name, *vars_parts = args
            func = dict_functions.get(func_name)
            var = '___'.join(vars_parts)

            if func:
                res = func(self, var)

        if res is not None:
            return res

        raise ValueError(
            f'Key not found: {key}. '
            f'Known keys: {list(self.dict_names)}. '
            f'Known functions: {list(self.full)}'
        )

    @property
    def full(self) -> Dict[str, str]:
        """
        Return a dict with all combinations of tab and
        selected_
        """
        dict_functions = self.dict_functions_ or {}

        return (
            self.dict_names
            |
            {
                f'{func_name}___{key}': func(self, key)
                for func_name in sorted(dict_functions)
                for func in [dict_functions[func_name]]
                for key in sorted(self.dict_names)
            }
        )

    def save(self, filename: Union[str, Path]):
        """
        Save the known tables to a file.
        """
        import yaml
        with open(filename, 'w', encoding='utf-8') as f:
            yaml.safe_dump(self.dict_names, f)
        return self

    def load(self, filename: Union[str, Path]) -> "NamesManager":
        """
        Load a file with the list of known tables.
        """
        import yaml

        if not Path(filename).exists():
            return self

        with open(filename, 'r', encoding='utf-8') as f:
            self.dict_names = yaml.safe_load(f)
        return self


def select_query_(tb: NamesManager, tab: str) -> Union[str, None]:
    """ Return a select query """
    if tab in tb.dict_names:
        tab_final = tb.dict_names[tab]
        return f'''select * from {tab_final}'''
    return None


def sample_query_(tb: NamesManager, tab: str) -> Union[str, None]:
    """ Return a select query """
    import textwrap

    if tab in tb.dict_names:
        tab_final = tb.dict_names[tab]
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
) -> NamesManagerFunction:
    """
    Return a function to select the maximum
    partition.
    """
    def select_gen(
        tb: NamesManager, tab: str
    ) -> Union[str, None]:
        """ Return a select query """
        import textwrap

        if tab in tb.dict_names:
            tab_final = tb.dict_names[tab]
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
