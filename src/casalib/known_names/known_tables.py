from dataclasses import dataclass
from typing import Callable, Union

from ._base import NamesManager


@dataclass
class KnownTables(NamesManager):
    '''
    Names Manager for tables. Add some functions to generate
    some queries.
    '''
    name: str = 'KnownTables'



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