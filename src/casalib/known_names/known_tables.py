'''
Module defines names for tables.
'''
from typing import Callable, ClassVar, Optional

from ._base import NamesManager


class KnownTables(NamesManager):
    '''
    Names Manager for tables. Add some functions to generate
    some queries.
    '''
    name: ClassVar[str] = 'KnownTables'


def select_query_(tb: KnownTables, tab: str) -> Optional[str]:
    """ Return a select query """
    if not tab in tb:
        return None

    tab_final = tb[tab]
    return f'''select * from {tab_final}'''


def sample_query_(tb: KnownTables, tab: str) -> Optional[str]:
    """ Return a select query """
    import textwrap

    if not tab in tb:
        return None

    tab_final = tb[tab]
    return textwrap.dedent(f'''
    with
    input_ as (
        select * from {tab_final}
        limit 100
    )
    select * from input_
    ''')


def select_max_col_(
    col: str
) -> Callable[[KnownTables, str], Optional[str]]:
    """
    Return a function to select the maximum
    partition.
    """
    def select_gen(
        tb: KnownTables, tab: str
    ) -> Optional[str]:
        """ Return a select query """
        import textwrap

        if not tab in tb:
            return None

        tab_final = tb[tab]
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

    return select_gen


KnownTables.register_func('select', select_query_)  # type: ignore
KnownTables.register_func('sample', sample_query_)  # type: ignore
KnownTables.register_func('select_max', select_max_col_('dt_ingestao'))  # type: ignore
