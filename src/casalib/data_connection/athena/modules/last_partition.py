"""
Module with the functionality to get a query to obtain the
last partition for a table
"""
# pylint: disable=too-many-arguments
from typing import Any, List, Optional

from ..templates import templates_dict


def last_partition(
    table_name: str,
    cross_columns_: List[str],
    max_column_: Optional[str] = None,
    **filters: List[Any],
):
    """ Return the last partition for a table """
    if set([max_column_]) - set(cross_columns_):
        raise ValueError(
            'max_column must be in cross_columns'
        )

    groupby_cols = [
        col for col in cross_columns_ if col != max_column_
    ]

    filters_final = {
        col: [repr(val) for val in vals]
        for col, vals in filters.items()
    }

    template_ = templates_dict['last_partition']

    return template_.render(
        table_name=table_name,
        groupby_cols=groupby_cols,
        max_column=max_column_,
        filters=filters_final,
    )
