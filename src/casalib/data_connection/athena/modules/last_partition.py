"""
Module with the functionality to get a query to obtain the
last partition for a table
"""
# pylint: disable=too-many-arguments
from typing import Any, List, Optional

import jinja2

from ..templates import templates_dict


def last_partition(
    table: str,
    cross_columns: List[str],
    max_column: Optional[str] = None,
    **filters: List[Any],
):
    """ Return the last partition for a table """
    if set([max_column]) - set(cross_columns):
        raise ValueError(
            'max_column must be in cross_columns'
        )

    groupby_cols = [
        col for col in cross_columns if col != max_column
    ]

    filters_final = {
        col: [repr(val) for val in vals]
        for col, vals in filters.items()
    }

    env = jinja2.Environment()
    template_ = env.from_string(
        templates_dict['last_partition']
    )

    return template_.render(
        table=table,
        groupby_cols = groupby_cols,
        max_column=max_column,
        filters=filters_final,
    )
