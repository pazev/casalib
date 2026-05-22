"""Shared helpers for SQL dialect implementations."""
from typing import (
    List,
    Sequence,
    Tuple,
    Union,
)


def normalise_keys(
    keys: Sequence[Union[str, Tuple[str, str]]],
) -> List[Tuple[str, str]]:
    """Normalise key list to (left, right) pairs.

    Args:
        keys: Mix of bare column names and
            (left_col, right_col) tuples.

    Returns:
        List of (left_col, right_col) pairs.
    """
    def _norm(
        k: Union[str, Tuple[str, str]]
    ) -> Tuple[str, str]:
        if isinstance(k, tuple):
            return k
        return (k, k)

    return [_norm(k) for k in keys]
