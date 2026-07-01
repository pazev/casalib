"""PrestoDialect — Dialect for Presto/Trino SQL."""
from dataclasses import dataclass
from typing import ClassVar

from ..dialect import Dialect
from .dialect_def import PrestoDialectDef


@dataclass
class PrestoDialect(Dialect):
    """
    Dialect for Presto/Trino SQL. Rendering delegated to
    PrestoDialectDef.
    """

    _dialect_def: ClassVar[PrestoDialectDef] = (
        PrestoDialectDef()
    )
