"""AnsiDialect — Dialect for ANSI SQL."""
from dataclasses import dataclass

from ..dialect import Dialect
from .dialect_def import AnsiDialectDef


@dataclass
class AnsiDialect(Dialect):
    """
    Dialect for ANSI SQL. Rendering delegated to
    AnsiDialectDef.
    """

    _dialect_def = AnsiDialectDef()
