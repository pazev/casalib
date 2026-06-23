"""SQL dialect implementations."""
from dataclasses import dataclass
from typing import ClassVar

from .dialect import Dialect
from .ansi.dialect_def import AnsiDialectDef
from .presto.dialect_def import PrestoDialectDef


@dataclass
class AnsiDialect(Dialect):
    _dialect_def: ClassVar[AnsiDialectDef] = (
        AnsiDialectDef()
    )


@dataclass
class PrestoDialect(Dialect):
    _dialect_def: ClassVar[PrestoDialectDef] = (  # type: ignore[assignment]
        PrestoDialectDef()
    )


__all__ = ["Dialect", "AnsiDialect", "PrestoDialect"]
