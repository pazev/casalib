"""PrestoDialectDef — DialectDefinition for Presto/Trino."""
from ..ansi.dialect_def import AnsiDialectDef


class PrestoDialectDef(AnsiDialectDef):
    """Presto/Trino dialect rendering primitives.

    Inherits all rendering from AnsiDialectDef.
    Presto-specific SQL (SELECT * EXCEPT, jsonify) is handled
    in PrestoDialect at the method level, not here.
    """
