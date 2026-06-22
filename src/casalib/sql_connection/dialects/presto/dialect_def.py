"""PrestoDialectDef — DialectDefinition for Presto/Trino."""
from pathlib import Path
from typing import List

from ..ansi.dialect_def import AnsiDialectDef

_TEMPLATE_FLD = Path(__file__).parent / "templates"


class PrestoDialectDef(AnsiDialectDef):
    """Presto/Trino dialect rendering.

    Inherits all rendering from AnsiDialectDef.
    Overrides render_jsonify to use map_from_arrays
    with ARRAY literals.
    """

    def render_jsonify(
        self,
        input_query: str,
        keys: List[str],
        columns: List[str],
    ) -> str:
        return self._load_template(
            _TEMPLATE_FLD / "jsonify.sql"
        ).render(
            input_query=input_query,
            keys=keys,
            columns=columns,
        )
