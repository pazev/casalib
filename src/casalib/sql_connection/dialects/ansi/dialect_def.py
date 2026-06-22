"""AnsiDialectDef — DialectDefinition for ANSI SQL."""
from typing import Tuple

from .._dialect_def import DialectDefinition
from .._helpers import AggCol


class AnsiDialectDef(DialectDefinition):
    """ANSI dialect rendering primitives.

    Uses ``approx_percentile`` for PERCENTILE, which is
    Presto/Trino/Athena-compatible. Override ``render_percentile``
    in a subclass for engines with different syntax (e.g. Spark).
    """

    def render_percentile(self, agg_col: AggCol) -> Tuple[str, str]:
        cfg = agg_col.percentile_config
        assert cfg is not None
        col = agg_col.col
        pct = cfg.percentile / 100.0
        if cfg.ignore_values:
            vals = ", ".join(str(v) for v in cfg.ignore_values)
            inner = (
                f"CASE WHEN {col} NOT IN ({vals})"
                f" THEN {col} ELSE NULL END"
            )
        else:
            inner = col
        return (
            f"approx_percentile({inner}, {pct})",
            f"{col}__p{cfg.percentile}",
        )
