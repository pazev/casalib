'''
SqlDialectAbstract — base class for SQL dialect implementations.

Each method transforms `input_query` into a new SQL string and
returns it wrapped in a `Query` object.
'''
from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import TYPE_CHECKING, Dict, List

if TYPE_CHECKING:
    from .query import Query


@dataclass
class SqlDialectAbstract(ABC):
    input_query: str

    def _query(self, sql: str) -> Query:
        from .query import Query
        return Query(sql, self)

    def select(self) -> Query:
        '''
        SELECT * FROM the input query.
        '''
        return self._query(f"SELECT * FROM ({self.input_query}) t")

    def agg(self, group_cols: List[str], agg_cols: Dict[str, str]) -> Query:
        '''
        Aggregate the input query.

        `group_cols` — columns to GROUP BY.
        `agg_cols`   — mapping of output alias → aggregation expression,
                       e.g. {"total": "SUM(amount)", "n": "COUNT(*)"}.
        '''
        group = ", ".join(group_cols)
        aggs = ", ".join(f"{expr} AS {alias}" for alias, expr in agg_cols.items())
        select_cols = f"{group}, {aggs}" if group_cols else aggs
        group_clause = f" GROUP BY {group}" if group_cols else ""
        sql = f"SELECT {select_cols} FROM ({self.input_query}) t{group_clause}"
        return self._query(sql)

    def get_duplicates(self, keys: List[str]) -> Query:
        '''
        Return all rows whose combination of `keys` appears more than once.
        '''
        partition = ", ".join(keys)
        sql = (
            f"SELECT * FROM ("
            f"SELECT *, COUNT(*) OVER (PARTITION BY {partition}) AS _dup_count "
            f"FROM ({self.input_query}) t"
            f") t WHERE _dup_count > 1"
        )
        return self._query(sql)

    @abstractmethod
    def jsonify(self, keys: List[str], columns: List[str]) -> Query:
        '''
        Keep `keys` as regular columns and collapse `columns` into a
        single JSON value. Syntax varies by dialect — implement in subclass.
        '''

    def sample(self, num_samples: int) -> Query:
        '''
        Return the first `num_samples` rows from the input query.
        '''
        return self._query(f"SELECT * FROM ({self.input_query}) t LIMIT {num_samples}")
