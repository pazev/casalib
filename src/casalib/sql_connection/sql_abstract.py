'''
SqlDialectAbstract — base class for SQL dialect implementations.

Each method transforms `input_query` into a new SQL string and
returns it wrapped in a `Query` object.
'''
from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import TYPE_CHECKING, Dict, List, Optional, Tuple, Union

if TYPE_CHECKING:
    from .query import Query


@dataclass
class SqlDialectAbstract(ABC):
    input_query: str

    @abstractmethod
    def select(self) -> Query:
        '''
        SELECT * FROM the input query.
        '''

    @abstractmethod
    def agg(
        self,
        query: str,
        groupby: Optional[List[str]] = None,
        count_: Optional[List[str]] = None,
        count_null_: Optional[List[str]] = None,
        count_distinct_: Optional[List[str]] = None,
        sum_: Optional[List[str]] = None,
        mean_: Optional[List[str]] = None,
        min_: Optional[List[str]] = None,
        max_: Optional[List[str]] = None,
        percentile_: Optional[Dict[int, List[str]]] = None,
        percentile_ignore_values_: Optional[Dict[str, List[float]]] = None,
        cols_before: Optional[List[Union[str, Tuple[str, str]]]] = None,
        cols_after: Optional[List[Union[str, Tuple[str, str]]]] = None,
    ) -> Query:
        '''
        Aggregate `query`.

        `groupby`                     — columns to GROUP BY.
        `count_`                      — columns to COUNT.
        `count_null_`                 — columns to COUNT considering nulls.
        `count_distinct_`             — columns to COUNT DISTINCT.
        `sum_`                        — columns to SUM.
        `mean_`                       — columns to AVG.
        `min_`                        — columns to MIN.
        `max_`                        — columns to MAX.
        `percentile_`                 — mapping of percentile (0–100) → columns.
        `percentile_ignore_values_`   — mapping of column → list of values to
                                        exclude before computing percentile.
        `cols_before`                 — extra expressions prepended to SELECT,
                                        either a column name or (expression, alias).
        `cols_after`                  — extra expressions appended to SELECT,
                                        either a column name or (expression, alias).
        '''

    @abstractmethod
    def get_duplicates(self, keys: List[str]) -> Query:
        '''
        Return all rows whose combination of `keys` appears more than once.
        '''

    @abstractmethod
    def jsonify(self, keys: List[str], columns: List[str]) -> Query:
        '''
        Keep `keys` as regular columns and collapse `columns` into a
        single JSON value. Syntax varies by dialect — implement in subclass.
        '''

    @abstractmethod
    def sample(self, num_samples: int) -> Query:
        '''
        Return a sample of `num_samples` rows from the input query.
        '''
