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
    def select(self, table_name: str) -> Query:
        '''
        SELECT * FROM `table_name`.
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

        Generated column names follow the pattern `col__aggregation`
        (e.g. `amount__sum`, `id__count_distinct`).

        `groupby`                     — columns to GROUP BY.
        `count_`                      — columns to COUNT.
        `count_null_`                 — columns to COUNT including nulls.
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

    @abstractmethod
    def last_partitions(
        self,
        date_ingestion: str,
        columns: List[str],
    ) -> Query:
        '''
        For each combination of `columns`, select only the rows from the
        most recent `date_ingestion` value (i.e. the last partition).

        `date_ingestion` — name of the date/timestamp column that identifies
                           the ingestion partition.
        `columns`        — columns that define the partition key.
        '''

    @abstractmethod
    def enrich(
        self,
        other: Union[str, List[str]],
        keys: List[Union[str, Tuple[str, str]]],
        prefix: Optional[Union[str, List[str]]] = None,
    ) -> Query:
        '''
        LEFT JOIN `input_query` with one or more queries, bringing all
        columns from the joined side(s).

        `other`   — a single query string or a list of query strings to join.
        `keys`    — join condition. Each element is either a column name
                    (same in both sides) or a (left_col, right_col) tuple.
        `prefix`  — optional prefix for columns coming from the joined side(s).
                    If `other` is a list, a list of prefixes can be passed.
        '''

    @abstractmethod
    def get_diffs(
        self,
        other: str,
        keys: List[Union[str, Tuple[str, str]]],
        columns: List[Union[str, Tuple[str, str]]],
    ) -> Query:
        '''
        Compare `columns` between `input_query` and `other`.

        `other`   — the second query to compare against.
        `keys`    — join keys. Each element is a column name (same in both
                    queries) or a (left_col, right_col) tuple.
        `columns` — columns to compare. Each element is a column name (same
                    in both queries) or a (left_col, right_col) tuple.
        '''

    @abstractmethod
    def op(
        self,
        add: Optional[Dict[str, str]] = None,
        rename: Optional[Dict[str, str]] = None,
        select_only: Optional[List[str]] = None,
        exclude: Optional[List[str]] = None,
    ) -> Query:
        '''
        Operate on the columns of `input_query`.

        `add`         — {new_col: sql_expression} columns to add.
        `rename`      — {new_name: old_name} columns to rename.
        `select_only` — keep only these columns (applied after add/rename).
        `exclude`     — drop these columns (applied after add/rename).
        '''

    @abstractmethod
    def has_fullname(self, table_name: str) -> bool:
        '''
        Return True if `table_name` contains all the information needed
        to fully identify the table (e.g. schema and table name).
        '''

    @abstractmethod
    def split_tablename(self, table_name: str) -> Tuple[str, str]:
        '''
        Split `table_name` into a (schema, table) tuple.
        '''
