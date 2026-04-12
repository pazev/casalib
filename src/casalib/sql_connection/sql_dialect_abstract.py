"""SqlDialectAbstract — base class for SQL
dialect implementations.

Each method transforms ``input_query`` into
a new SQL string and returns it.
"""
from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import (
    Dict,
    List,
    Optional,
    Protocol,
    Tuple,
    TypeVar,
    Union,
)

T_co = TypeVar('T_co', covariant=True)


class SqlDialectProtocol(Protocol[T_co]):
    """Structural interface shared by dialect
    instances and _QueryBuilder.

    Transformation methods return T_co:
    - str for concrete dialect implementations
    - Query for _QueryBuilder
    """

    # pylint: disable=missing-function-docstring

    def select(
        self, table_name: str
    ) -> T_co: ...

    def agg(  # pylint: disable=too-many-arguments
        self,
        groupby: Optional[List[str]] = None,
        *,
        count_: Optional[List[str]] = None,
        count_null_: Optional[List[str]] = None,
        count_distinct_: Optional[
            List[str]
        ] = None,
        sum_: Optional[List[str]] = None,
        mean_: Optional[List[str]] = None,
        min_: Optional[List[str]] = None,
        max_: Optional[List[str]] = None,
        percentile_: Optional[
            Dict[int, List[str]]
        ] = None,
        percentile_ignore_values_: Optional[
            Dict[str, List[float]]
        ] = None,
        cols_before: Optional[
            List[Union[str, Tuple[str, str]]]
        ] = None,
        cols_after: Optional[
            List[Union[str, Tuple[str, str]]]
        ] = None,
    ) -> T_co: ...

    def get_duplicates(
        self, keys: List[str]
    ) -> T_co: ...

    def jsonify(
        self,
        keys: List[str],
        columns: List[str],
    ) -> T_co: ...

    def sample(
        self, num_samples: int
    ) -> T_co: ...

    def last_partitions(
        self,
        date_ingestion: str,
        columns: List[str],
    ) -> T_co: ...

    def enrich(
        self,
        other: Union[str, List[str]],
        keys: List[
            Union[str, Tuple[str, str]]
        ],
        prefix: Optional[
            Union[str, List[str]]
        ] = None,
    ) -> T_co: ...

    def get_diffs(
        self,
        other: str,
        keys: List[
            Union[str, Tuple[str, str]]
        ],
        columns: List[
            Union[str, Tuple[str, str]]
        ],
    ) -> T_co: ...

    def op(
        self,
        add: Optional[Dict[str, str]] = None,
        rename: Optional[
            Dict[str, str]
        ] = None,
        select_only: Optional[
            List[str]
        ] = None,
        exclude: Optional[List[str]] = None,
    ) -> T_co: ...


@dataclass
class SqlDialectAbstract(ABC):
    """Base class for SQL dialect implementations.

    Attributes:
        input_query: The SQL string that serves
            as input for transformation methods.
    """

    input_query: str

    @abstractmethod
    def select(self, table_name: str) -> str:
        """Generate a SELECT * query for a table.

        Args:
            table_name: Fully qualified table
                name.

        Returns:
            A ``SELECT * FROM <table_name>``
            SQL string.
        """

    @abstractmethod
    def agg(  # pylint: disable=too-many-arguments
        self,
        groupby: Optional[List[str]] = None,
        *,
        count_: Optional[List[str]] = None,
        count_null_: Optional[List[str]] = None,
        count_distinct_: Optional[List[str]] = None,
        sum_: Optional[List[str]] = None,
        mean_: Optional[List[str]] = None,
        min_: Optional[List[str]] = None,
        max_: Optional[List[str]] = None,
        percentile_: Optional[
            Dict[int, List[str]]
        ] = None,
        percentile_ignore_values_: Optional[
            Dict[str, List[float]]
        ] = None,
        cols_before: Optional[
            List[Union[str, Tuple[str, str]]]
        ] = None,
        cols_after: Optional[
            List[Union[str, Tuple[str, str]]]
        ] = None,
    ) -> str:
        """Generate an aggregation query.

        Generated column names follow the
        ``col__aggregation`` convention, e.g.
        ``amount__sum``, ``id__count_distinct``.

        Args:
            groupby: Columns to GROUP BY.
            count_: Columns to COUNT.
            count_null_: Columns to COUNT
                including nulls.
            count_distinct_: Columns to COUNT
                DISTINCT.
            sum_: Columns to SUM.
            mean_: Columns to AVG.
            min_: Columns to MIN.
            max_: Columns to MAX.
            percentile_: Mapping of percentile
                value (0–100) to columns.
            percentile_ignore_values_: Mapping
                of column name to a list of
                values to exclude before
                computing the percentile.
            cols_before: Extra expressions
                prepended to SELECT. Each element
                is either a column name or an
                ``(expression, alias)`` tuple.
            cols_after: Extra expressions
                appended to SELECT. Each element
                is either a column name or an
                ``(expression, alias)`` tuple.

        Returns:
            The aggregation SQL string.
        """

    @abstractmethod
    def get_duplicates(
        self, keys: List[str]
    ) -> str:
        """Return all rows whose key combination
        appears more than once.

        Args:
            keys: Columns that form the
                duplicate key.

        Returns:
            SQL string whose result contains
            only the duplicated rows.
        """

    @abstractmethod
    def jsonify(
        self, keys: List[str], columns: List[str]
    ) -> str:
        """Collapse columns into a single JSON
        value, keeping keys intact.

        Syntax varies by SQL dialect and must be
        implemented in subclasses.

        Args:
            keys: Columns to keep as regular
                output columns.
            columns: Columns to fold into a
                JSON value.

        Returns:
            SQL string with ``keys`` as columns
            and a JSON column for the rest.
        """

    @abstractmethod
    def sample(self, num_samples: int) -> str:
        """Return a sample of rows from the
        input query.

        Args:
            num_samples: Number of rows to
                return.

        Returns:
            SQL string limited to
            ``num_samples`` rows.
        """

    @abstractmethod
    def last_partitions(
        self,
        date_ingestion: str,
        columns: List[str],
    ) -> str:
        """Select only the most recent partition
        rows for each key group.

        For each combination of ``columns``,
        keep only the rows where
        ``date_ingestion`` is at its maximum
        value.

        Args:
            date_ingestion: Name of the
                date/timestamp column that
                identifies the ingestion
                partition.
            columns: Columns that define the
                partition key.

        Returns:
            SQL string containing only the
            latest-partition rows.
        """

    @abstractmethod
    def enrich(
        self,
        other: Union[str, List[str]],
        keys: List[Union[str, Tuple[str, str]]],
        prefix: Optional[Union[str, List[str]]] = None,
    ) -> str:
        """LEFT JOIN the input query with one or
        more queries.

        All columns from the joined side(s) are
        included in the result.

        Args:
            other: A single query string or a
                list of query strings to join.
            keys: Join condition. Each element
                is either a column name (same on
                both sides) or a
                ``(left_col, right_col)`` tuple.
            prefix: Optional prefix for columns
                from the joined side(s). When
                ``other`` is a list, a list of
                prefixes may be provided.

        Returns:
            SQL string wrapping the enriched
            result set.
        """

    @abstractmethod
    def get_diffs(
        self,
        other: str,
        keys: List[Union[str, Tuple[str, str]]],
        columns: List[Union[str, Tuple[str, str]]],
    ) -> str:
        """Compare column values between the
        input query and another query.

        Args:
            other: The second query to compare
                against.
            keys: Join keys. Each element is a
                column name (same in both
                queries) or a
                ``(left_col, right_col)`` tuple.
            columns: Columns to compare. Each
                element is a column name (same
                in both queries) or a
                ``(left_col, right_col)`` tuple.

        Returns:
            SQL string showing the differing
            rows and their values.
        """

    @abstractmethod
    def op(
        self,
        add: Optional[Dict[str, str]] = None,
        rename: Optional[Dict[str, str]] = None,
        select_only: Optional[List[str]] = None,
        exclude: Optional[List[str]] = None,
    ) -> str:
        """Apply column-level operations to the
        input query.

        Args:
            add: Mapping of
                ``{new_col: sql_expression}``
                for columns to add.
            rename: Mapping of
                ``{new_name: old_name}`` for
                columns to rename.
            select_only: Keep only these
                columns. Applied after ``add``
                and ``rename``.
            exclude: Drop these columns. Applied
                after ``add`` and ``rename``.

        Returns:
            SQL string with the column
            transformations applied.
        """

    @classmethod
    @abstractmethod
    def util_table_name_has_fullname(
        cls, table_name: str
    ) -> bool:
        """Check whether a table name is fully
        qualified.

        Args:
            table_name: Table name to inspect.

        Returns:
            True if ``table_name`` contains all
            information needed to identify the
            table (e.g. both schema and table
            name).
        """

    @classmethod
    @abstractmethod
    def util_table_name_split_schema(
        cls, table_name: str
    ) -> Tuple[str, str]:
        """Split a table name into its schema
        and table components.

        Args:
            table_name: Fully qualified table
                name.

        Returns:
            A ``(schema, table)`` tuple.
        """
