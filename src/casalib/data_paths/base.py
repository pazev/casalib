"""
Module to deal with data paths.

Data paths are data structures to represent operations to be
applied over a structured table.
"""
from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import Dict, List, Optional, Tuple


class ColumnTypeAbstract(ABC):
    """
    Abstract class representing the type of a column.

    Subclasses should implement logic describing the
    column's data type.
    """


class InputAbstract(ABC):
    """
    Abstract class representing a structured data table.
    """

    @abstractmethod
    def get_metadata(
        self
    ) -> Dict[str, Tuple[str, ColumnTypeAbstract]]:
        """Retrieves metadata for the structured table.

        Returns:
            Dict[str, Tuple[str, ColumnTypeAbstract]]: A
                mapping from column names to a tuple
                containing the original column name and its
                type.
        """


@dataclass(frozen=True)
class ColumnMetadata:
    """Stores metadata for a single column.

    Attributes:
        name (str): The column name.
        col_type (ColumnTypeAbstract): The type of the
            column.
        orig_cols (Optional[List[str]]): List of original
            column names involved, if any.
        input (Optional[InputAbstract]): Reference to the
            input table the column originated from.
    """
    name: str
    col_type: ColumnTypeAbstract
    orig_cols: Optional[List[str]] = None
    input: Optional[InputAbstract] = None


@dataclass(frozen=True)
class PathMetadata:
    """
    Represents a collection of columns and their
    metadata.

    Attributes:
        columns (Dict[str, ColumnMetadata]): A dictionary
            mapping column names to their metadata.
    """
    columns: Dict[str, ColumnMetadata]

    def add_column(
        self,
        name: str,
        col_type: ColumnTypeAbstract,
        orig_cols: Optional[List[str]] = None,
        input: Optional[InputAbstract] = None
    ) -> "PathMetadata":
        """Returns a new PathMetadata with an added column.

        Args:
            name (str): Name of the new column.
            col_type (ColumnTypeAbstract): Type of the new
                column.
            orig_cols (Optional[List[str]]): Original
                columns used to compute the new one.
            input (Optional[InputAbstract]): Source input
                associated with the new column.

        Returns:
            PathMetadata: A new PathMetadata instance
                including the added column.
        """
        new_column = ColumnMetadata(
            name,
            col_type,
            orig_cols,
            input
        )
        columns = {**self.columns, name: new_column}
        return PathMetadata(columns)

    def drop_columns(
        self,
        names: List[str]
    ) -> "PathMetadata":
        """
        Returns a new PathMetadata without the specified
        columns.

        Args:
            names (List[str]): List of column names to
                remove.

        Returns:
            PathMetadata: A new PathMetadata instance
                excluding the specified columns.
        """
        columns = {
            col: col_meta
            for col, col_meta in self.columns.items()
            if col not in names
        }
        return PathMetadata(columns)

    def select_columns(
        self,
        names: List[str]
    ) -> "PathMetadata":
        """
        Returns a new PathMetadata including only the
        specified columns.

        Args:
            names (List[str]): List of column names to keep.

        Returns:
            PathMetadata: A new PathMetadata instance
                containing only the specified columns.
        """
        columns = {
            col: col_meta
            for col, col_meta in self.columns.items()
            if col in names
        }
        return PathMetadata(columns)
