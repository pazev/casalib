import pytest
from typing import Dict, Tuple, List
from your_module import PathMetadata, ColumnMetadata, ColumnTypeAbstract, InputAbstract


# --- Mock Classes for Testing ---
class DummyColumnType(ColumnTypeAbstract):
    def describe(self) -> str:
        return "dummy"


class DummyInput(InputAbstract):
    def get_metadata(self) -> Dict[str, Tuple[str, ColumnTypeAbstract]]:
        return {
            "col1": ("col1", DummyColumnType()),
            "col2": ("col2", DummyColumnType()),
        }


# --- Fixtures ---

@pytest.fixture
def dummy_input():
    return DummyInput()

@pytest.fixture
def dummy_col_type():
    return DummyColumnType()

@pytest.fixture
def path_metadata(dummy_col_type, dummy_input):
    return PathMetadata(columns={
        "a": ColumnMetadata(name="a", col_type=dummy_col_type, orig_cols=["x", "y"], input=dummy_input),
        "b": ColumnMetadata(name="b", col_type=dummy_col_type, orig_cols=["z"], input=dummy_input),
    })


# --- Tests ---

def test_add_column(path_metadata, dummy_col_type, dummy_input):
    new_meta = path_metadata.add_column("c", dummy_col_type, ["a", "b"], dummy_input)
    assert "c" in new_meta.columns
    assert new_meta.columns["c"].orig_cols == ["a", "b"]
    assert new_meta.columns["c"].input == dummy_input

def test_drop_columns(path_metadata):
    dropped = path_metadata.drop_columns(["a"])
    assert "a" not in dropped.columns
    assert "b" in dropped.columns

def test_select_columns(path_metadata):
    selected = path_metadata.select_columns(["b"])
    assert "b" in selected.columns
    assert "a" not in selected.columns
    assert len(selected.columns) == 1

def test_column_metadata(dummy_col_type, dummy_input):
    metadata = ColumnMetadata(name="test", col_type=dummy_col_type, orig_cols=["x", "y"], input=dummy_input)
    assert metadata.name == "test"
    assert metadata.col_type.describe() == "dummy"
    assert metadata.orig_cols == ["x", "y"]
    assert metadata.input == dummy_input

def test_dummy_input_metadata(dummy_input):
    metadata = dummy_input.get_metadata()
    assert "col1" in metadata
    assert isinstance(metadata["col1"][1], ColumnTypeAbstract)

def test_immutability(path_metadata):
    with pytest.raises(TypeError):
        path_metadata.columns["a"] = None  # frozen dataclass prevents modification
