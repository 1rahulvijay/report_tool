import os
import sys

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

import pytest
from pydantic import ValidationError
from app.schemas.query import QueryRequest, FilterCondition


def test_numeric_valid_equals():
    cond = FilterCondition(
        column="salary", datatype="number", operator="eq", value=5000
    )
    assert cond.value == 5000


def test_numeric_valid_greater():
    cond = FilterCondition(column="salary", datatype="number", operator="gt", value=100)
    assert cond.value == 100


def test_numeric_invalid_contains():
    with pytest.raises(ValidationError) as exc:
        FilterCondition(
            column="salary", datatype="number", operator="contains", value=100
        )
    assert "not allowed for datatype" in str(exc.value)


def test_numeric_in_with_array():
    cond = FilterCondition(
        column="salary", datatype="number", operator="in", value=[1000, 2000]
    )
    assert cond.value == [1000, 2000]


def test_numeric_in_with_string():
    with pytest.raises(ValidationError) as exc:
        FilterCondition(column="salary", datatype="number", operator="in", value="abc")
    assert "must be a list for operator" in str(exc.value)


def test_numeric_is_null():
    cond = FilterCondition(
        column="salary", datatype="number", operator="is_null", value=None
    )
    assert cond.operator == "is_null"


def test_string_contains_valid():
    cond = FilterCondition(
        column="name", datatype="string", operator="contains", value="John"
    )
    assert cond.value == "John"


def test_string_starts_with():
    cond = FilterCondition(
        column="name", datatype="string", operator="starts_with", value="A"
    )
    assert cond.value == "A"


def test_string_greater_than():
    with pytest.raises(ValidationError) as exc:
        FilterCondition(column="name", datatype="string", operator="gt", value="A")
    assert "not allowed for datatype" in str(exc.value)


def test_string_in_valid():
    cond = FilterCondition(
        column="name", datatype="string", operator="in", value=["A", "B"]
    )
    assert cond.value == ["A", "B"]


def test_date_greater_valid():
    cond = FilterCondition(
        column="created_date", datatype="date", operator="gt", value="2024-01-01"
    )
    assert cond.value == "2024-01-01"


def test_date_between_valid():
    cond = FilterCondition(
        column="created_date",
        datatype="date",
        operator="between",
        value=["2024-01-01", "2024-02-01"],
    )
    assert cond.value == ["2024-01-01", "2024-02-01"]


def test_date_contains_invalid():
    with pytest.raises(ValidationError) as exc:
        FilterCondition(
            column="created_date", datatype="date", operator="contains", value="2024"
        )
    assert "not allowed for datatype" in str(exc.value)


def test_timestamp_greater_equal_valid():
    cond = FilterCondition(
        column="updated_at",
        datatype="timestamp",
        operator="gte",
        value="2024-01-01T10:00:00",
    )
    assert cond.value == "2024-01-01T10:00:00"


def test_timestamp_between_valid():
    cond = FilterCondition(
        column="updated_at",
        datatype="timestamp",
        operator="between",
        value=["t1", "t2"],
    )
    assert cond.value == ["t1", "t2"]


def test_timestamp_starts_with_invalid():
    with pytest.raises(ValidationError) as exc:
        FilterCondition(
            column="updated_at",
            datatype="timestamp",
            operator="starts_with",
            value="2024",
        )
    assert "not allowed for datatype" in str(exc.value)


def test_between_invalid_length():
    with pytest.raises(ValidationError) as exc:
        FilterCondition(
            column="created_date",
            datatype="date",
            operator="between",
            value=["2024-01-01"],
        )
    assert "EXACTLY 2 items" in str(exc.value)
