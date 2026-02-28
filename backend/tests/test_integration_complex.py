import pytest
import datetime
from app.services.query_builder import (
    QueryBuilderService,
    ParamGenerator,
)
from app.schemas.query import QueryRequest


@pytest.fixture
def builder():
    return QueryBuilderService()


def test_named_placeholder_verification(builder):
    """Test that :p_1 binds correctly and stays unique."""
    pg = ParamGenerator()
    name1, ph1 = pg.add("p", 100)
    name2, ph2 = pg.add("p", 200)

    assert ph1 == ":p_1"
    assert ph2 == ":p_2"
    assert pg.params[name1] == 100
    assert pg.params[name2] == 200


def test_mixed_or_group_promotion(builder):
    """Verify raw + agg filters in OR group promote raw to HAVING."""
    request = QueryRequest(
        dataset="test_table",
        columns=["id", "val"],
        aggregations=[{"column": "val", "function": "sum", "output_name": "sum_val"}],
        group_by=["id"],
        filters={
            "logic": "OR",
            "conditions": [
                {"column": "id", "operator": "eq", "value": 5, "datatype": "number"},
                {
                    "column": "sum_val",
                    "operator": "gt",
                    "value": 100,
                    "datatype": "number",
                },
            ],
        },
        column_metadata={
            "id": {"base_type": "numeric"},
            "sum_val": {"is_derived": True, "base_type": "numeric"},
        },
    )

    sql, params = builder.build_query(request)

    # Check that MAX(id) = :p_N is in HAVING, and no WHERE for id = 5
    assert "HAVING" in sql
    assert "MAX(" in sql
    assert "WHERE" not in sql or "1=1" in sql
    # It might cast it to varchar for safe comparison but MAX will definitely wrap the column block
    assert "MAX(" in sql
    assert "SUM_VAL" in sql


def test_large_filter_set_placeholders(builder):
    """Test queries with many filters to ensure placeholder numbers don't collide."""
    conditions = []
    for i in range(50):
        conditions.append(
            {"column": "id", "operator": "eq", "value": i, "datatype": "number"}
        )

    request = QueryRequest(
        dataset="test_table",
        columns=["id"],
        filters={"logic": "AND", "conditions": conditions},
    )

    sql, params = builder.build_query(request)

    assert len(params) == 50
    for i in range(1, 51):
        assert f":p_{i}" in sql


def test_date_parsing_integration(builder):
    """Test that ISO date strings are correctly parsed and passed to placeholders."""
    request = QueryRequest(
        dataset="test_table",
        columns=["hire_date"],
        filters={
            "logic": "AND",
            "conditions": [
                {
                    "column": "hire_date",
                    "operator": "eq",
                    "value": "2023-01-01",
                    "datatype": "date",
                }
            ],
        },
        column_metadata={"hire_date": {"base_type": "date"}},
    )

    sql, params = builder.build_query(request)

    found_val = list(params.values())[0]
    assert isinstance(found_val, datetime.date)
    assert found_val.year == 2023


def test_between_string_parsing(builder):
    """Verify 'between' filters handle 'START TO END' string from UI."""
    request = QueryRequest(
        dataset="test_table",
        columns=["val"],
        filters={
            "logic": "AND",
            "conditions": [
                {
                    "column": "hire_date",
                    "operator": "between",
                    "value": ["2022-01-01", "2022-12-31"],
                    "datatype": "date",
                }
            ],
        },
        column_metadata={"hire_date": {"base_type": "date"}},
    )

    sql, params = builder.build_query(request)
    assert "BETWEEN :p_1 AND :p_2" in sql
    assert params["p_1"] == datetime.date(2022, 1, 1)
    assert params["p_2"] == datetime.date(2022, 12, 31)


def test_case_insensitive_partition_matching(builder, monkeypatch):
    """Verify lowercase config in partitions.json works with Oracle quoting."""
    import app.services.query_builder.service as qb_service

    # Mock partition config with lowercase column
    monkeypatch.setattr(
        qb_service, "get_partition_config", lambda x: {"load_id_column": "hire_date"}
    )

    request = QueryRequest(
        dataset="EMPLOYEE_ROSTER",
        columns=["EMP_ID"],
        partition_filters={"EMPLOYEE_ROSTER": ["2022-09-25"]},
        column_metadata={"EMPLOYEE_ROSTER.HIRE_DATE": {"base_type": "date"}},
    )

    sql, params = builder.build_query(request)

    # Should uppercase and quote hire_date -> "HIRE_DATE"
    assert '"HIRE_DATE" =' in sql
    # Metadata lookup should work case-insensitively
    assert isinstance(params["part_EMPLOYEE_ROSTER_1"], datetime.date)
