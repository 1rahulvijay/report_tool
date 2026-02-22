import pytest
from app.services.query_builder import QueryBuilderService
from app.schemas.query import QueryRequest, FilterCondition, LogicalGroup


def test_string_ops_cast_to_varchar():
    builder = QueryBuilderService("sqlite")
    request = QueryRequest(
        dataset="test_table",
        columns=["id"],
        filters=LogicalGroup(
            logic="AND",
            conditions=[
                FilterCondition(column="numeric_col", operator="contains", value="123")
            ],
        ),
    )
    sql, params = builder.build_query(request)
    assert 'CAST("numeric_col" AS VARCHAR) LIKE' in sql


if __name__ == "__main__":
    test_string_ops_cast_to_varchar()
    print("Test passed!")
