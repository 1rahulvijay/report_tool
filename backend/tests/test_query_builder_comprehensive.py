import pytest
import datetime
from app.schemas.query import (
    QueryRequest,
    FilterCondition,
    FilterOperator,
    LogicalGroup,
    JoinCondition,
    JoinOn,
    JoinType,
    AggregationCondition,
    AggregationFunction,
    SortCondition,
)
from app.services.query_builder.service import QueryBuilderService


@pytest.fixture
def query_builder():
    return QueryBuilderService()


def test_basic_select(query_builder):
    req = QueryRequest(
        dataset="USERS", columns=["ID", "NAME", "CREATED_AT"], limit=50, offset=0
    )
    sql, params = query_builder.build_query(req)
    assert (
        'SELECT "USERS"."ID" AS "USERS.ID", "USERS"."NAME" AS "USERS.NAME", "USERS"."CREATED_AT" AS "USERS.CREATED_AT"'
        in sql
    )
    assert 'FROM "USERS" "USERS"' in sql
    assert "OFFSET 0 ROWS FETCH NEXT 50 ROWS ONLY" in sql
    assert params == {}


def test_date_contains_filter(query_builder):
    """Tests the new 'contains' capabilities for dates implemented for Header Filters"""
    req = QueryRequest(
        dataset="USERS",
        columns=["ID"],
        filters=LogicalGroup(
            logic="AND",
            conditions=[
                FilterCondition(
                    column="CREATED_AT",
                    datatype="date",
                    operator=FilterOperator.CONTAINS,
                    value="2020",
                )
            ],
        ),
    )
    sql, params = query_builder.build_query(req)
    assert (
        "WHERE (TO_CHAR(\"CREATED_AT\", 'YYYY-MM-DD HH24:MI:SS') LIKE UPPER(:p_1))"
        in sql
    )
    assert params["p_1"] == "%2020%"


def test_date_between_filter(query_builder):
    """Tests the fix for Date arrays"""
    req = QueryRequest(
        dataset="USERS",
        columns=["ID"],
        filters=LogicalGroup(
            logic="AND",
            conditions=[
                FilterCondition(
                    column="CREATED_AT",
                    datatype="date",
                    operator=FilterOperator.BETWEEN,
                    value=["2020-01-01", "2020-12-31"],
                )
            ],
        ),
    )
    sql, params = query_builder.build_query(req)
    assert "BETWEEN :p_1 AND :p_2" in sql

    # Assert proper string to date translation in pydantic/builder
    assert isinstance(params["p_1"], datetime.date)
    assert params["p_1"].year == 2020


def test_text_not_equals_filter(query_builder):
    """Tests the fix that prevents NOT LIKE dropping nulls"""
    req = QueryRequest(
        dataset="USERS",
        columns=["ID"],
        filters=LogicalGroup(
            logic="AND",
            conditions=[
                FilterCondition(
                    column="ROLE",
                    datatype="string",
                    operator=FilterOperator.NOT_EQUALS,
                    value="ADMIN",
                )
            ],
        ),
    )
    sql, params = query_builder.build_query(req)
    assert 'UPPER(CAST("ROLE" AS VARCHAR2(4000))) != :p_1' in sql
    assert params["p_1"] == "ADMIN"


def test_aggregations_and_group_by(query_builder):
    """Tests complex aggregations mapping aliases"""
    req = QueryRequest(
        dataset="ORDERS",
        group_by=["DEPARTMENT"],
        aggregations=[
            AggregationCondition(
                column="TOTAL_SALES",
                function=AggregationFunction.SUM,
                output_name="SUM_SALES",
            )
        ],
    )
    sql, params = query_builder.build_query(req)
    # Check aggregation outputs and groupings
    assert 'SUM("ORDERS"."TOTAL_SALES") AS "SUM_SALES"' in sql
    assert 'GROUP BY "ORDERS"."DEPARTMENT"' in sql


def test_complex_joins(query_builder):
    """Tests multiple joins and alias resolution"""
    req = QueryRequest(
        dataset="EMP",
        columns=["EMP.NAME", "DP.DEPT_NAME"],
        joins=[
            JoinCondition(
                left_dataset="EMP",
                right_dataset="DP",
                join_type=JoinType.INNER,
                on=[JoinOn(left_column="EMP_ID", right_column="MANAGER_ID")],
            )
        ],
    )
    sql, params = query_builder.build_query(req)
    assert 'INNER JOIN "DP" "DP" ON "EMP"."EMP_ID" = "DP"."MANAGER_ID"' in sql


def test_advanced_nested_logical_groups(query_builder):
    """Tests recursive deep logic translation"""
    req = QueryRequest(
        dataset="USERS",
        columns=["ID"],
        filters=LogicalGroup(
            logic="OR",
            conditions=[
                FilterCondition(
                    column="STATUS",
                    datatype="string",
                    operator=FilterOperator.EQUALS,
                    value="ACTIVE",
                ),
                LogicalGroup(
                    logic="AND",
                    conditions=[
                        FilterCondition(
                            column="AGE",
                            datatype="number",
                            operator=FilterOperator.GREATER_THAN,
                            value=18,
                        ),
                        FilterCondition(
                            column="AGE",
                            datatype="number",
                            operator=FilterOperator.LESS_THAN,
                            value=65,
                        ),
                    ],
                ),
            ],
        ),
    )
    sql, params = query_builder.build_query(req)

    assert " OR " in sql
    assert " AND " in sql
    assert "> :p_2" in sql
    assert "< :p_3" in sql
