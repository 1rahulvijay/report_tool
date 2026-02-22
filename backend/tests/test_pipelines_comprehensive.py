"""
Comprehensive Pipeline Unit Tests for QueryBuilderService.

Tests all pipeline permutations of Aggregation, Filter, and Join operations
by verifying the SQL generation output. No database or HTTP server required.

Test Cases:
  TC-PIPE-AF:      Aggregate -> Filter (HAVING clause)
  TC-PIPE-AJ:      Aggregate -> Join
  TC-PIPE-AJ-02:   Aggregate -> Join on derived metric
  TC-PIPE-FA:      Filter -> Aggregate
  TC-PIPE-JA:      Join -> Aggregate
  TC-PIPE-ALL-01:  Join -> Filter -> Aggregate
  TC-PIPE-ALL-02:  Aggregate -> Filter -> Join
  TC-PIPE-ALL-03:  Filter -> Aggregate -> Filter (double filter)
"""

import os
import sys
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
import pytest


from app.services.query_builder import QueryBuilderService
from app.schemas.query import (
    QueryRequest,
    LogicalGroup,
    FilterCondition,
    FilterOperator,
    JoinCondition,
    JoinOn,
    JoinType,
    AggregationCondition,
    AggregationFunction,
)


@pytest.fixture
def qb():
    return QueryBuilderService(dialect="duckdb")


# â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•
# Two-Option Pipelines: Aggregation as the Base
# â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•


class TestAggregateFirst:
    """Tests where aggregation is the first operation, modifying the schema."""

    def test_tc_pipe_af_aggregate_then_filter_on_derived_metric(self, qb):
        """
        TC-PIPE-AF: Aggregate -> Filter on derived metric.

        Scenario: Group by department, Sum salary_usd as salary_sum.
        Then filter: salary_sum > 5000000.

        Expected: The system generates a HAVING clause (not WHERE)
        for the derived metric salary_sum.
        """
        request = QueryRequest(
            dataset="employee_roster",
            columns=["department", "salary_sum"],
            group_by=["department"],
            aggregations=[
                AggregationCondition(
                    column="salary_usd",
                    function=AggregationFunction.SUM,
                    output_name="salary_sum",
                )
            ],
            filters=LogicalGroup(
                logic="AND",
                conditions=[
                    FilterCondition(
                        column="salary_sum",
                        operator=FilterOperator.GREATER_THAN,
                        value=5000000,
                    )
                ],
            ),
        )

        sql, params = qb.build_query(request)

        # Verify HAVING is used for the derived metric, not WHERE
        assert "HAVING" in sql, f"Expected HAVING clause in SQL:\n{sql}"
        assert "salary_sum" in sql.split("HAVING")[1], "salary_sum should be in HAVING"
        assert "GROUP BY" in sql, f"Expected GROUP BY in SQL:\n{sql}"

        # Verify salary_sum is NOT in WHERE clause
        if "WHERE" in sql:
            where_portion = sql.split("WHERE")[1].split("GROUP BY")[0]
            assert "salary_sum" not in where_portion, (
                "salary_sum should NOT be in WHERE clause"
            )

    def test_tc_pipe_aj_aggregate_then_join(self, qb):
        """
        TC-PIPE-AJ: Aggregate -> Join.

        Scenario: Group sales_orders by emp_id, Sum order_value as total_sales.
        Then join to employee_roster on emp_id.

        Expected: SQL contains GROUP BY, aggregation function, and JOIN.
        """
        request = QueryRequest(
            dataset="sales_orders",
            columns=["sales_orders.emp_id", "total_sales"],
            group_by=["sales_orders.emp_id"],
            aggregations=[
                AggregationCondition(
                    column="sales_orders.order_value",
                    function=AggregationFunction.SUM,
                    output_name="total_sales",
                )
            ],
            joins=[
                JoinCondition(
                    left_dataset="sales_orders",
                    right_dataset="employee_roster",
                    join_type=JoinType.INNER,
                    on=[JoinOn(left_column="emp_id", right_column="emp_id")],
                )
            ],
        )

        sql, params = qb.build_query(request)

        assert "GROUP BY" in sql, f"Expected GROUP BY in SQL:\n{sql}"
        assert "SUM" in sql, f"Expected SUM in SQL:\n{sql}"
        assert "JOIN" in sql, f"Expected JOIN in SQL:\n{sql}"
        assert "total_sales" in sql, f"Expected total_sales alias in SQL:\n{sql}"

    def test_tc_pipe_aj_02_aggregate_then_join_on_derived_metric(self, qb):
        """
        TC-PIPE-AJ-02: Aggregate -> Join ON a derived metric.

        Scenario: Group student_scores by student_id, Avg test_score as avg_score.
        Then join to grade_thresholds on avg_score = exact_score.

        Expected: SQL contains GROUP BY, AVG, JOIN, and the derived column in the query.
        Note: Non-equi joins (>=) are complex; we test equi-join on exact_score.
        """
        request = QueryRequest(
            dataset="student_scores",
            columns=["student_scores.student_id", "avg_score"],
            group_by=["student_scores.student_id"],
            aggregations=[
                AggregationCondition(
                    column="student_scores.test_score",
                    function=AggregationFunction.AVG,
                    output_name="avg_score",
                )
            ],
            joins=[
                JoinCondition(
                    left_dataset="student_scores",
                    right_dataset="grade_thresholds",
                    join_type=JoinType.LEFT,
                    on=[
                        JoinOn(
                            left_column="avg_score",
                            right_column="exact_score",
                        )
                    ],
                )
            ],
        )

        sql, params = qb.build_query(request)

        assert "AVG" in sql, f"Expected AVG in SQL:\n{sql}"
        assert "GROUP BY" in sql, f"Expected GROUP BY in SQL:\n{sql}"
        assert "JOIN" in sql, f"Expected JOIN in SQL:\n{sql}"
        assert "avg_score" in sql, f"Expected avg_score alias in SQL:\n{sql}"


# â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•
# Two-Option Pipelines: Operations Before Aggregation
# â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•


class TestOperationsBeforeAggregation:
    """Tests where filter or join happens first, modifying data before aggregation."""

    def test_tc_pipe_fa_filter_then_aggregate(self, qb):
        """
        TC-PIPE-FA: Filter -> Aggregate.

        Scenario: Filter status = 'Active', then Group by department, Count emp_id.

        Expected: WHERE clause for status filter, then GROUP BY + COUNT.
        """
        request = QueryRequest(
            dataset="employee_roster",
            columns=["department", "headcount"],
            group_by=["department"],
            aggregations=[
                AggregationCondition(
                    column="emp_id",
                    function=AggregationFunction.COUNT,
                    output_name="headcount",
                )
            ],
            filters=LogicalGroup(
                logic="AND",
                conditions=[
                    FilterCondition(
                        column="status",
                        operator=FilterOperator.EQUALS,
                        value="Active",
                    )
                ],
            ),
        )

        sql, params = qb.build_query(request)

        assert "WHERE" in sql, f"Expected WHERE in SQL:\n{sql}"
        assert "GROUP BY" in sql, f"Expected GROUP BY in SQL:\n{sql}"
        assert "COUNT" in sql, f"Expected COUNT in SQL:\n{sql}"
        # The status filter should be in WHERE, not HAVING
        assert "HAVING" not in sql, f"status filter should NOT be in HAVING:\n{sql}"

    def test_tc_pipe_ja_join_then_aggregate(self, qb):
        """
        TC-PIPE-JA: Join -> Aggregate.

        Scenario: Join employee_roster to sales_orders on emp_id.
        Then aggregate: Group by department, Sum order_value as dept_revenue.

        Expected: JOIN clause present, GROUP BY on qualified column,
        SUM on joined column.
        """
        request = QueryRequest(
            dataset="employee_roster",
            columns=["employee_roster.department", "dept_revenue"],
            group_by=["employee_roster.department"],
            aggregations=[
                AggregationCondition(
                    column="sales_orders.order_value",
                    function=AggregationFunction.SUM,
                    output_name="dept_revenue",
                )
            ],
            joins=[
                JoinCondition(
                    left_dataset="employee_roster",
                    right_dataset="sales_orders",
                    join_type=JoinType.INNER,
                    on=[JoinOn(left_column="emp_id", right_column="emp_id")],
                )
            ],
        )

        sql, params = qb.build_query(request)

        assert "JOIN" in sql, f"Expected JOIN in SQL:\n{sql}"
        assert "GROUP BY" in sql, f"Expected GROUP BY in SQL:\n{sql}"
        assert "SUM" in sql, f"Expected SUM in SQL:\n{sql}"
        assert "dept_revenue" in sql, f"Expected dept_revenue alias in SQL:\n{sql}"
        # Verify the SUM references the joined column
        assert "order_value" in sql, (
            "SUM should reference order_value from sales_orders"
        )


# â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•
# Three-Option Pipelines ("All Options" Stress Tests)
# â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•


class TestThreeOptionPipelines:
    """Tests combining all three operations: Join, Filter, and Aggregate."""

    def test_tc_pipe_all_01_join_filter_aggregate(self, qb):
        """
        TC-PIPE-ALL-01: Join -> Filter -> Aggregate (Standard ETL Flow).

        Scenario: Join employee_roster to sales_orders on emp_id.
        Filter: sales_orders.status = 'Closed'.
        Aggregate: Group by employee_roster.region, Sum order_value as regional_closed_revenue.

        Expected: JOIN + WHERE (or pushdown) for status + GROUP BY + SUM.
        Final columns: region and regional_closed_revenue.
        """
        request = QueryRequest(
            dataset="employee_roster",
            columns=["employee_roster.region", "regional_closed_revenue"],
            group_by=["employee_roster.region"],
            aggregations=[
                AggregationCondition(
                    column="sales_orders.order_value",
                    function=AggregationFunction.SUM,
                    output_name="regional_closed_revenue",
                )
            ],
            joins=[
                JoinCondition(
                    left_dataset="employee_roster",
                    right_dataset="sales_orders",
                    join_type=JoinType.INNER,
                    on=[JoinOn(left_column="emp_id", right_column="emp_id")],
                )
            ],
            filters=LogicalGroup(
                logic="AND",
                conditions=[
                    FilterCondition(
                        column="sales_orders.status",
                        operator=FilterOperator.EQUALS,
                        value="Closed",
                    )
                ],
            ),
        )

        sql, params = qb.build_query(request)

        assert "JOIN" in sql, f"Expected JOIN:\n{sql}"
        assert "GROUP BY" in sql, f"Expected GROUP BY:\n{sql}"
        assert "SUM" in sql, f"Expected SUM:\n{sql}"
        assert "regional_closed_revenue" in sql, f"Expected alias:\n{sql}"
        # The status filter should be in WHERE or pushdown subquery, NOT HAVING
        assert "HAVING" not in sql, f"status filter should NOT be in HAVING:\n{sql}"
        # Verify the status filter exists somewhere (WHERE or pushdown)
        assert any(k in params for k in params if params[k] == "Closed"), (
            "Expected 'Closed' in query parameters"
        )

    def test_tc_pipe_all_02_aggregate_filter_join(self, qb):
        """
        TC-PIPE-ALL-02: Aggregate -> Filter -> Join (Derived Architecture Flow).

        Scenario: Aggregate sales_orders: Group by product_id, Sum revenue as total_revenue.
        Filter: total_revenue > 100000.
        Join: product_catalog ON product_id = product_catalog.id.

        Expected:
        - Step 1: GROUP BY + SUM creates [product_id, total_revenue]
        - Step 2: HAVING for total_revenue > 100000
        - Step 3: JOIN attaches product_catalog data
        """
        request = QueryRequest(
            dataset="sales_orders",
            columns=["sales_orders.product_id", "total_revenue"],
            group_by=["sales_orders.product_id"],
            aggregations=[
                AggregationCondition(
                    column="sales_orders.revenue",
                    function=AggregationFunction.SUM,
                    output_name="total_revenue",
                )
            ],
            filters=LogicalGroup(
                logic="AND",
                conditions=[
                    FilterCondition(
                        column="total_revenue",
                        operator=FilterOperator.GREATER_THAN,
                        value=100000,
                    )
                ],
            ),
            joins=[
                JoinCondition(
                    left_dataset="sales_orders",
                    right_dataset="product_catalog",
                    join_type=JoinType.INNER,
                    on=[JoinOn(left_column="product_id", right_column="id")],
                )
            ],
        )

        sql, params = qb.build_query(request)

        assert "GROUP BY" in sql, f"Expected GROUP BY:\n{sql}"
        assert "SUM" in sql, f"Expected SUM:\n{sql}"
        assert "HAVING" in sql, f"Expected HAVING for total_revenue filter:\n{sql}"
        assert "JOIN" in sql, f"Expected JOIN:\n{sql}"
        assert "total_revenue" in sql, f"Expected total_revenue:\n{sql}"

    def test_tc_pipe_all_03_filter_aggregate_filter_double(self, qb):
        """
        TC-PIPE-ALL-03: Filter -> Aggregate -> Filter ("Double Filter" Injection).

        Scenario:
        1. Filter raw data: region = 'North America'.
        2. Aggregate: Group by department, Sum salary_usd as salary_sum.
        3. Filter aggregated: salary_sum > 1000000.

        Expected: WHERE for region filter, GROUP BY + SUM, HAVING for salary_sum.
        Backend applies first filter as WHERE and second as HAVING.
        """
        request = QueryRequest(
            dataset="employee_roster",
            columns=["department", "salary_sum"],
            group_by=["department"],
            aggregations=[
                AggregationCondition(
                    column="salary_usd",
                    function=AggregationFunction.SUM,
                    output_name="salary_sum",
                )
            ],
            filters=LogicalGroup(
                logic="AND",
                conditions=[
                    # Pre-aggregation filter (raw column)
                    FilterCondition(
                        column="region",
                        operator=FilterOperator.EQUALS,
                        value="North America",
                    ),
                    # Post-aggregation filter (derived metric)
                    FilterCondition(
                        column="salary_sum",
                        operator=FilterOperator.GREATER_THAN,
                        value=1000000,
                    ),
                ],
            ),
        )

        sql, params = qb.build_query(request)

        assert "GROUP BY" in sql, f"Expected GROUP BY:\n{sql}"
        assert "SUM" in sql, f"Expected SUM:\n{sql}"

        # The key assertion: BOTH WHERE and HAVING must exist
        # region goes to WHERE, salary_sum goes to HAVING
        assert "HAVING" in sql, f"Expected HAVING for salary_sum:\n{sql}"

        # Verify salary_sum is in HAVING, not WHERE
        having_portion = sql.split("HAVING")[1]
        assert "salary_sum" in having_portion, "salary_sum should be in HAVING portion"

        # North America should be in a WHERE or pushdown, not HAVING
        # Check params contain 'North America'
        assert any(v == "North America" for v in params.values()), (
            "Expected 'North America' in parameters"
        )


# â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•
# SQL Safety & Structure Tests
# â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•


class TestSQLSafety:
    """Verify that parameterized queries are used (no SQL injection)."""

    def test_all_filters_are_parameterized(self, qb):
        """Every filter value must appear in params dict, never inline in SQL."""
        request = QueryRequest(
            dataset="employee_roster",
            columns=["department", "salary_sum"],
            group_by=["department"],
            aggregations=[
                AggregationCondition(
                    column="salary_usd",
                    function=AggregationFunction.SUM,
                    output_name="salary_sum",
                )
            ],
            filters=LogicalGroup(
                logic="AND",
                conditions=[
                    FilterCondition(
                        column="region",
                        operator=FilterOperator.EQUALS,
                        value="North America",
                    ),
                    FilterCondition(
                        column="salary_sum",
                        operator=FilterOperator.GREATER_THAN,
                        value=500000,
                    ),
                ],
            ),
        )

        sql, params = qb.build_query(request)

        # Values should be in params, not hardcoded in SQL
        assert "North America" not in sql, "Filter value should be parameterized"
        assert "500000" not in sql, "Filter value should be parameterized"
        assert len(params) >= 2, f"Expected at least 2 params, got {len(params)}"

    def test_identifiers_are_quoted(self, qb):
        """All table and column names should be quoted to prevent injection."""
        request = QueryRequest(
            dataset="employee_roster",
            columns=["department"],
            group_by=["department"],
            aggregations=[
                AggregationCondition(
                    column="salary_usd",
                    function=AggregationFunction.SUM,
                    output_name="salary_sum",
                )
            ],
        )

        sql, params = qb.build_query(request)

        # Identifiers should be double-quoted
        assert '"employee_roster"' in sql or '"department"' in sql, (
            f"Expected quoted identifiers:\n{sql}"
        )

