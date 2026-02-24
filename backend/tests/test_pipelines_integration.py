"""
Integration Tests for Pipeline Scenarios.

Executes real queries against test_engine.db using DuckDBAdapter + QueryBuilderService.
Verifies that the generated SQL runs successfully and returns expected schemas/data.

Prerequisites:
    Run `python seed_pipeline_test_db.py` first to create the test database.
"""

import os
import sys

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
import pytest

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from app.services.query_builder import QueryBuilderService
from app.db.oracle_adapter import OracleAdapter
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


@pytest.fixture(scope="module")
def db():
    """Shared database connection for all tests in the module."""
    adapter = OracleAdapter(user="SYSTEM", password="password", dsn="localhost:1521/xe")
    yield adapter
    adapter.close()


@pytest.fixture
def qb():
    return QueryBuilderService()


def _execute(qb, db, request):
    """Helper: build SQL and execute, return result list of dicts."""
    sql, params = qb.build_query(request)
    return db.execute_query(sql, params), sql


# â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•
# TC-PIPE-AF: Aggregate -> Filter (HAVING)
# â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•


class TestIntegrationAggregateFilter:
    def test_tc_pipe_af_having_filters_departments(self, qb, db):
        """
        Group by department, sum salary. Filter salary_sum > 200000.
        Should return only departments with total salary > 200k.
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
                        value=200000,
                    )
                ],
            ),
            limit=100,
        )

        results, sql = _execute(qb, db, request)

        assert len(results) > 0, f"Expected results, got none.\nSQL: {sql}"
        # All returned salary_sum values must be > 200000
        for row in results:
            assert row["salary_sum"] > 200000, (
                f"Expected salary_sum > 200000, got {row['salary_sum']}"
            )
        # Verify schema: only department + salary_sum
        assert set(results[0].keys()) == {"department", "salary_sum"}

    def test_tc_pipe_af_schema_only_has_aggregated_columns(self, qb, db):
        """Filter dropdown should show only department and salary_sum."""
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
            limit=100,
        )

        results, sql = _execute(qb, db, request)

        assert len(results) > 0
        for row in results:
            assert set(row.keys()) == {"department", "salary_sum"}


# â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•
# TC-PIPE-FA: Filter -> Aggregate
# â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•


class TestIntegrationFilterAggregate:
    def test_tc_pipe_fa_only_active_employees_counted(self, qb, db):
        """
        Filter status = Active first, then count by department.
        The headcount should only reflect active employees.
        """
        # First, get unfiltered count for comparison
        unfiltered = QueryRequest(
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
            limit=100,
        )
        unfiltered_results, _ = _execute(qb, db, unfiltered)
        unfiltered_total = sum(r["headcount"] for r in unfiltered_results)

        # Now with Active filter
        filtered = QueryRequest(
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
            limit=100,
        )
        filtered_results, sql = _execute(qb, db, filtered)
        filtered_total = sum(r["headcount"] for r in filtered_results)

        # Filtered count must be less (we have some Inactive employees)
        assert filtered_total < unfiltered_total, (
            f"Filtered count ({filtered_total}) should be less than unfiltered ({unfiltered_total})"
        )
        assert filtered_total > 0, "Expected some active employees"


# â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•
# TC-PIPE-JA: Join -> Aggregate
# â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•


class TestIntegrationJoinAggregate:
    def test_tc_pipe_ja_revenue_per_department(self, qb, db):
        """
        Join employee_roster to sales_orders, then group by department
        and sum order_value as dept_revenue.
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
            limit=100,
        )

        results, sql = _execute(qb, db, request)

        assert len(results) > 0, f"Expected results.\nSQL: {sql}"
        # Verify schema
        for row in results:
            assert "employee_roster.department" in row or "department" in row
            assert "dept_revenue" in row
            assert row["dept_revenue"] > 0, "Revenue should be positive"


# â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•
# TC-PIPE-ALL-01: Join -> Filter -> Aggregate
# â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•


class TestIntegrationJoinFilterAggregate:
    def test_tc_pipe_all_01_regional_closed_revenue(self, qb, db):
        """
        Join employee_roster to sales_orders. Filter status='Closed'.
        Aggregate by region, sum order_value.
        Only closed sales should contribute.
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
            limit=100,
        )

        results, sql = _execute(qb, db, request)

        assert len(results) > 0, f"Expected results.\nSQL: {sql}"
        for row in results:
            assert row["regional_closed_revenue"] > 0

        # Compare to unfiltered to verify the filter works
        unfiltered_req = QueryRequest(
            dataset="employee_roster",
            columns=["employee_roster.region", "total_all_revenue"],
            group_by=["employee_roster.region"],
            aggregations=[
                AggregationCondition(
                    column="sales_orders.order_value",
                    function=AggregationFunction.SUM,
                    output_name="total_all_revenue",
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
            limit=100,
        )
        unfiltered_results, _ = _execute(qb, db, unfiltered_req)
        unfiltered_total = sum(r["total_all_revenue"] for r in unfiltered_results)
        filtered_total = sum(r["regional_closed_revenue"] for r in results)

        # We have some 'Open' orders, so filtered should be less
        assert filtered_total <= unfiltered_total, (
            f"Closed-only total ({filtered_total}) should be <= all ({unfiltered_total})"
        )


# â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•
# TC-PIPE-ALL-02: Aggregate -> Filter -> Join
# â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•


class TestIntegrationAggregateFilterJoin:
    def test_tc_pipe_all_02_top_products_with_catalog(self, qb, db):
        """
        Aggregate sales_orders by product_id, sum revenue as total_revenue.
        Filter total_revenue > 100000.
        Join to product_catalog on product_id = id.
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
            limit=100,
        )

        results, sql = _execute(qb, db, request)

        assert len(results) > 0, f"Expected results.\nSQL: {sql}"
        for row in results:
            assert row["total_revenue"] > 100000, (
                f"total_revenue should be > 100000, got {row['total_revenue']}"
            )


# â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•
# TC-PIPE-ALL-03: Filter -> Aggregate -> Filter (Double Filter)
# â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•


class TestIntegrationDoubleFilter:
    def test_tc_pipe_all_03_na_departments_high_salary(self, qb, db):
        """
        Filter region = 'North America'.
        Aggregate by department, sum salary_usd as salary_sum.
        Filter salary_sum > 100000.

        First filter = WHERE, second = HAVING.
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
                        column="region",
                        operator=FilterOperator.EQUALS,
                        value="North America",
                    ),
                    FilterCondition(
                        column="salary_sum",
                        operator=FilterOperator.GREATER_THAN,
                        value=100000,
                    ),
                ],
            ),
            limit=100,
        )

        results, sql = _execute(qb, db, request)

        assert len(results) > 0, f"Expected results.\nSQL: {sql}"
        for row in results:
            assert row["salary_sum"] > 100000, (
                f"salary_sum should be > 100000, got {row['salary_sum']}"
            )

    def test_tc_pipe_all_03_schema_transition(self, qb, db):
        """Verify the schema transitions from raw to aggregated correctly."""
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
            limit=100,
        )

        results, sql = _execute(qb, db, request)

        assert len(results) > 0
        # Schema should be exactly {department, salary_sum}
        for row in results:
            assert set(row.keys()) == {"department", "salary_sum"}, (
                f"Unexpected schema: {set(row.keys())}"
            )


# â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•
# Count Query Consistency
# â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•


class TestCountQueryConsistency:
    def test_count_query_matches_data_query(self, qb, db):
        """build_count_query should report consistent row count with build_query."""
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
                        value=100000,
                    )
                ],
            ),
            limit=1000,
        )

        # Data query
        data_sql, data_params = qb.build_query(request)
        data_results = db.execute_query(data_sql, data_params)

        # Count query
        count_sql, count_params = qb.build_count_query(request)
        count_results = db.execute_query(count_sql, count_params)

        data_count = len(data_results)
        reported_count = count_results[0]["total_rows"]

        assert data_count == reported_count, (
            f"Data returned {data_count} rows but count query reports {reported_count}"
        )
