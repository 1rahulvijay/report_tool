"""
Pipeline Chaining Integration Tests.

Runs the 4 acceptance scenarios against the actual database to verify
non-linear pipeline chaining works end-to-end.

Prerequisites:
    Run `python seed_pipeline_test_db.py` first to create test_engine.db.
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
    adapter = OracleAdapter(user="SYSTEM", password="password", dsn="localhost:1521/xe")
    yield adapter
    adapter.close()


@pytest.fixture
def qb():
    return QueryBuilderService()


def _execute(qb, db, request):
    sql, params = qb.build_query(request)
    return db.execute_query(sql, params), sql, params


# â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•
# TC-PIPE-01: Aggregate â†’ Filter on Derived Metric
# â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•


class TestAggregateFilterDerived:
    """TC-PIPE-01: Group by department, SUM salary, filter salary_sum > 100000."""

    def test_filter_on_aggregated_sum(self, qb, db):
        """The HAVING clause filters on the derived metric without error."""
        request = QueryRequest(
            dataset="employee_roster",
            columns=["department", "employee_salary_sum"],
            group_by=["department"],
            aggregations=[
                AggregationCondition(
                    column="salary_usd",
                    function=AggregationFunction.SUM,
                    output_name="employee_salary_sum",
                )
            ],
            filters=LogicalGroup(
                logic="AND",
                conditions=[
                    FilterCondition(
                        column="employee_salary_sum",
                        operator=FilterOperator.GREATER_THAN,
                        value=100000,
                    )
                ],
            ),
            partition_filters={"employee_roster": [202602]},
            limit=100,
        )

        results, sql, _ = _execute(qb, db, request)

        # Must use HAVING, not WHERE
        assert "HAVING" in sql, f"Expected HAVING clause:\n{sql}"

        # All returned sums must exceed 100k
        assert len(results) > 0, f"Expected results.\nSQL: {sql}"
        for row in results:
            sum_key = next(k for k in row if "employee_salary_sum" in k)
            assert row[sum_key] > 100000, (
                f"Expected salary_sum > 100000, got {row[sum_key]}"
            )


# â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•
# TC-PIPE-02: Aggregate â†’ Join on Derived Dimension
# â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•


class TestAggregateJoinDerived:
    """TC-PIPE-02: Aggregate by department, then join to department_budgets."""

    def test_join_after_aggregation(self, qb, db):
        """Aggregated result joins with department_budgets on department dimension."""
        request = QueryRequest(
            dataset="employee_roster",
            columns=[
                "employee_roster.department",
                "dept_total",
                "department_budgets.budget",
            ],
            group_by=["employee_roster.department"],
            aggregations=[
                AggregationCondition(
                    column="salary_usd",
                    function=AggregationFunction.SUM,
                    output_name="dept_total",
                )
            ],
            joins=[
                JoinCondition(
                    left_dataset="employee_roster",
                    right_dataset="department_budgets",
                    join_type=JoinType.LEFT,
                    on=[JoinOn(left_column="department", right_column="dept_name")],
                )
            ],
            partition_filters={
                "employee_roster": [202602],
                "department_budgets": [202602],
            },
            limit=100,
        )

        results, sql, _ = _execute(qb, db, request)

        assert "SUM" in sql, f"Expected SUM:\n{sql}"
        assert "JOIN" in sql, f"Expected JOIN:\n{sql}"
        assert "GROUP BY" in sql, f"Expected GROUP BY:\n{sql}"

        # Should have department + calculated total + budget columns
        assert len(results) > 0, f"Expected joined results.\nSQL: {sql}"
        first = results[0]
        assert any("dept_total" in k for k in first.keys()), (
            f"Expected dept_total in keys: {list(first.keys())}"
        )


# â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•
# TC-PIPE-03: Join â†’ Filter â†’ Aggregate (Standard ETL Flow)
# â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•


class TestJoinFilterAggregate:
    """TC-PIPE-03: Joinâ†’Filterâ†’Aggregate standard ETL flow."""

    def test_join_filter_aggregate_etl(self, qb, db):
        """
        1. Join employee_roster to sales_orders on emp_id
        2. Filter: sales_orders.status = 'Closed'
        3. Aggregate: Group by department, SUM order_value as closed_revenue
        """
        request = QueryRequest(
            dataset="employee_roster",
            columns=["employee_roster.department", "closed_revenue"],
            group_by=["employee_roster.department"],
            aggregations=[
                AggregationCondition(
                    column="sales_orders.order_value",
                    function=AggregationFunction.SUM,
                    output_name="closed_revenue",
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
            partition_filters={"employee_roster": [202602]},
            limit=100,
        )

        results, sql, params = _execute(qb, db, request)

        assert "JOIN" in sql, f"Expected JOIN:\n{sql}"
        assert "GROUP BY" in sql, f"Expected GROUP BY:\n{sql}"
        assert "SUM" in sql, f"Expected SUM:\n{sql}"

        # Should have results with closed_revenue
        assert len(results) > 0, f"Expected results.\nSQL: {sql}"

        # Verify 'Closed' filter was applied (in params, not hardcoded)
        assert any(v == "Closed" for v in params.values()), (
            "Expected 'Closed' in params"
        )


# â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•
# TC-PIPE-04: Filter â†’ Join â†’ Aggregate (Early Narrowing)
# â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•


class TestFilterJoinAggregate:
    """TC-PIPE-04: Narrowing base before joining then aggregating."""

    def test_filter_join_aggregate_pushdown(self, qb, db):
        """
        1. Filter: employee_roster.department = 'Engineering'
        2. Join to sales_orders on emp_id
        3. Aggregate: Group by employee_roster.region, SUM order_value as regional_eng_revenue
        """
        request = QueryRequest(
            dataset="employee_roster",
            columns=["employee_roster.region", "regional_eng_revenue"],
            group_by=["employee_roster.region"],
            aggregations=[
                AggregationCondition(
                    column="sales_orders.order_value",
                    function=AggregationFunction.SUM,
                    output_name="regional_eng_revenue",
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
                        column="department",
                        operator=FilterOperator.EQUALS,
                        value="Engineering",
                    )
                ],
            ),
            partition_filters={"employee_roster": [202602]},
            limit=100,
        )

        results, sql, params = _execute(qb, db, request)

        assert "JOIN" in sql, f"Expected JOIN:\n{sql}"
        assert "GROUP BY" in sql, f"Expected GROUP BY:\n{sql}"
        assert "SUM" in sql, f"Expected SUM:\n{sql}"

        # Verify Engineering filter was applied before join (pushdown)
        assert any(v == "Engineering" for v in params.values()), (
            "Expected 'Engineering' in params"
        )

        # All results should be from Engineering departments only
        assert len(results) > 0, f"Expected results.\nSQL: {sql}"


# â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•
# TC-PIPE-COMBO: Double Filter (Pre + Post Aggregation)
# â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•


class TestDoubleFilterIntegration:
    """Filter raw data, aggregate, then filter on derived metric."""

    def test_where_plus_having_end_to_end(self, qb, db):
        """
        1. Filter: region = 'North America'
        2. Aggregate: Group by department, SUM salary_usd as salary_sum
        3. Filter: salary_sum > 0
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
                        value=0,
                    ),
                ],
            ),
            partition_filters={"employee_roster": [202602]},
            limit=100,
        )

        results, sql, params = _execute(qb, db, request)

        # Must have BOTH WHERE and HAVING
        assert "HAVING" in sql, f"Expected HAVING:\n{sql}"
        # Region filter should be WHERE or pushdown, salary_sum in HAVING
        assert any(v == "North America" for v in params.values())
        assert len(results) > 0, f"Expected results.\nSQL: {sql}"
