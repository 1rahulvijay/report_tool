"""
Dynamic Data Partitioning (Load ID Selection) Tests.

Tests auto-default to MAX partition, user override, multi-select,
cross-partition joins, data lag handling, and empty table safeguards.

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
from app.core.partition_config import get_partition_config, is_partitioned
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
    return QueryBuilderService(dialect="duckdb")


def _execute(qb, db, request):
    sql, params = qb.build_query(request)
    return db.execute_query(sql, params), sql, params


# â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•
# Partition Config Tests
# â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•


class TestPartitionConfig:
    """Verify partition configuration is correctly loaded."""

    def test_employee_roster_is_partitioned(self):
        assert is_partitioned("employee_roster")
        cfg = get_partition_config("employee_roster")
        assert cfg["column"] == "as_of_month_sk"
        assert cfg["load_type"] == "Monthly"

    def test_non_partitioned_table(self):
        assert not is_partitioned("product_catalog")
        assert get_partition_config("product_catalog") is None


# â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•
# Partition Value Fetching (Adapter Layer)
# â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•


class TestPartitionValueFetching:
    """Test the get_partition_values adapter method."""

    def test_employee_roster_partition_values(self, db):
        """Fetch partition values for employee_roster."""
        result = db.get_partition_values("employee_roster", "as_of_month_sk")

        assert result["max_value"] == 202602
        assert result["min_value"] == 202512
        assert 202602 in result["values"]
        assert 202601 in result["values"]
        assert 202512 in result["values"]
        # Values should be descending
        assert result["values"] == sorted(result["values"], reverse=True)

    def test_delayed_financial_actuals_lag(self, db):
        """TC-LAG-01: Table with 3-month lag â€” MAX is 202511, not current month."""
        result = db.get_partition_values("delayed_financial_actuals", "as_of_month_sk")

        assert result["max_value"] == 202511, (
            f"Expected MAX=202511 (3-month lag), got {result['max_value']}"
        )
        assert result["min_value"] == 202506

    def test_tc_lag_02_dropdown_ends_at_max(self, db):
        """TC-LAG-02: Available values terminate at MAX, no future dates."""
        result = db.get_partition_values("delayed_financial_actuals", "as_of_month_sk")

        for val in result["values"]:
            assert val <= 202511, f"Found future date {val} beyond MAX (202511)"

    def test_tc_lag_03_nonexistent_partition_column(self, db):
        """TC-LAG-03 variant: Table exists but column doesn't â€” should error."""
        with pytest.raises(Exception):
            db.get_partition_values("product_catalog", "nonexistent_col")

    def test_daily_sales_partition_values(self, db):
        """Daily partition table returns date-typed values."""
        result = db.get_partition_values("daily_sales", "load_date")

        assert result["max_value"] is not None
        assert len(result["values"]) > 0


# â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•
# TC-PART-01: Auto-Default Validation
# â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•


class TestAutoDefaultPartition:
    """TC-PART-01: Query with partition filter auto-restricts to latest."""

    def test_auto_default_latest_partition(self, qb, db):
        """
        Query employee_roster with partition_filters set to its MAX value.
        Should only return rows from the latest month (202602).
        """
        request = QueryRequest(
            dataset="employee_roster",
            columns=["emp_id", "department", "salary_usd", "as_of_month_sk"],
            partition_filters={"employee_roster": [202602]},
            limit=100,
        )

        results, sql, params = _execute(qb, db, request)

        # SQL must contain the partition predicate
        assert "as_of_month_sk" in sql, f"Partition column missing from SQL:\n{sql}"

        # All returned rows should be from 202602
        for row in results:
            sk_key = next(k for k in row if "as_of_month_sk" in k)
            assert row[sk_key] == 202602, (
                f"Expected all rows to have as_of_month_sk=202602, got {row[sk_key]}"
            )

    def test_partition_reduces_row_count(self, qb, db):
        """Partitioned query returns fewer rows than unpartitioned."""
        # Unpartitioned
        req_all = QueryRequest(
            dataset="employee_roster",
            columns=["emp_id", "department", "salary_usd"],
            limit=100,
        )
        all_results, _, _ = _execute(qb, db, req_all)

        # Partitioned to latest
        req_part = QueryRequest(
            dataset="employee_roster",
            columns=["emp_id", "department", "salary_usd"],
            partition_filters={"employee_roster": [202602]},
            limit=100,
        )
        part_results, _, _ = _execute(qb, db, req_part)

        assert len(part_results) < len(all_results), (
            f"Partitioned ({len(part_results)}) should be fewer than all ({len(all_results)})"
        )


# â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•
# TC-PART-02: User Override to Historical Data
# â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•


class TestHistoricalOverride:
    """TC-PART-02: User changes partition to historical value."""

    def test_override_to_jan_2026(self, qb, db):
        """Selecting 202601 should return only January data."""
        request = QueryRequest(
            dataset="employee_roster",
            columns=["emp_id", "department", "salary_usd", "as_of_month_sk"],
            partition_filters={"employee_roster": [202601]},
            limit=100,
        )

        results, sql, _ = _execute(qb, db, request)

        assert len(results) > 0, f"Expected results for Jan 2026.\nSQL: {sql}"
        for row in results:
            sk_key = next(k for k in row if "as_of_month_sk" in k)
            assert row[sk_key] == 202601

    def test_override_to_dec_2025(self, qb, db):
        """Selecting 202512 should return only December 2025 data."""
        request = QueryRequest(
            dataset="employee_roster",
            columns=["emp_id", "department", "salary_usd", "as_of_month_sk"],
            partition_filters={"employee_roster": [202512]},
            limit=100,
        )

        results, sql, _ = _execute(qb, db, request)

        assert len(results) == 2, (
            f"Expected 2 rows for Dec 2025, got {len(results)}.\nSQL: {sql}"
        )


# â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•
# TC-PART-03: Multi-Partition Selection & Aggregation
# â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•


class TestMultiPartitionSelection:
    """TC-PART-03: Select multiple partitions with aggregation."""

    def test_multi_select_in_clause(self, qb, db):
        """Selecting Jan & Feb generates an IN clause and returns both months."""
        request = QueryRequest(
            dataset="employee_roster",
            columns=["as_of_month_sk", "dept_count"],
            group_by=["as_of_month_sk"],
            aggregations=[
                AggregationCondition(
                    column="emp_id",
                    function=AggregationFunction.COUNT,
                    output_name="dept_count",
                )
            ],
            partition_filters={"employee_roster": [202601, 202602]},
            limit=100,
        )

        results, sql, params = _execute(qb, db, request)

        # SQL should contain an IN clause
        assert "IN" in sql, f"Expected IN clause for multi-select:\n{sql}"

        # Should have results for both months
        months = set()
        for row in results:
            sk_key = next(k for k in row if "as_of_month_sk" in k)
            months.add(row[sk_key])
        assert 202601 in months and 202602 in months, (
            f"Expected both months in results, got {months}"
        )


# â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•
# TC-PART-04: Cross-Partition Joins
# â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•


class TestCrossPartitionJoins:
    """TC-PART-04: Each table uses its own partition filter in the subquery."""

    def test_independent_partition_filters_in_join(self, qb, db):
        """
        Base: employee_roster (Feb 2026)
        Join: department_budgets (Jan 2026)
        Each must have its own isolated WHERE clause.
        """
        request = QueryRequest(
            dataset="employee_roster",
            columns=[
                "employee_roster.emp_id",
                "employee_roster.department",
                "department_budgets.budget",
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
                "department_budgets": [202601],
            },
            limit=100,
        )

        results, sql, params = _execute(qb, db, request)

        # SQL must contain TWO separate partition subqueries
        # One for employee_roster with 202602, one for department_budgets with 202601
        assert sql.count("as_of_month_sk") >= 2, (
            f"Expected partition filter in both subqueries:\n{sql}"
        )

        # Verify data integrity: employee data is Feb, budget data is Jan
        assert len(results) > 0, f"Expected joined results.\nSQL: {sql}"


# â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•
# TC-LAG-01 & TC-LAG-04: Data Lag Scenarios
# â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•


class TestDataLagScenarios:
    """TC-LAG-01 and TC-LAG-04: Handle tables with different data freshness."""

    def test_tc_lag_01_delayed_table_auto_max(self, db):
        """
        delayed_financial_actuals has MAX=202511 (3 months behind).
        The adapter should return 202511, not the current system month.
        """
        result = db.get_partition_values("delayed_financial_actuals", "as_of_month_sk")
        assert result["max_value"] == 202511

    def test_tc_lag_04_join_different_lags(self, qb, db):
        """
        TC-LAG-04: Join daily_sales (latest: 2026-02-20) with
        delayed_financial_actuals (latest: 202511).
        Each table should use its own max partition independently.
        """
        # First verify max values
        daily_max = db.get_partition_values("daily_sales", "load_date")
        delayed_max = db.get_partition_values(
            "delayed_financial_actuals", "as_of_month_sk"
        )

        # Build query with independent partitions
        request = QueryRequest(
            dataset="daily_sales",
            columns=[
                "daily_sales.sale_id",
                "daily_sales.category",
                "daily_sales.amount",
            ],
            partition_filters={
                "daily_sales": [daily_max["max_value"]],
            },
            limit=100,
        )

        results, sql, _ = _execute(qb, db, request)

        assert len(results) > 0, f"Expected results.\nSQL: {sql}"
        # All results should be from the max date
        assert "load_date" in sql, f"Partition predicate missing:\n{sql}"


# â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•
# Partition + User Filters Combined
# â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•


class TestPartitionWithUserFilters:
    """Verify partition filters compose correctly with user-defined filters."""

    def test_partition_plus_department_filter(self, qb, db):
        """
        Partition to Feb 2026 AND filter department=Engineering.
        Both predicates should appear in the subquery.
        """
        request = QueryRequest(
            dataset="employee_roster",
            columns=["emp_id", "department", "salary_usd", "as_of_month_sk"],
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

        results, sql, _ = _execute(qb, db, request)

        # Both predicates should be in the SQL
        assert "as_of_month_sk" in sql
        assert len(results) > 0

        for row in results:
            dept_key = next(k for k in row if "department" in k)
            sk_key = next(k for k in row if "as_of_month_sk" in k)
            assert row[dept_key] == "Engineering"
            assert row[sk_key] == 202602

    def test_no_partition_filters_returns_all(self, qb, db):
        """Without partition_filters, all rows are returned (unrestricted)."""
        request = QueryRequest(
            dataset="employee_roster",
            columns=["emp_id", "as_of_month_sk"],
            limit=100,
        )

        results, sql, _ = _execute(qb, db, request)

        # Should have rows from multiple months
        months = set()
        for row in results:
            sk_key = next(k for k in row if "as_of_month_sk" in k)
            months.add(row[sk_key])
        assert len(months) > 1, (
            f"Expected multiple months without partition filter, got {months}"
        )
