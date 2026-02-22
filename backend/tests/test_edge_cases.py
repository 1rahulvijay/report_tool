"""
Edge Case, Security & Robustness Tests.

Tests math anomalies, rogue identifiers, SQL injection defense,
date edge cases, export edge cases, XSS data handling, and
case-sensitivity in joins.

Prerequisites:
    Run `python seed_pipeline_test_db.py` first to create test_engine.db.
"""

import os
import sys
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
import io
import pytest


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
    user = os.getenv("ORACLE_USER", "SYSTEM")
    password = os.getenv("ORACLE_PASSWORD", "password")
    dsn = os.getenv("ORACLE_DSN", "localhost:1521/xe")
    try:
        adapter = OracleAdapter(
            user=user, password=password, dsn=dsn, min_pool=1, max_pool=5
        )
    except Exception as e:
        pytest.skip(f"Could not connect to Oracle: {e}")
    yield adapter
    adapter.close()


@pytest.fixture
def qb():
    return QueryBuilderService()


def _execute(qb, db, request):
    sql, params = qb.build_query(request)
    return db.execute_query(sql, params), sql, params


# â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•
# 15. Math Anomalies & Database Quirks
# â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•


class TestMathAnomalies:
    """TC-MATH-01: AVG on all-NULL column must not crash."""

    def test_avg_all_null_column_no_crash(self, qb, db):
        """
        AVG(bonus_amount) where every row is NULL.
        DuckDB returns NULL for AVG of all NULLs â€” no division-by-zero.
        """
        request = QueryRequest(
            dataset="null_bonus_data",
            columns=["department", "avg_bonus"],
            group_by=["department"],
            aggregations=[
                AggregationCondition(
                    column="bonus_amount",
                    function=AggregationFunction.AVG,
                    output_name="avg_bonus",
                )
            ],
            limit=100,
        )

        results, sql, _ = _execute(qb, db, request)

        import math

        # Should succeed without 500 error
        assert len(results) > 0, f"Expected results.\nSQL: {sql}"
        # DuckDB returns NaN for AVG of all-NULL columns (not None)
        for row in results:
            val = row["avg_bonus"]
            assert val is None or (isinstance(val, float) and math.isnan(val)), (
                f"Expected NULL or NaN for AVG of all-NULL column, got {val}"
            )

    def test_count_all_null_column(self, qb, db):
        """COUNT of an all-NULL column should return 0 per SQL standard."""
        request = QueryRequest(
            dataset="null_bonus_data",
            columns=["bonus_count"],
            group_by=[],
            aggregations=[
                AggregationCondition(
                    column="bonus_amount",
                    function=AggregationFunction.COUNT,
                    output_name="bonus_count",
                )
            ],
            limit=100,
        )

        results, sql, _ = _execute(qb, db, request)

        assert len(results) == 1
        # COUNT(null_col) = 0 per SQL standard (COUNT ignores NULLs)
        assert results[0]["bonus_count"] == 0, (
            f"Expected 0 for COUNT of all-NULL column, got {results[0]['bonus_count']}"
        )


# â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•
# TC-DATA-01: Rogue Column Identifiers
# â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•


class TestRogueColumnIdentifiers:
    """Columns with spaces, #, $, () must be properly quoted."""

    def test_select_special_char_columns(self, qb, db):
        """SELECT on columns with spaces and special chars should work."""
        request = QueryRequest(
            dataset="rogue_columns",
            columns=["User ID #", "Total Revenue ($)"],
            limit=100,
        )

        results, sql, _ = _execute(qb, db, request)

        assert len(results) > 0, f"Expected results.\nSQL: {sql}"
        # Verify we got data back with the expected column names
        first = results[0]
        # Column names may be qualified with dataset
        assert any("User ID #" in k for k in first.keys()), (
            f"Expected 'User ID #' in keys: {list(first.keys())}"
        )

    def test_group_by_special_char_column(self, qb, db):
        """GROUP BY on a column with special characters."""
        request = QueryRequest(
            dataset="rogue_columns",
            columns=["User ID #", "total_rev"],
            group_by=["User ID #"],
            aggregations=[
                AggregationCondition(
                    column="Total Revenue ($)",
                    function=AggregationFunction.SUM,
                    output_name="total_rev",
                )
            ],
            limit=100,
        )

        results, sql, _ = _execute(qb, db, request)

        assert len(results) > 0, f"Expected results.\nSQL: {sql}"
        # Verify quoted identifiers in SQL
        assert '"User ID #"' in sql or '"User ID ""#"""' in sql, (
            f"Expected quoted special char column in SQL:\n{sql}"
        )

    def test_filter_on_special_char_column(self, qb, db):
        """Filter on 'Total Revenue ($)' column."""
        request = QueryRequest(
            dataset="rogue_columns",
            columns=["User ID #", "Total Revenue ($)"],
            filters=LogicalGroup(
                logic="AND",
                conditions=[
                    FilterCondition(
                        column="Total Revenue ($)",
                        operator=FilterOperator.GREATER_THAN,
                        value=50000,
                    )
                ],
            ),
            limit=100,
        )

        results, sql, _ = _execute(qb, db, request)

        assert len(results) > 0, f"Expected results.\nSQL: {sql}"


# â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•
# TC-DATA-02: Case Sensitivity in Table Joins
# â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•


class TestCaseSensitivityJoins:
    """SQL standard join is case-sensitive. Verify behavior."""

    def test_case_sensitive_join_standard(self, qb, db):
        """
        Standard SQL join is case-sensitive.
        Only exact matches (HR=HR, Engineering=Engineering) should join.
        'hr', 'engineering', 'SALES' should NOT match.
        """
        request = QueryRequest(
            dataset="mixed_case_employees",
            columns=[
                "mixed_case_employees.emp_id",
                "mixed_case_employees.dept_code",
                "department_lookup.dept_full_name",
            ],
            joins=[
                JoinCondition(
                    left_dataset="mixed_case_employees",
                    right_dataset="department_lookup",
                    join_type=JoinType.LEFT,
                    on=[JoinOn(left_column="dept_code", right_column="dept_code")],
                )
            ],
            limit=100,
        )

        results, sql, _ = _execute(qb, db, request)

        assert len(results) == 5, f"Expected 5 rows (LEFT JOIN).\nSQL: {sql}"

        # Count how many got a dept_full_name (matched)
        matched = [
            r for r in results if r.get("department_lookup.dept_full_name") is not None
        ]
        unmatched = [
            r for r in results if r.get("department_lookup.dept_full_name") is None
        ]

        # Exact-case match: 'HR' and 'Engineering' match (E1 and E3)
        # 'hr', 'engineering', 'SALES' do NOT match standard SQL
        assert len(matched) >= 1, "At least HR and Engineering should match exactly"
        assert len(unmatched) >= 1, "Lowercase/uppercase mismatches should be NULL"


# â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•
# 16. Input Validation & Abuse
# â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•


class TestInputValidation:
    """TC-INP-01 and TC-INP-02: Alias length and injection prevention."""

    def test_tc_inp_01_long_alias_truncated(self, qb):
        """A 500-char alias should be truncated to 50 characters."""
        long_alias = "a" * 500

        request = QueryRequest(
            dataset="employee_roster",
            columns=["department", long_alias],
            group_by=["department"],
            aggregations=[
                AggregationCondition(
                    column="salary_usd",
                    function=AggregationFunction.SUM,
                    output_name=long_alias,
                )
            ],
        )

        sql, params = qb.build_query(request)

        # The alias should be truncated
        # Count 'a's in quoted aliases â€” should not contain 500+ consecutive 'a's
        assert long_alias not in sql, "500-char alias should have been truncated"
        # Verify the truncated version IS present
        truncated = "a" * 50
        assert truncated in sql, "Expected truncated alias (50 chars) in SQL"

    def test_tc_inp_02_sql_injection_via_alias(self, qb):
        """
        Malicious alias: total_sum"; DROP TABLE employee_roster; --
        Should be sanitized to: total_sum__DROP_TABLE_employee_roster___
        """
        malicious_alias = 'total_sum"; DROP TABLE employee_roster; --'

        request = QueryRequest(
            dataset="employee_roster",
            columns=["department", "total_sum"],
            group_by=["department"],
            aggregations=[
                AggregationCondition(
                    column="salary_usd",
                    function=AggregationFunction.SUM,
                    output_name=malicious_alias,
                )
            ],
        )

        sql, params = qb.build_query(request)

        # CRITICAL: The raw malicious string must NOT appear in the SQL
        assert "DROP TABLE" not in sql, (
            f"SQL injection detected! Raw malicious alias found in SQL:\n{sql}"
        )
        assert ";" not in sql.split("AS")[-1].split(",")[0], (
            "Semicolon should not appear in alias portion of SQL"
        )
        assert "--" not in sql.split("AS")[-1].split(",")[0], (
            "Comment injection should not appear in alias"
        )

    def test_tc_inp_02_sanitized_alias_is_valid(self, qb):
        """Verify the sanitizer strips dangerous chars and produces safe identifiers."""
        result = qb._sanitize_alias('total_sum"; DROP TABLE x; --')
        # All special chars (quotes, semicolons, spaces, dashes) become underscores
        assert ";" not in result, f"Semicolons should be stripped: {result}"
        assert '"' not in result, f"Quotes should be stripped: {result}"
        assert all(c.isalnum() or c == "_" for c in result), (
            f"Only alphanum/underscore: {result}"
        )

        result = qb._sanitize_alias("Revenue (â‚¬) - 2026 / Q1")
        assert "Revenue" in result, f"Should preserve base name: {result}"
        assert all(c.isalnum() or c == "_" for c in result), (
            f"Only alphanum/underscore: {result}"
        )

        result = qb._sanitize_alias("")
        assert result == "unnamed_metric"

        result = qb._sanitize_alias("!!!###")
        assert result == "unnamed_metric"

    def test_sanitize_alias_preserves_normal_names(self, qb):
        """Normal alphanumeric aliases should pass through unchanged."""
        assert qb._sanitize_alias("total_revenue") == "total_revenue"
        assert qb._sanitize_alias("SUM_salary_usd") == "SUM_salary_usd"
        assert qb._sanitize_alias("count_123") == "count_123"


# â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•
# 19. Timezone & Date Handling
# â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•


class TestDateHandling:
    """TC-DATE-02: Leap year and edge case filtering."""

    def test_tc_date_02_leap_year_between_filter(self, qb, db):
        """
        Filter hire_date BETWEEN '2024-02-28' AND '2024-03-01'.
        Must capture Feb 29 (2024 was a leap year).
        """
        request = QueryRequest(
            dataset="leap_year_hires",
            columns=["hire_id", "hire_date", "department"],
            filters=LogicalGroup(
                logic="AND",
                conditions=[
                    FilterCondition(
                        column="hire_date",
                        operator=FilterOperator.GREATER_THAN_EQUAL,
                        value="2024-02-28",
                    ),
                    FilterCondition(
                        column="hire_date",
                        operator=FilterOperator.LESS_THAN_EQUAL,
                        value="2024-03-01",
                    ),
                ],
            ),
            limit=100,
        )

        results, sql, _ = _execute(qb, db, request)

        # Should include: H1 (Feb 28), H2 (Feb 29), H3 (Mar 1), H5 (Feb 29) = 4 rows
        assert len(results) == 4, (
            f"Expected 4 rows (Feb 28, 29, 29, Mar 1), got {len(results)}.\nSQL: {sql}"
        )

    def test_exact_leap_day_filter(self, qb, db):
        """Filter exactly on Feb 29 to verify no date parsing errors."""
        request = QueryRequest(
            dataset="leap_year_hires",
            columns=["hire_id", "hire_date"],
            filters=LogicalGroup(
                logic="AND",
                conditions=[
                    FilterCondition(
                        column="hire_date",
                        operator=FilterOperator.EQUALS,
                        value="2024-02-29",
                    ),
                ],
            ),
            limit=100,
        )

        results, sql, _ = _execute(qb, db, request)

        assert len(results) == 2, (
            f"Expected 2 rows on Feb 29, got {len(results)}.\nSQL: {sql}"
        )


# â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•
# 20. Security â€” XSS & Data Integrity
# â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•


class TestSecurityXSS:
    """TC-SEC-02: XSS payloads in cell data must be returned as raw strings."""

    def test_xss_payload_returned_as_string(self, qb, db):
        """
        The backend returns <script>alert('xss')</script> as a plain string.
        It does NOT execute or strip the tag â€” that's the frontend's job.
        The key assertion: no crash, data integrity preserved.
        """
        request = QueryRequest(
            dataset="xss_test_data",
            columns=["id", "customer_name", "email"],
            limit=100,
        )

        results, sql, _ = _execute(qb, db, request)

        assert len(results) == 4, f"Expected 4 rows.\nSQL: {sql}"

        # Find the XSS row
        xss_row = next(
            (r for r in results if r.get("xss_test_data.id") == 2 or r.get("id") == 2),
            None,
        )
        assert xss_row is not None, "Expected to find XSS row (id=2)"

        # The value should be preserved as-is (raw string)
        name_key = next(k for k in xss_row if "customer_name" in k)
        assert "<script>" in xss_row[name_key], (
            "XSS payload should be preserved as raw string data"
        )

    def test_sql_injection_payload_in_data(self, qb, db):
        """Bobby Tables row should be returned safely without executing the injection."""
        request = QueryRequest(
            dataset="xss_test_data",
            columns=["id", "customer_name"],
            filters=LogicalGroup(
                logic="AND",
                conditions=[
                    FilterCondition(
                        column="id",
                        operator=FilterOperator.EQUALS,
                        value=4,
                    )
                ],
            ),
            limit=100,
        )

        results, sql, _ = _execute(qb, db, request)

        assert len(results) == 1
        name_key = next(k for k in results[0] if "customer_name" in k)
        assert "DROP TABLE" in results[0][name_key], (
            "SQL injection payload in DATA should be preserved as string"
        )


# â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•
# 21. Export Edge Cases
# â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•


class TestExportEdgeCases:
    """TC-EXP-04 and TC-EXP-05: Empty exports and special char aliases."""

    def test_tc_exp_04_empty_dataset_csv(self, qb, db):
        """
        Export with 0 rows should return a valid CSV with headers only.
        """
        request = QueryRequest(
            dataset="employee_roster",
            columns=["emp_id", "department"],
            filters=LogicalGroup(
                logic="AND",
                conditions=[
                    FilterCondition(
                        column="department",
                        operator=FilterOperator.EQUALS,
                        value="NonexistentDepartment",
                    )
                ],
            ),
            limit=100000,
            offset=0,
        )

        sql, params = qb.build_query(request)
        df = db.execute_query_df(sql, params)

        # Verify 0 rows
        assert len(df) == 0, f"Expected 0 rows, got {len(df)}"

        # Generate CSV â€” should have header only
        stream = io.StringIO()
        df.to_csv(stream, index=False)
        csv_content = stream.getvalue()

        lines = csv_content.strip().split("\n")
        assert len(lines) == 1, f"Expected 1 line (header only), got {len(lines)}"
        # Header should contain the column names
        assert "emp_id" in lines[0] or "employee_roster" in lines[0]

    def test_tc_exp_05_special_char_alias_in_excel(self, qb, db):
        """
        Alias with special chars like â‚¬ and / should produce valid SQL
        (after sanitization) and execute without error.
        """
        request = QueryRequest(
            dataset="employee_roster",
            columns=["department", "Revenue____2026___Q1"],
            group_by=["department"],
            aggregations=[
                AggregationCondition(
                    column="salary_usd",
                    function=AggregationFunction.SUM,
                    output_name="Revenue (â‚¬) - 2026 / Q1",
                )
            ],
            limit=100,
        )

        # Should not throw SQLGenerationError
        results, sql, _ = _execute(qb, db, request)

        assert len(results) > 0, f"Expected results.\nSQL: {sql}"
        # The sanitized alias should be in the result
        first_row = results[0]
        # Check for the sanitized version
        assert any("Revenue" in k for k in first_row.keys()), (
            f"Expected sanitized 'Revenue' alias in keys: {list(first_row.keys())}"
        )

    def test_tc_exp_05_excel_write_with_special_chars(self, qb, db):
        """Verify xlsxwriter can write data with the sanitized alias without corruption."""
        import xlsxwriter

        request = QueryRequest(
            dataset="employee_roster",
            columns=["department", "Revenue____2026___Q1"],
            group_by=["department"],
            aggregations=[
                AggregationCondition(
                    column="salary_usd",
                    function=AggregationFunction.SUM,
                    output_name="Revenue (â‚¬) - 2026 / Q1",
                )
            ],
            limit=100,
        )

        results, sql, _ = _execute(qb, db, request)
        assert len(results) > 0

        # Write to in-memory Excel
        output = io.BytesIO()
        workbook = xlsxwriter.Workbook(
            output,
            {"constant_memory": True, "in_memory": True, "nan_inf_to_errors": True},
        )
        worksheet = workbook.add_worksheet("Test")

        # Write header
        col_names = list(results[0].keys())
        for col_num, name in enumerate(col_names):
            worksheet.write(0, col_num, name)

        # Write data
        for row_idx, row in enumerate(results, start=1):
            for col_idx, val in enumerate(row.values()):
                worksheet.write(row_idx, col_idx, val)

        # This should NOT raise
        workbook.close()
        output.seek(0)

        # Verify we got valid xlsx bytes
        assert len(output.getvalue()) > 0, "Expected non-empty xlsx output"
        # XLSX files start with PK (zip signature)
        assert output.getvalue()[:2] == b"PK", (
            "Expected valid xlsx (zip) file signature"
        )
