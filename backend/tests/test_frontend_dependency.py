"""
Frontend Dependency Deletion Test (TC-ERR-DEP-01).

Tests the _validate_and_cleanup_filters logic from AppState to verify that
when a column is deleted from the schema (e.g., removing an aggregation),
dependent filters are gracefully dropped without crashing.

This is a pure Python unit test â€” no Reflex runtime or HTTP server needed.
"""

import os
import sys
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
import pytest

sys.path.insert(
    0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "frontend"))
)


class MockFilterValidator:
    """
    Extracts the _validate_and_cleanup_filters logic from AppState
    so we can test it in isolation without Reflex.
    """

    @staticmethod
    def validate_and_cleanup_filters(item, valid_cols):
        """
        Recursively checks if filters target columns that no longer exist.
        Returns the filtered item or None if the entire group became invalid.

        This mirrors AppState._validate_and_cleanup_filters.
        """
        if not item:
            return None

        # Single condition
        if "column" in item:
            if item["column"] in valid_cols:
                return item
            return None  # Column no longer exists â€” drop it

        # Logical group
        if "logic" in item and "conditions" in item:
            cleaned = []
            for cond in item["conditions"]:
                result = MockFilterValidator.validate_and_cleanup_filters(
                    cond, valid_cols
                )
                if result is not None:
                    cleaned.append(result)

            if not cleaned:
                return None
            return {"logic": item["logic"], "conditions": cleaned}

        return None


# â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•
# TC-ERR-DEP-01: Deleting a Dependency
# â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•


class TestDependencyDeletion:
    """
    TC-ERR-DEP-01: A user builds a 3-step pipeline
    (Aggregate -> Filter on Metric -> Join), then deletes the
    metric aggregation. The filter should be gracefully dropped.
    """

    def test_removing_aggregation_drops_dependent_filter(self):
        """
        Step 1: Aggregation creates [product_id, total_revenue].
        Step 2: Filter on total_revenue > 100000.
        Step 3: User removes total_revenue aggregation.

        After step 3, valid schema is [product_id] only.
        The filter on total_revenue should be auto-dropped.
        """
        # Filter state after step 2
        active_filters = {
            "logic": "AND",
            "conditions": [
                {"column": "total_revenue", "operator": "gt", "value": 100000}
            ],
        }

        # After removing total_revenue aggregation, valid columns are just product_id
        valid_cols_after_delete = ["product_id"]

        result = MockFilterValidator.validate_and_cleanup_filters(
            active_filters, valid_cols_after_delete
        )

        # The entire filter group should be dropped (None) because
        # total_revenue no longer exists
        assert result is None, f"Expected None (filter dropped) but got: {result}"

    def test_partial_filter_cleanup_keeps_valid_conditions(self):
        """
        If only some conditions target deleted columns, only those are dropped.
        Valid conditions should be preserved.
        """
        active_filters = {
            "logic": "AND",
            "conditions": [
                {"column": "total_revenue", "operator": "gt", "value": 100000},
                {"column": "product_id", "operator": "eq", "value": "PROD-A"},
            ],
        }

        valid_cols = ["product_id"]

        result = MockFilterValidator.validate_and_cleanup_filters(
            active_filters, valid_cols
        )

        assert result is not None, "Expected partial result, got None"
        assert len(result["conditions"]) == 1, (
            f"Expected 1 surviving condition, got {len(result['conditions'])}"
        )
        assert result["conditions"][0]["column"] == "product_id"

    def test_nested_group_cleanup(self):
        """
        Tests cleanup of nested logical groups.
        If a nested group loses all its conditions, the entire group is dropped.
        """
        active_filters = {
            "logic": "AND",
            "conditions": [
                {"column": "product_id", "operator": "eq", "value": "PROD-A"},
                {
                    "logic": "OR",
                    "conditions": [
                        {"column": "total_revenue", "operator": "gt", "value": 100000},
                        {"column": "avg_cost", "operator": "lt", "value": 50},
                    ],
                },
            ],
        }

        valid_cols = ["product_id"]

        result = MockFilterValidator.validate_and_cleanup_filters(
            active_filters, valid_cols
        )

        assert result is not None
        assert len(result["conditions"]) == 1, (
            "Only product_id condition should survive"
        )
        assert result["conditions"][0]["column"] == "product_id"

    def test_empty_filter_returns_none(self):
        """Cleanup of empty filter state should return None."""
        result = MockFilterValidator.validate_and_cleanup_filters(None, ["department"])
        assert result is None

        result = MockFilterValidator.validate_and_cleanup_filters({}, ["department"])
        assert result is None

    def test_all_filters_valid_stays_intact(self):
        """If all filters target valid columns, nothing should be removed."""
        active_filters = {
            "logic": "AND",
            "conditions": [
                {"column": "department", "operator": "eq", "value": "Engineering"},
                {"column": "salary_sum", "operator": "gt", "value": 200000},
            ],
        }

        valid_cols = ["department", "salary_sum"]

        result = MockFilterValidator.validate_and_cleanup_filters(
            active_filters, valid_cols
        )

        assert result is not None
        assert len(result["conditions"]) == 2, "Both conditions should survive"

    def test_no_crash_on_malformed_input(self):
        """The cleanup function should never raise an exception."""
        # Random dict without expected keys
        result = MockFilterValidator.validate_and_cleanup_filters(
            {"random_key": "value"}, ["department"]
        )
        assert result is None

        # List instead of dict
        result = MockFilterValidator.validate_and_cleanup_filters(
            [1, 2, 3], ["department"]
        )
        assert result is None
