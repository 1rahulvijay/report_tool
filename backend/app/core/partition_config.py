"""
Partition Configuration — Maps datasets to their partition/load ID columns.

This config drives automatic predicate injection in the QueryBuilder,
ensuring queries are restricted to the latest data partition by default.

To register a new partitioned table, add an entry to PARTITION_MAP:
    "table_name": {
        "column": "partition_column_name",
        "load_type": "Daily" | "Monthly" | "Snapshot"
    }
"""

from typing import Optional, Dict, Any


# Dataset → Partition Column mapping
PARTITION_MAP: Dict[str, Dict[str, str]] = {
    "employee_roster": {
        "column": "hire_date",
        "load_type": "Monthly",
    },
    "department_budgets": {
        "column": "as_of_month_sk",
        "load_type": "Monthly",
    },
    "daily_sales": {
        "column": "load_date",
        "load_type": "Daily",
    },
    "delayed_financial_actuals": {
        "column": "as_of_month_sk",
        "load_type": "Monthly",
    },
}


def get_partition_config(dataset: str) -> Optional[Dict[str, Any]]:
    """Returns partition config for a dataset, or None if not partitioned."""
    return PARTITION_MAP.get(dataset.lower())


def is_partitioned(dataset: str) -> bool:
    """Quick check if a dataset has partition configuration."""
    return dataset.lower() in PARTITION_MAP
