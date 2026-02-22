"""
Partition Configuration — Maps datasets to their respective data vintage configurations.

This config drives automatic predicate injection in the QueryBuilder,
ensuring queries are restricted to the latest data partition by default,
as well as configuring the "Data Vintage" UI in the frontend.

To register a new partitioned table, add an entry to PARTITION_MAP:
    "table_name": {
        "load_type_column": "load_type",         # (Optional) Column indicating frequency (e.g., 'Daily', 'Monthly')
        "load_id_column": "partition_column",    # The actual column containing partition values
        "date_column": "display_date_column",    # Column used for visual context in the UI (e.g. 'hire_date')
        "supported_types": ["Monthly", "Daily"] # Array of allowed frequencies you expect the table to contain
    }
"""

from typing import Optional, Dict, Any
import json
import os

# Path to the external configuration file
CONFIG_PATH = os.path.join(
    os.path.dirname(os.path.dirname(os.path.dirname(__file__))), "partitions.json"
)

# In-memory cache for the configuration
_cached_config: Dict[str, Dict[str, Any]] = {}
_cached_mtime: float = 0.0


def _load_config() -> Dict[str, Dict[str, Any]]:
    """
    Loads the partition configuration from partitions.json.
    Caches the result and reloads only if the file's modification time changes.
    """
    global _cached_config, _cached_mtime

    if not os.path.exists(CONFIG_PATH):
        # Fallback to empty if not found, to avoid breaking the app
        print(f"Warning: Partition config not found at {CONFIG_PATH}")
        return {}

    try:
        current_mtime = os.path.getmtime(CONFIG_PATH)
        # Reload only if the file was modified since last read
        if current_mtime > _cached_mtime:
            with open(CONFIG_PATH, "r") as f:
                _cached_config = json.load(f)
            _cached_mtime = current_mtime

            # Normalize keys to lowercase for case-insensitive matching
            _cached_config = {k.lower(): v for k, v in _cached_config.items()}

    except Exception as e:
        print(f"Error loading partition config from {CONFIG_PATH}: {e}")
        # Keep using the old cache if there was a JSON parsing error

    return _cached_config


def get_partition_config(dataset: str) -> Optional[Dict[str, Any]]:
    """Returns partition config for a dataset, or None if not partitioned."""
    config_map = _load_config()
    return config_map.get(dataset.lower())


def is_partitioned(dataset: str) -> bool:
    """Quick check if a dataset has partition configuration."""
    config_map = _load_config()
    return dataset.lower() in config_map
