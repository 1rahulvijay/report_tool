"""
Table Configuration — Maps database table/column names to user-friendly display names.

This config drives:
  - Friendly table names in the sidebar/dropdown
  - Friendly column names in headers and filters
  - Per-table column whitelisting (only listed columns are shown in the UI)

To register a table mapping, add an entry to table_config.json:
    "SCHEMA.TABLE": {
        "display_name": "Friendly Name",
        "columns": {
            "COL_NAME": { "display_name": "Friendly Column" }
        }
    }

If a table has a "columns" key, ONLY those columns will be fetched by the UI.
If no "columns" key is present, all columns are shown.
"""

from typing import Optional, Dict, Any
import json
import os
from app.core.logger import logger

# Path to the external configuration file
CONFIG_PATH = os.path.join(
    os.path.dirname(os.path.dirname(os.path.dirname(__file__))), "table_config.json"
)

# In-memory cache
_cached_config: Dict[str, Any] = {}
_cached_mtime: float = 0.0


def _load_config() -> Dict[str, Any]:
    """
    Loads table configuration from table_config.json.
    Caches the result and reloads only if the file's modification time changes.
    """
    global _cached_config, _cached_mtime

    if not os.path.exists(CONFIG_PATH):
        return {}

    try:
        current_mtime = os.path.getmtime(CONFIG_PATH)
        if current_mtime > _cached_mtime:
            with open(CONFIG_PATH, "r") as f:
                raw = json.load(f)
            # Store the tables dict with case-insensitive keys
            tables = raw.get("tables", {})
            _cached_config = {k.upper(): v for k, v in tables.items()}
            _cached_mtime = current_mtime
    except Exception as e:
        logger.warning(f"Error loading table config from {CONFIG_PATH}: {e}")

    return _cached_config


def get_table_config(dataset: str) -> Optional[Dict[str, Any]]:
    """Returns config for a dataset, or None if not configured.
    Supports logical-to-physical name mapping.
    """
    config_map = _load_config()
    key = dataset.upper()

    # 1. Try exact match on logical key
    if key in config_map:
        return config_map[key]

    # 2. Try match on physical_name
    for cfg in config_map.values():
        if cfg.get("physical_name", "").upper() == key:
            return cfg

    # 3. Fallback: strip schema prefix
    if "." in key:
        table_only = key.split(".", 1)[1]
        for k, v in config_map.items():
            if k.endswith(f".{table_only}") or k == table_only:
                return v
    return None


def resolve_physical_name(dataset: str) -> str:
    """
    Resolves a logical dataset name to its physical database counterpart.
    If no 'physical_name' is configured, returns the input name as-is.
    """
    cfg = get_table_config(dataset)
    if cfg and cfg.get("physical_name"):
        return cfg["physical_name"].upper()
    return dataset.upper()


def get_table_display_name(dataset: str) -> str:
    """Returns the friendly display name, or the table-only part of the name as fallback."""
    cfg = get_table_config(dataset)
    if cfg and cfg.get("display_name"):
        return cfg["display_name"]
    # Default: strip schema prefix
    return dataset.split(".")[-1] if "." in dataset else dataset


def get_column_config(dataset: str) -> Optional[Dict[str, Dict[str, Any]]]:
    """
    Returns the column configuration for a dataset.
    If the table has a 'columns' key, returns that dict.
    Otherwise returns None (meaning: show all columns).
    """
    cfg = get_table_config(dataset)
    if cfg and "columns" in cfg:
        # Normalize column keys to uppercase
        return {k.upper(): v for k, v in cfg["columns"].items()}
    return None


def get_column_display_name(dataset: str, column_name: str) -> str:
    """Returns the friendly column name, or the raw name as fallback."""
    col_cfg = get_column_config(dataset)
    if col_cfg:
        key = column_name.upper()
        if key in col_cfg and col_cfg[key].get("display_name"):
            return col_cfg[key]["display_name"]
    return column_name


def resolve_physical_column_name(dataset: str, logical_column: str) -> str:
    """
    Resolves a logical column name to its physical database counterpart for a given dataset.
    """
    col_cfg = get_column_config(dataset)
    if col_cfg:
        key = logical_column.upper()
        if key in col_cfg and col_cfg[key].get("physical_name"):
            return col_cfg[key]["physical_name"].upper()
    return logical_column.upper()


def get_all_table_display_names() -> Dict[str, str]:
    """Returns a mapping of all configured dataset names to their display names."""
    config_map = _load_config()
    return {
        k: v.get("display_name", k.split(".")[-1])
        for k, v in config_map.items()
        if v.get("display_name")
    }
