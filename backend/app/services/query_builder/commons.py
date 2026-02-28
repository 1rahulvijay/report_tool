from typing import Dict, Any, Tuple
import re
from .base import SQLGenerationError


class CommonsMixin:
    """Utility methods for quoting, sanitizing, and resolving identifiers."""

    def _quote_identifier(self, identifier: str) -> str:
        """
        Safely quote a table or column name and normalize to UPPERCASE.
        Supports qualified identifiers like \"schema.table.column\" -> \"SCHEMA\".\"TABLE\".\"COLUMN\".
        Limits to at most 2 parts (alias.column) to avoid Oracle ORA-00904.
        """

        def quote(s):
            val = str(s).upper().replace('"', '""')
            return f'"{val}"'

        if "." in identifier:
            parts = [p for p in identifier.rsplit(".", 1) if p.strip()]
            if len(parts) == 0:
                return '""'
            if len(parts) == 1:
                return quote(parts[0])
            return f"{quote(parts[0])}.{quote(parts[1])}"

        return quote(identifier) if identifier.strip() else '""'

    def _sanitize_alias(self, alias: str, max_length: int = 50) -> str:
        """Sanitize a user-provided output alias."""
        sanitized = re.sub(r"[^a-zA-Z0-9_]", "_", str(alias))
        sanitized = sanitized.strip("_")
        sanitized = sanitized[:max_length]
        return sanitized if sanitized else "unnamed_metric"

    def _resolve_column_ref(
        self, col_ref: str, alias_map: dict, default_dataset: str
    ) -> Tuple[str, str]:
        """Resolve a column reference that may contain a dataset prefix."""
        if not col_ref:
            return default_dataset, ""

        parts = col_ref.split(".")
        if len(parts) == 1:
            return default_dataset, col_ref

        # Handle fully qualified names by matching suffixes in alias_map
        potential_dataset = ".".join(parts[:-1])
        if potential_dataset in alias_map:
            return potential_dataset, parts[-1]

        # Case-insensitive suffix matching for datasets
        upper_pd = potential_dataset.upper()
        for ds in alias_map:
            if ds.upper() == upper_pd or ds.upper().endswith("." + upper_pd):
                return ds, parts[-1]

        return potential_dataset, parts[-1]
