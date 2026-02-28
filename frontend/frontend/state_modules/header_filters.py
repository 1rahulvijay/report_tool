from typing import List, Dict, Any
import reflex as rx
from .column import ColumnState


class HeaderFilterState(ColumnState):
    """Manages simple top-row header filters."""

    # Store simple k:v text filters directly tied to columns
    header_filters: Dict[str, str] = {}

    def set_header_filter(self, col_name: str, value: str):
        """Bind header filter string value for a specific column."""
        if value:
            self.header_filters[col_name] = value
        elif col_name in self.header_filters:
            del self.header_filters[col_name]

    @rx.var
    def has_active_header_filters(self) -> bool:
        """Returns True if there are any active inline header filters."""
        return len(self.header_filters) > 0

    def _execute_header_filters(
        self, lookup_map: dict, normalized_lookup: dict
    ) -> List[Dict[str, Any]]:
        """
        Parses the active text-based header filters against the table schema
        and generates a backend-compliant list of filter conditions.
        """
        header_conditions = []
        for col, text in self.header_filters.items():
            if not text or not text.strip():
                continue

            col_upper = col.upper()

            def normalize(s):
                return s.upper().replace(" ", "").replace("_", "").replace(".", "")

            col_info = lookup_map.get(col_upper) or normalized_lookup.get(
                normalize(col_upper)
            )

            if not col_info:
                for k, v in lookup_map.items():
                    if k.endswith(f".{col_upper}"):
                        col_info = v
                        break
                    # TC-COL-01: Support mapping from user-friendly display name back to technical name
                    if v.get("display_name", "").upper() == col_upper:
                        col_info = v
                        break

            if col_info:
                raw_datatype = str(
                    col_info.get("base_type", col_info.get("type", "string"))
                ).lower()
                val_text = text.strip()

                if any(
                    t in raw_datatype
                    for t in (
                        "number",
                        "integer",
                        "int",
                        "float",
                        "numeric",
                        "double",
                        "decimal",
                        "dec",
                    )
                ):
                    value = val_text
                    operator = "starts_with"
                    datatype = "number"
                elif any(t in raw_datatype for t in ("date", "time", "stamp")):
                    # Support Date Picker ranges (e.g. "2021-01-01 to 2021-12-31")
                    import re

                    if " to " in val_text.lower():
                        value = [v.strip() for v in re.split(r"(?i)\s+to\s+", val_text)]
                        operator = "between"
                    elif "," in val_text:
                        value = [v.strip() for v in val_text.split(",")]
                        operator = "between"
                    else:
                        value = val_text
                        operator = "contains"
                    datatype = "date"
                else:
                    value = val_text
                    operator = "contains"
                    datatype = "string"

                column_name = col_info["name"]
                if "." not in column_name and getattr(self, "selected_dataset", None):
                    column_name = f"{self.selected_dataset}.{column_name}"

                header_conditions.append(
                    {
                        "type": "rule",
                        "column": column_name.upper(),  # Force Canonical UPPERCASE matching
                        "datatype": datatype,
                        "operator": operator,
                        "value": value,
                    }
                )

        return header_conditions
