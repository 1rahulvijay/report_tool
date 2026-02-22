from typing import List, Dict, Any, Optional

from .column import ColumnState


class FilterState(ColumnState):
    """Manages advanced filtering logic and modal states."""

    def toggle_filter_modal(self):
        """Toggles the state using explicit button clicks."""
        self.is_filter_modal_open = not self.is_filter_modal_open

    def set_is_filter_modal_open(self, value: bool):
        """Sets the state using explicit boolean matching the radzen overlay open event."""
        self.is_filter_modal_open = value

    def _validate_and_cleanup_filters(
        self, item: Dict[str, Any], valid_cols: List[str]
    ) -> Optional[Dict[str, Any]]:
        """
        Recursively checks if filters target columns that no longer exist.
        Returns the filtered item or None if the entire group became invalid.
        """
        if not item:
            return None

        if item.get("type") == "group":
            new_conditions = []
            for cond in item.get("conditions", []):
                cleaned = self._validate_and_cleanup_filters(cond, valid_cols)
                if cleaned:
                    new_conditions.append(cleaned)

            if not new_conditions:
                return None  # Drop empty groups

            return {**item, "conditions": new_conditions}
        else:
            # Rule type
            col = item.get("column", "")
            if col in valid_cols:
                return item
            return None  # Drop the rule if column is missing

    def _get_translated_filters(self) -> Optional[Dict[str, Any]]:
        """Recursively maps UI friendly operators to Backend API Enum values."""
        if not self.active_filters or not self.active_filters.get("conditions"):
            return None

        return self._translate_recursive(self.active_filters)

    def _translate_recursive(self, item: Dict[str, Any]) -> Dict[str, Any]:
        """Recursive helper for filter translation."""
        if item.get("type") == "group":
            # It's a group
            return {
                "logic": item["logic"],
                "conditions": [
                    self._translate_recursive(c) for c in item["conditions"]
                ],
            }
        else:
            # It's a rule
            op_map = {
                "=": "eq",
                "!=": "neq",
                "equal": "eq",
                "not_equal": "neq",
                "not equal": "neq",
                ">": "gt",
                "<": "lt",
                ">=": "gte",
                "<=": "lte",
                "in": "in",
                "contains": "contains",
                "not contains": "not_contains",
                "starts with": "starts_with",
                "ends with": "ends_with",
                "is empty": "is_empty",
                "is not empty": "is_not_empty",
                "is null": "is_null",
                "is not null": "is_not_null",
            }
            backend_op = op_map.get(item.get("operator", "="), "eq")
            value = item.get("value", "")

            # TC-API-03: Sanitize the Payload in Reflex
            # For unary operators, ensure the value is explicitly None to avoid ghost validation errors
            unary_ops = ["is_empty", "is_not_empty", "is_null", "is_not_null"]
            if backend_op in unary_ops:
                value = None

            # TC-DAT-01: Auto-split 'between' operator if it arrives as a comma-separated string
            if backend_op == "between" and isinstance(value, str):
                if "," in value:
                    value = [v.strip() for v in value.split(",")]
                else:
                    value = [value, value]  # fallback if they only select one date

            # TC-PIPE-01: Auto-detect numeric values for all columns (including derived/joined)
            # UI text inputs always send strings; convert to float/int if strictly numeric
            if value is not None and isinstance(value, str) and value.strip():
                val_str = value.strip()
                # Prevent casting strings with leading zeros (e.g. employee IDs like '007')
                # except exactly "0" or floats like "0.5" or "-0.5"
                is_zero_padded = (
                    len(val_str) > 1
                    and val_str.startswith("0")
                    and not val_str.startswith("0.")
                )
                is_neg_zero_padded = (
                    len(val_str) > 2
                    and val_str.startswith("-0")
                    and not val_str.startswith("-0.")
                )

                # If the user chose a text-only operator, force it to remain a string
                text_operators = [
                    "contains",
                    "not_contains",
                    "starts_with",
                    "ends_with",
                ]

                if (
                    not (is_zero_padded or is_neg_zero_padded)
                    and backend_op not in text_operators
                ):
                    try:
                        # Try int first, fallback to float
                        if "." in val_str:
                            value = float(val_str)
                        else:
                            value = int(val_str)
                    except (ValueError, TypeError):
                        pass  # Leave as string if conversion fails

            return {
                "column": item.get("column", ""),
                "datatype": item.get("datatype", "string"),
                "operator": backend_op,
                "value": value,
            }
