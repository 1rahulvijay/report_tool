from typing import List, Dict, Any, Optional

from .header_filters import HeaderFilterState


class FilterState(HeaderFilterState):
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
                "type": "group",
                "logic": item["logic"],
                "conditions": [
                    self._translate_recursive(c) for c in item["conditions"]
                ],
            }
        else:
            # It's a rule
            print(
                f"[TRANSLATE DEBUG] Raw rule: column={item.get('column')}, op={item.get('operator')}, datatype={item.get('datatype')}, value={repr(item.get('value'))}"
            )
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
                "max": "lte",
                "min": "gte",
                "in": "in",
                "not_in": "not_in",
                "between": "between",
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

            # TC-DAT-01: Auto-split 'between' operator if it arrives as a comma-separated or " TO " string
            if backend_op == "between" and isinstance(value, str):
                import re

                if " to " in value.lower():
                    value = [v.strip() for v in re.split(r"(?i)\s+to\s+", value)]
                elif "," in value:
                    value = [v.strip() for v in value.split(",")]
                else:
                    value = [value, value]  # fallback if they only select one date

            # TC-PIPE-01: Auto-detect numeric values for all columns (including derived/joined)
            # UI text inputs always send strings; convert to float/int if strictly numeric
            # IMPORTANT: Skip numeric casting for IN/NOT_IN — the backend handles splitting
            column_name = item.get("column", "")
            datatype = item.get("datatype", "string")

            # Only cast if the UI explicitly bound this to a Number column.
            # Otherwise, keep it as a string to prevent Dates (e.g., '2020') from turning into integers.
            if (
                value is not None
                and backend_op not in ["in", "not_in"]
                and datatype == "number"
            ):

                def try_cast(val_str):
                    if not isinstance(val_str, str):
                        return val_str
                    val_str = val_str.strip()
                    if not val_str:
                        return val_str
                    try:
                        if "." in val_str:
                            return float(val_str)
                        return int(val_str)
                    except (ValueError, TypeError):
                        return val_str

                if isinstance(value, str):
                    value = try_cast(value)
                elif isinstance(value, list):
                    value = [try_cast(str(v)) if v is not None else v for v in value]

            column_name = item.get("column", "")

            # Phase 1: Oracle Case Sensitivity & Friendly Name Mapping
            # Attempt to resolve the canonical UPPERCASE name from the frontend metadata
            try:
                # We need access to the current state instance, but we are in a mixin/subclass.
                # Assuming `self` has access to `_get_column_metadata_map()` from `BaseState`
                meta_map = self._get_column_metadata_map()

                # Check for exact match first
                if column_name and column_name not in meta_map:
                    # Reverse lookup: find the canonical key where the display_name matches
                    for qualified_key, meta in meta_map.items():
                        if meta.get("display_name") == column_name:
                            column_name = meta.get("name")  # Use the canonical name
                            break
                        elif meta.get("name") == column_name.upper():
                            column_name = meta.get("name")
                            break
            except Exception:
                pass  # Fallback to original if map lookup fails

            return {
                "type": "rule",
                "column": column_name.upper() if column_name else "",
                "datatype": item.get("datatype", "string"),
                "operator": backend_op,
                "value": value,
            }

    def _flatten_path(self, raw_path: Any) -> List[int]:
        """Flattens arbitrarily nested lists of path indices received from the frontend."""
        if isinstance(raw_path, tuple):
            return list(raw_path)
        elif isinstance(raw_path, int):
            return [raw_path]
        elif isinstance(raw_path, str):
            try:
                val = int(raw_path)
                return [val]
            except ValueError:
                return []
        elif isinstance(raw_path, list):
            flat = []
            for item in raw_path:
                flat.extend(self._flatten_path(item))
            return flat
        return []

    async def add_filter_rule(self, raw_path: Any):
        """Adds a simple rule to the group at the specified path."""
        import copy

        path = self._flatten_path(raw_path)
        new_filters = copy.deepcopy(self.active_filters)
        target = self._get_group_at_path(new_filters, path)
        new_rule = {
            "type": "rule",
            "column": "",
            "datatype": "string",
            "operator": "=",
            "value": "",
        }
        target["conditions"].append(new_rule)
        self.active_filters = new_filters
        yield

    async def add_filter_group(self, raw_path: Any):
        """Adds a nested logical group to the group at the specified path."""
        import copy

        path = self._flatten_path(raw_path)
        new_filters = copy.deepcopy(self.active_filters)
        target = self._get_group_at_path(new_filters, path)
        new_group = {"type": "group", "logic": "AND", "conditions": []}
        target["conditions"].append(new_group)
        self.active_filters = new_filters
        yield

    async def remove_filter_item(self, raw_path: Any):
        """Removes a rule or group at the specified path."""
        import copy

        path = self._flatten_path(raw_path)
        if not path:
            return  # Cannot remove root group

        new_filters = copy.deepcopy(self.active_filters)
        parent_path = path[:-1]
        index = path[-1]
        parent = self._get_group_at_path(new_filters, parent_path)

        if "conditions" in parent and 0 <= index < len(parent["conditions"]):
            parent["conditions"].pop(index)
            self.active_filters = new_filters
        yield

    async def update_filter_item(self, raw_path: Any, field: str, value: Any):
        """Updates a specific property of a rule at a path."""
        import copy

        path = self._flatten_path(raw_path)
        new_filters = copy.deepcopy(self.active_filters)
        parent_path = path[:-1]
        index = path[-1]
        parent = self._get_group_at_path(new_filters, parent_path)

        if "conditions" in parent and 0 <= index < len(parent["conditions"]):
            if field == "column":
                # Determine datatype from frontend metadata cache for consistent defaulting
                # Try to use display name or canonical name
                meta_map = self._get_column_metadata_map()
                col_type = "string"
                for qualified_name, meta in meta_map.items():
                    if (
                        qualified_name == value
                        or meta.get("name") == value
                        or meta.get("display_name") == value
                    ):
                        raw_type = str(
                            meta.get("base_type", meta.get("type", "string"))
                        ).lower()
                        if any(t in raw_type for t in ["num", "int", "float", "dec"]):
                            col_type = "number"
                        elif any(t in raw_type for t in ["date", "time", "stamp"]):
                            col_type = "date"
                        break

                parent["conditions"][index]["datatype"] = col_type

                if col_type == "number":
                    parent["conditions"][index]["operator"] = "="
                elif col_type == "date":
                    parent["conditions"][index]["operator"] = "between"
                else:
                    parent["conditions"][index]["operator"] = "="
                parent["conditions"][index]["value"] = ""

            parent["conditions"][index][field] = value

            # Ghost value wipe for unary ops
            unary_ops = ["is null", "is not null", "is empty", "is not empty"]
            if field == "operator" and value in unary_ops:
                parent["conditions"][index]["value"] = ""

            self.active_filters = new_filters
        yield

    async def update_filter_between_date(self, raw_path: Any, part: str, value: str):
        """Updates the start or end of a between date range value (comma-separated)."""
        import copy

        path = self._flatten_path(raw_path)
        new_filters = copy.deepcopy(self.active_filters)
        parent_path = path[:-1]
        index = path[-1]
        parent = self._get_group_at_path(new_filters, parent_path)

        if "conditions" in parent and 0 <= index < len(parent["conditions"]):
            current_value = parent["conditions"][index].get("value", "")
            parts = (
                current_value.split(",")
                if "," in current_value
                else [current_value, ""]
            )
            if len(parts) < 2:
                parts.append("")

            # The key fix here: handle clearing/resetting dates properly
            val_to_insert = value.strip() if value else ""

            if part == "start":
                parts[0] = val_to_insert
            else:
                parts[1] = val_to_insert

            parent["conditions"][index]["value"] = f"{parts[0]},{parts[1]}"
            print(
                f"[BETWEEN DATE DEBUG] part={part}, input_val={value}, stored_value={parent['conditions'][index]['value']}, column={parent['conditions'][index].get('column')}"
            )
            self.active_filters = new_filters
        yield

    async def set_filter_logic(self, raw_path: Any, val: str):
        """Sets the logic (AND/OR) for a group at a path."""
        import copy

        path = self._flatten_path(raw_path)
        new_filters = copy.deepcopy(self.active_filters)
        target = self._get_group_at_path(new_filters, path)
        target["logic"] = "OR" if val in ["Match ANY", "OR"] else "AND"
        self.active_filters = new_filters
        yield

    def _get_group_at_path(
        self, root: Dict[str, Any], path: List[int]
    ) -> Dict[str, Any]:
        """Helper to navigate the recursive structure using a list of indices."""
        curr = root
        for idx in path:
            curr = curr["conditions"][idx]
        return curr

    async def clear_filters(self):
        """Resets the advanced filters and queries."""
        self.active_filters = {"type": "group", "logic": "AND", "conditions": []}
        yield
        from frontend.state import AppState

        yield AppState.execute_query(force=True)

    async def apply_filters(self):
        """Closes the modal and executes the query."""
        self.is_filter_modal_open = False
        self.page_number = 1
        self.query_results = []
        yield
        from frontend.state import AppState

        yield AppState.execute_query(force=True)
