import os
from typing import List, Dict, Any
import reflex as rx
import httpx
import copy
from .filter import FilterState

# The base URL where our FastAPI backend is running
API_BASE_URL = os.getenv("API_BASE_URL", "http://localhost:8080/api/v1")


class JoinState(FilterState):
    """Handles the complexities of multi-table joins and data merging."""

    OPERATOR_MAP: Dict[str, List[str]] = {
        "number": [
            "=",
            "!=",
            "<",
            ">",
            "<=",
            ">=",
            "max",
            "min",
            "between",
            "in",
            "is null",
            "is not null",
        ],
        "string": [
            "=",
            "!=",
            "contains",
            "not contains",
            "starts with",
            "ends with",
            "in",
            "is null",
            "is not null",
        ],
        "date": [
            "=",
            "!=",
            "<",
            ">",
            "<=",
            ">=",
            "max",
            "min",
            "between",
            "is null",
            "is not null",
        ],
    }

    @staticmethod
    def _display_name(qualified: str) -> str:
        """Strip schema.table prefix: 'MGBCM.REAL_DATA_1.COL' -> 'COL', 'table.col' -> 'col'."""
        return qualified.split(".")[-1] if "." in qualified else qualified

    @staticmethod
    def _display_table_name(qualified: str) -> str:
        """Strip schema prefix: 'MGBCM.REAL_DATA_1' -> 'REAL_DATA_1'."""
        return qualified.split(".")[-1] if "." in qualified else qualified

    def toggle_join_modal(self):
        """Toggles the join modal visibility."""
        if not self.is_join_modal_open:
            # Reset transient state when opening
            self.new_join_left_dataset = (
                self.joins[-1]["right_dataset"] if self.joins else self.selected_dataset
            )
            self.new_join_right_dataset = ""
            self.new_join_type = "inner"
            self.new_join_conditions = [{"left_column": "", "right_column": ""}]
            # Reset search fields
            self.join_table_search = ""
            self.join_left_col_search = ""
            self.join_right_col_search = ""
        self.is_join_modal_open = not self.is_join_modal_open

    def add_join_condition(self):
        """Adds a new blank condition row to the join builder."""
        self.new_join_conditions.append({"left_column": "", "right_column": ""})

    def remove_join_condition(self, index: int):
        """Removes a condition row at the specific index."""
        if len(self.new_join_conditions) > 1:
            self.new_join_conditions.pop(index)

    @rx.var
    def raw_column_names(self) -> List[str]:
        """
        Returns the physical columns available in the current set (Primary + Joins).
        Crucial for Aggregation Builder source selection to avoid self-referential loops.
        """
        if not self.selected_dataset:
            return []

        all_qualified = [
            f"{self.selected_dataset}.{c['name']}"
            for c in self._dataset_column_cache.get(self.selected_dataset, [])
        ]

        for join in self.joins:
            r_dataset = join["right_dataset"]
            all_qualified.extend(
                [
                    f"{r_dataset}.{c['name']}"
                    for c in self._dataset_column_cache.get(r_dataset, [])
                ]
            )

        return sorted(list(set(all_qualified)))

    @rx.var
    def numeric_column_names(self) -> List[str]:
        """
        Returns only the numeric physical columns for aggregation source selection.
        """
        if not self.selected_dataset:
            return []

        cols = []
        # Check primary dataset
        primary_cols = self._dataset_column_cache.get(self.selected_dataset, [])
        for c in primary_cols:
            if c.get("base_type") == "numeric":
                cols.append(f"{self.selected_dataset}.{c['name']}")

        # Check joins
        for join in self.joins:
            r_ds = join["right_dataset"]
            r_cols = self._dataset_column_cache.get(r_ds, [])
            for c in r_cols:
                if c.get("base_type") == "numeric":
                    cols.append(f"{r_ds}.{c['name']}")

        return sorted(list(set(cols)))

    @rx.var
    def all_column_names_for_agg(self) -> List[str]:
        """Returns ALL physical columns (any type) for distinct_count on string/text fields."""
        return self.raw_column_names

    @rx.var
    def join_anchor_datasets(self) -> List[str]:
        """Returns the list of tables already in the query that can act as join anchors."""
        if not self.selected_dataset:
            return []
        anchors = [self.selected_dataset]
        for join in self.joins:
            anchors.append(join["right_dataset"])
        return anchors

    @rx.var
    def left_side_column_names(self) -> List[str]:
        """Returns qualified names of all columns from the selected left anchor dataset."""
        ds = (
            self.new_join_left_dataset
            if self.new_join_left_dataset
            else self.selected_dataset
        )
        cols = self._dataset_column_cache.get(ds, [])
        return [f"{ds}.{c['name']}" for c in cols]

    @rx.var
    def right_side_column_names(self) -> List[str]:
        """Returns columns for the dataset currently being joined (Right side)."""
        if not self.new_join_right_dataset:
            return []
        cols = self._dataset_column_cache.get(self.new_join_right_dataset, [])
        return sorted([f"{self.new_join_right_dataset}.{c['name']}" for c in cols])

    # ─── Filtered Search Vars ──────────────────────────────

    @rx.var
    def filtered_join_datasets(self) -> List[str]:
        """Filters dataset names for the join builder table search."""
        names = sorted([ds.get("name", "") for ds in self.datasets])
        if not self.join_table_search:
            return names
        s = self.join_table_search.lower()
        return [n for n in names if s in n.lower()]

    @rx.var
    def filtered_left_col_names(self) -> List[str]:
        """Left-side column names filtered by left column search."""
        cols = self.left_side_column_names
        if not self.join_left_col_search:
            return cols
        s = self.join_left_col_search.lower()
        return [c for c in cols if s in c.lower()]

    @rx.var
    def filtered_right_col_names(self) -> List[str]:
        """Right-side column names filtered by right column search."""
        cols = self.right_side_column_names
        if not self.join_right_col_search:
            return cols
        s = self.join_right_col_search.lower()
        return [c for c in cols if s in c.lower()]

    @rx.var
    def filtered_group_by_columns(self) -> List[str]:
        """Raw column names filtered by group-by search."""
        cols = self.raw_column_names
        if not self.agg_group_by_search:
            return cols
        s = self.agg_group_by_search.lower()
        return [c for c in cols if s in c.lower()]

    @rx.var
    def filtered_numeric_columns(self) -> List[str]:
        """Numeric column names filtered by metrics search."""
        cols = self.numeric_column_names
        if not self.agg_metrics_search:
            return cols
        s = self.agg_metrics_search.lower()
        return [c for c in cols if s in c.lower()]

    @rx.var
    def filtered_all_agg_columns(self) -> List[str]:
        """All column names filtered by metrics search (for distinct_count on any type)."""
        cols = self.all_column_names_for_agg
        if not self.agg_metrics_search:
            return cols
        s = self.agg_metrics_search.lower()
        return [c for c in cols if s in c.lower()]

    @rx.var
    def filtered_filter_columns(self) -> List[str]:
        """Column names filtered by filter column search."""
        names = [c["name"] for c in self.columns]
        if not self.filter_col_search:
            return names
        s = self.filter_col_search.lower()
        return [n for n in names if s in n.lower()]

    # ─── Display-Pair Vars (for component dropdowns) ─────────
    # Each returns List[List[str]] where inner list is [full_name, display_name]

    @staticmethod
    def _to_pairs(names: list) -> list:
        """Convert list of qualified names to [[full, display], ...]."""
        return [[n, n.split(".")[-1] if "." in n else n] for n in names]

    @rx.var
    def join_anchor_display(self) -> List[List[str]]:
        """Join anchor datasets as [full_name, display_name] pairs."""
        return self._to_pairs(self.join_anchor_datasets)

    @rx.var
    def filtered_join_datasets_display(self) -> List[List[str]]:
        """Filtered join datasets as [full_name, display_name] pairs."""
        return self._to_pairs(self.filtered_join_datasets)

    @rx.var
    def filtered_left_col_display(self) -> List[List[str]]:
        """Filtered left column names as [full_name, display_name] pairs."""
        return self._to_pairs(self.filtered_left_col_names)

    @rx.var
    def filtered_right_col_display(self) -> List[List[str]]:
        """Filtered right column names as [full_name, display_name] pairs."""
        return self._to_pairs(self.filtered_right_col_names)

    @rx.var
    def filtered_group_by_display(self) -> List[List[str]]:
        """Filtered group-by columns as [full_name, display_name] pairs."""
        return self._to_pairs(self.filtered_group_by_columns)

    @rx.var
    def filtered_all_agg_display(self) -> List[List[str]]:
        """Filtered all-agg columns as [full_name, display_name] pairs."""
        return self._to_pairs(self.filtered_all_agg_columns)

    @rx.var
    def filtered_filter_col_display(self) -> List[List[str]]:
        """Filtered filter columns as [full_name, display_name] pairs."""
        return self._to_pairs(self.filtered_filter_columns)

    @rx.var
    def preview_column_names(self) -> List[str]:
        """Returns keys for the preview data set."""
        if not self.join_preview_data:
            return []
        return list(self.join_preview_data[0].keys())

    def toggle_join_preview(self):
        self.is_join_preview_modal_open = not self.is_join_preview_modal_open
        if not self.is_join_preview_modal_open:
            self.join_preview_data = []

    async def fetch_join_preview(self):
        """Fetches a small sample of the CURRENT join configuration for preview."""
        if not self.selected_dataset or not self.new_join_right_dataset:
            return

        self.error_message = ""

        # Build transient join for preview
        l_ds = (
            self.new_join_left_dataset
            if self.new_join_left_dataset
            else self.selected_dataset
        )
        preview_join = {
            "left_dataset": l_ds,
            "right_dataset": self.new_join_right_dataset,
            "join_type": self.new_join_type,
            "on": self.new_join_conditions,
        }

        # Build strict column requirement for pruning safeguard
        preview_cols = []
        for c in self._dataset_column_cache.get(self.selected_dataset, []):
            preview_cols.append(f"{self.selected_dataset}.{c['name']}")
        for c in self._dataset_column_cache.get(self.new_join_right_dataset, []):
            preview_cols.append(f"{self.new_join_right_dataset}.{c['name']}")

        # Payload for a limited preview
        payload = {
            "dataset": self.selected_dataset,
            "columns": preview_cols,  # Strict column pruning explicitly blocks SELECT *
            "joins": self.joins + [preview_join],
            "limit": 10,
            "offset": 0,
            "filters": None,
            "column_metadata": self._get_column_metadata_map(),
            "partition_filters": self._get_partition_filters(),
        }

        try:
            async with httpx.AsyncClient() as client:
                res = await client.post(f"{API_BASE_URL}/query/preview", json=payload)
                res.raise_for_status()
                data = res.json()
                self.join_preview_data = data.get("data", [])
                self.is_join_preview_modal_open = True
        except Exception as e:
            self.error_message = f"Preview Failed: {str(e)}"

    async def set_new_join_right_dataset(self, value: str):
        self.new_join_right_dataset = value
        if value and value not in self._dataset_column_cache:
            try:
                async with httpx.AsyncClient() as client:
                    res = await client.get(f"{API_BASE_URL}/datasets/{value}/columns")
                    res.raise_for_status()
                    cols = res.json().get("columns", [])
                    self._dataset_column_cache[value] = cols
            except Exception as e:
                self.error_message = f"Failed to load columns for {value}: {str(e)}"

    def update_new_join_condition(self, index: int, field: str, value: str):
        """Updates a specific field (left_column or right_column) in a new join condition."""
        new_conditions = copy.deepcopy(self.new_join_conditions)
        if 0 <= index < len(new_conditions):
            new_conditions[index][field] = value
            self.new_join_conditions = new_conditions

    async def apply_join(self):
        """Finalizes the join configuration and refreshes the data."""
        if not self.new_join_right_dataset:
            self.error_message = "Please select a table to join."
            return

        valid_conditions = [
            c
            for c in self.new_join_conditions
            if c["left_column"] and c["right_column"]
        ]
        if not valid_conditions:
            self.error_message = "Please add at least one join condition."
            return

        # TC-JOIN-03: Data Type Protection
        # We need to verify that columns being joined have compatible types
        l_ds = (
            self.new_join_left_dataset
            if self.new_join_left_dataset
            else self.selected_dataset
        )
        left_cols = {
            c["name"]: c.get("type", "string")
            for c in self._dataset_column_cache.get(l_ds, [])
        }
        right_cols = {
            c["name"]: c.get("type", "string")
            for c in self._dataset_column_cache.get(self.new_join_right_dataset, [])
        }

        for cond in valid_conditions:
            l_col = cond["left_column"].split(".")[-1]
            r_col = cond["right_column"].split(".")[-1]
            l_type = left_cols.get(l_col, "string")
            r_type = right_cols.get(r_col, "string")

            # Simple compatibility check: numeric vs non-numeric
            is_l_num = l_type.lower() in [
                "int",
                "float",
                "decimal",
                "number",
                "integer",
                "double",
            ]
            is_r_num = r_type.lower() in [
                "int",
                "float",
                "decimal",
                "number",
                "integer",
                "double",
            ]

            if is_l_num != is_r_num:
                self.error_message = f"Type Mismatch: Cannot join {l_type} ({l_col}) to {r_type} ({r_col})."
                return

        await self.add_join(
            self.new_join_right_dataset,
            self.new_join_type,
            valid_conditions,
        )
        self.is_join_modal_open = False
        from frontend.state import AppState

        yield AppState.execute_query(force=True)

    async def reset_joins(self):
        """Clears all joins and reverts to the primary dataset view."""
        self.joins = []

        # Reset visible columns back to base schema
        primary_cols = self._dataset_column_cache.get(self.selected_dataset, [])
        self.visible_columns = [
            f"{self.selected_dataset}.{c['name']}" for c in primary_cols
        ]

        self.page_number = 1
        self._sync_all_columns()  # This reconstructs self.columns without the joins

        # TC-PIPE-05: Cascade Join Removal -> Clear Aggregations & Filters dependent on joined data
        if self.aggregations or self.aggregation_group_by:
            self.aggregations = []
            self.aggregation_group_by = []

        # Cleanup filters to remove any rules targeting joined tables
        valid_names = [f"{self.selected_dataset}.{c['name']}" for c in primary_cols]
        cleaned = self._validate_and_cleanup_filters(self.active_filters, valid_names)
        self.active_filters = (
            cleaned if cleaned else {"type": "group", "logic": "AND", "conditions": []}
        )

        yield
        from frontend.state import AppState

        yield AppState.execute_query(force=True)

    async def add_join(
        self, right_dataset: str, join_type: str, conditions: List[Dict[str, str]]
    ):
        """Adds a new join and fetches columns for the new dataset."""
        new_join = {
            "left_dataset": self.new_join_left_dataset
            if self.new_join_left_dataset
            else self.selected_dataset,
            "right_dataset": right_dataset,
            "join_type": join_type,
            "on": conditions,
        }
        self.joins.append(new_join)

        # Fetch columns for the new dataset if not in cache
        if right_dataset not in self._dataset_column_cache:
            try:
                async with httpx.AsyncClient() as client:
                    res = await client.get(
                        f"{API_BASE_URL}/datasets/{right_dataset}/columns"
                    )
                    res.raise_for_status()
                    cols = res.json().get("columns", [])
                    self._dataset_column_cache[right_dataset] = cols
            except Exception as e:
                self.error_message = (
                    f"Failed to load columns for {right_dataset}: {str(e)}"
                )

        # Refresh the global columns list to include new joined columns
        self._sync_all_columns()

    def _sync_all_columns(self):
        """Syncs the 'columns' list to include base, joined, AND derived aggregation columns."""
        all_cols = []
        # Add columns from primary dataset
        primary_cols = self._dataset_column_cache.get(self.selected_dataset, [])
        for col in primary_cols:
            all_cols.append({**col, "name": f"{self.selected_dataset}.{col['name']}"})

        # Add columns from joined datasets
        for join in self.joins:
            right_ds = join["right_dataset"]
            right_cols = self._dataset_column_cache.get(right_ds, [])
            for col in right_cols:
                all_cols.append({**col, "name": f"{right_ds}.{col['name']}"})

        # Append derived aggregation output names so they appear in
        # filter/join dropdowns (TC-PIPE-01, TC-PIPE-02 schema handoff)
        for agg in self.aggregations:
            output_name = agg.get("output_name", "")
            if output_name and not any(c["name"] == output_name for c in all_cols):
                all_cols.append(
                    {
                        "name": output_name,
                        "data_type": "DOUBLE",
                        "base_type": "numeric",
                        "nullable": True,
                        "is_filterable": True,
                        "is_sortable": True,
                        "is_derived": True,
                    }
                )

        self.columns = all_cols
        # Ensure visible_columns are also qualified if they weren't already
        new_visible = []
        for vcol in self.visible_columns:
            if "." not in vcol:
                # Don't qualify derived column names (they have no table prefix)
                if any(c["name"] == vcol and c.get("is_derived") for c in all_cols):
                    new_visible.append(vcol)
                else:
                    new_visible.append(f"{self.selected_dataset}.{vcol}")
            else:
                new_visible.append(vcol)
        self.visible_columns = new_visible

    @rx.var
    def column_type_map(self) -> Dict[str, str]:
        """Provides a lookup map of [qualified_name] -> base_type."""
        cmap = {c["name"]: c.get("base_type", "string") for c in self.columns}
        cmap[""] = "string"
        return cmap

    def _flatten_path(self, raw_path: Any) -> List[int]:
        """Flattens arbitrarily nested lists of path indices received from the frontend."""
        if isinstance(raw_path, int):
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
        path = self._flatten_path(raw_path)
        new_filters = copy.deepcopy(self.active_filters)
        target = self._get_group_at_path(new_filters, path)
        new_group = {"type": "group", "logic": "AND", "conditions": []}
        target["conditions"].append(new_group)
        self.active_filters = new_filters
        yield

    async def remove_filter_item(self, raw_path: Any):
        """Removes a rule or group at the specified path."""
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

    async def update_filter_item(self, raw_path: Any, field: str, value: str):
        """Updates a specific property of a rule at a path."""
        path = self._flatten_path(raw_path)
        print(
            f"DEBUG update_filter_item CALLED: path={path} (type={type(path)}), field={field}, value={value}"
        )
        new_filters = copy.deepcopy(self.active_filters)
        # For update_filter_item, path points to the item itself
        parent_path = path[:-1]
        index = path[-1]
        parent = self._get_group_at_path(new_filters, parent_path)

        # Check if parent has conditions to avoid KeyError on leaf modes
        if "conditions" in parent and 0 <= index < len(parent["conditions"]):
            # If changing the column, auto-select a compatible default operator based on the data type
            if field == "column":
                col_type = self.column_types.get(value, "string")
                # Normalize type for frontend consistency
                if col_type in ["numeric", "int", "float", "decimal", "number"]:
                    col_type = "number"
                elif col_type in ["date", "datetime", "timestamp"]:
                    col_type = "date"
                else:
                    col_type = "string"

                parent["conditions"][index]["datatype"] = col_type

                # Default operators per type
                if col_type == "number":
                    parent["conditions"][index]["operator"] = "="
                elif col_type == "date":
                    parent["conditions"][index]["operator"] = "between"
                else:
                    parent["conditions"][index]["operator"] = "="
                parent["conditions"][index]["value"] = ""

            parent["conditions"][index][field] = value

            # TC-OP-03: Ghost Value State Wipe
            # If changing to a unary operator, clear the value field
            unary_ops = ["is null", "is not null", "is empty", "is not empty"]
            if field == "operator" and value in unary_ops:
                parent["conditions"][index]["value"] = ""

            self.active_filters = new_filters
        yield

    async def update_filter_between_date(self, raw_path: Any, part: str, value: str):
        """Updates the start or end of a between date range value (comma-separated)."""
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

            if part == "start":
                parts[0] = value.strip()
            else:
                parts[1] = value.strip()

            parent["conditions"][index]["value"] = f"{parts[0]},{parts[1]}"
            self.active_filters = new_filters
        yield

    async def set_filter_logic(self, raw_path: Any, val: str):
        """Sets the logic (AND/OR) for a group at a path."""
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
            # We assume the path always leads to a group item
            curr = curr["conditions"][idx]
        return curr

    async def clear_filters(self):
        """Resets the advanced filters and queries."""
        self.active_filters = {"type": "group", "logic": "AND", "conditions": []}
        yield
        from frontend.state import AppState

        yield AppState.execute_query(force=True)
