from typing import List, Any
import copy

from .join import JoinState


class AggregationState(JoinState):
    """Manages group-by metrics, measures, and aggregations."""

    def toggle_aggregation_modal(self):
        """Toggles the aggregation builder modal."""
        self.is_aggregation_modal_open = not self.is_aggregation_modal_open

    def add_aggregation_row(self):
        """Adds a new aggregation metric row."""
        self.aggregations.append({"column": "", "function": "sum", "output_name": ""})

    def update_aggregation_row(self, index: int, field: str, value: str):
        """Updates a field in an aggregation row."""
        rows = copy.deepcopy(self.aggregations)
        if 0 <= index < len(rows):
            rows[index][field] = value
            self.aggregations = rows

    def remove_aggregation_row(self, index: int):
        """Removes an aggregation metric row."""
        rows = copy.deepcopy(self.aggregations)
        if 0 <= index < len(rows):
            rows.pop(index)
            self.aggregations = rows

    def add_group_by_column(self, col_name: str):
        """Adds a column to the Group By list if not already present."""
        if col_name and col_name not in self.aggregation_group_by:
            self.aggregation_group_by.append(col_name)

    def remove_group_by_column(self, col_name: str):
        """Removes a column from the Group By list."""
        if col_name in self.aggregation_group_by:
            self.aggregation_group_by.remove(col_name)

    async def clear_aggregations(self):
        """Resets the aggregation state."""
        self.aggregation_group_by = []
        self.aggregations = []
        # Restore original visible columns
        primary_cols = self._dataset_column_cache.get(self.selected_dataset, [])
        self.visible_columns = [
            f"{self.selected_dataset}.{c['name']}" for c in primary_cols
        ]
        self.page_number = 1
        yield
        from frontend.state import AppState

        yield AppState.execute_query(force=True)

    async def reset_all(self):
        """Resets filters, joins, and aggregations."""
        self.active_filters = {"type": "group", "logic": "AND", "conditions": []}
        self.joins = []
        self.aggregation_group_by = []
        self.aggregations = []

        # TC-RST-03: Clear search
        self.column_search_text = ""
        self.dataset_search_text = ""
        self.search_value_text = ""

        primary_cols = self._dataset_column_cache.get(self.selected_dataset, [])
        self.visible_columns = [
            f"{self.selected_dataset}.{c['name']}" for c in primary_cols
        ]

        self.page_number = 1
        self._sync_all_columns()  # CRITICAL: Hard-wipe the schema variable back to base
        yield
        from frontend.state import AppState

        yield AppState.execute_query(force=True)

    async def apply_aggregations(self):
        """Closes the modal and refreshes data with aggregations."""
        self.is_aggregation_modal_open = False
        self.page_number = 1
        self.query_results = []

        # Update visible_columns to exactly match the summarized report schema
        res_cols = self.aggregation_group_by.copy()
        for agg in self.aggregations:
            if agg.get("output_name"):
                res_cols.append(agg["output_name"])
            elif agg.get("column"):
                # Fallback alias if output name is blank
                func = agg.get("function", "sum").upper()
                res_cols.append(f"{func}_{agg['column']}")

        if res_cols:
            self.visible_columns = res_cols

        # TC-PIPE-04: Cascade Filter Deletion
        # If we have filters targeting columns deleted by the aggregation, drop them.
        if self.active_filters.get("conditions"):
            valid_names = res_cols if res_cols else [c["name"] for c in self.columns]
            cleaned = self._validate_and_cleanup_filters(
                self.active_filters, valid_names
            )
            self.active_filters = (
                cleaned
                if cleaned
                else {"type": "group", "logic": "AND", "conditions": []}
            )

        yield
        # Sync schema so derived columns appear in filter/join dropdowns
        self._sync_all_columns()
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

    async def set_partition_values(self, dataset: str, values: List[Any]):
        """User selects specific partition values from the Data Vintage dropdown."""
        if values:
            self.selected_partitions = {
                **self.selected_partitions,
                dataset: values,
            }
        else:
            # Remove partition filter for this dataset
            new_parts = {**self.selected_partitions}
            new_parts.pop(dataset, None)
            self.selected_partitions = new_parts
        self.page_number = 1
        self.query_results = []
        from frontend.state import AppState

        yield AppState.execute_query(force=True)

    async def toggle_partition_unrestricted(self):
        """Toggle unrestricted mode (scan all partitions). Use with caution."""
        self.partition_unrestricted = not self.partition_unrestricted
        self.page_number = 1
        self.query_results = []
        from frontend.state import AppState

        yield AppState.execute_query(force=True)
