import reflex as rx
import httpx
from typing import List, Dict, Any, Optional
import os

# The base URL where our FastAPI backend is running
API_BASE_URL = os.getenv("API_BASE_URL", "http://localhost:8080/api/v1")


class BaseState(rx.State):
    """
    The global application base state.
    Handles global configuration, load state, and dataset selection.
    """

    # Metadata State
    datasets: List[Dict[str, Any]] = []
    selected_dataset: str = ""
    columns: List[Dict[str, Any]] = []

    # Query State
    visible_columns: List[str] = []

    query_results: List[Dict[str, Any]] = []
    total_row_count: int = 0
    is_loading: bool = False
    is_exporting: bool = False

    # Join State
    joins: List[Dict[str, Any]] = []
    is_join_modal_open: bool = False

    # Join Preview/Sample State
    is_join_preview_modal_open: bool = False
    join_preview_data: List[Dict[str, Any]] = []

    # Aggregation State
    aggregation_group_by: List[str] = []
    aggregations: List[Dict[str, str]] = []
    is_aggregation_modal_open: bool = False

    # Internal map to keep track of columns for all involved datasets
    _dataset_column_cache: Dict[str, List[Dict[str, Any]]] = {}

    # Extreme Scale State
    is_virtual_scroll: bool = False
    use_oracle_in_memory: bool = False
    is_fetching_more: bool = False

    # Join Modal Transient State
    new_join_left_dataset: str = ""
    new_join_right_dataset: str = ""
    new_join_type: str = "inner"
    new_join_conditions: List[Dict[str, str]] = []

    # Pagination State
    page_number: int = 1
    page_size: int = 10

    # Error Handling
    error_message: str = ""

    # Async Export State
    export_job_id: str = ""
    export_progress: int = 0
    export_status: str = ""  # "" | "pending" | "processing" | "complete" | "failed"

    # Partition / Data Vintage State
    partition_info: Dict[
        str, Any
    ] = {}  # {column, load_type_column, available_values, max_value, min_value}
    selected_partitions: Dict[str, List[Any]] = {}  # dataset_name -> [selected values]
    partition_unrestricted: bool = False  # "Select All" override
    partition_load_type: str = ""  # Currently selected load type (e.g. "Monthly")

    async def fetch_datasets(self):
        """Fetch available datasets on load."""
        self.is_loading = True
        self.error_message = ""
        yield
        try:
            async with httpx.AsyncClient() as client:
                res = await client.get(f"{API_BASE_URL}/datasets")
                res.raise_for_status()
                data = res.json()
                self.datasets = data.get("datasets", [])

                # Note: By design, no table is selected by default.
        except Exception as e:
            self.error_message = f"Failed to load datasets: {str(e)}"
        finally:
            self.is_loading = False

    async def select_dataset(self, dataset_name: str):
        """When a user clicks a dataset, fetch its schema/columns."""
        self.selected_dataset = dataset_name
        self.is_loading = True
        self.error_message = ""

        try:
            async with httpx.AsyncClient() as client:
                res = await client.get(
                    f"{API_BASE_URL}/datasets/{dataset_name}/columns"
                )
                res.raise_for_status()
                data = res.json()
                self.columns = data.get("columns", [])

                # Parse partition metadata from backend response
                part_info = data.get("partition_info")
                if part_info:
                    self.partition_info = part_info

                    # Set default load type
                    supported_types = part_info.get("supported_types", [])
                    if supported_types:
                        self.partition_load_type = supported_types[0]

                    # Auto-select MAX partition value by default
                    if part_info.get("max_value") is not None:
                        self.selected_partitions = {
                            dataset_name: [part_info["max_value"]]
                        }
                    else:
                        self.selected_partitions = {}
                    self.partition_unrestricted = False
                else:
                    self.partition_info = {}
                    self.selected_partitions = {}
                    self.partition_unrestricted = False
                    self.partition_load_type = ""

                # Default all columns to visible (using sync to qualify them)
                self.visible_columns = [col["name"] for col in self.columns]
                self._dataset_column_cache[dataset_name] = self.columns

                from frontend.state import AppState

                AppState._sync_all_columns(self)

                # Clear joins when primary dataset changes
                self.joins = []

                # Reset pagination on dataset change
                self.page_number = 1

                # Immediately preview data
                self.query_results = []  # Reset results for new dataset
                from frontend.state import AppState

                yield AppState.execute_query()
        except Exception as e:
            self.error_message = f"Failed to load columns for {dataset_name}: {str(e)}"
        finally:
            self.is_loading = False

    # ─── Data Vintage Computed Vars ──────────────────────────────

    @rx.var
    def has_partition_info(self) -> bool:
        """True when the selected dataset has partition metadata AND at least one component to show."""
        if not self.partition_info:
            return False

        has_load_type = bool(self.partition_info.get("load_type_column"))
        has_supported = bool(self.partition_info.get("supported_types"))
        has_load_id = bool(self.partition_info.get("load_id_column"))
        has_date = bool(self.partition_info.get("date_column"))

        return has_load_type or has_supported or has_load_id or has_date

    @rx.var
    def has_load_type(self) -> bool:
        """True if the dataset has a load_type_column configured."""
        return bool(self.partition_info.get("load_type_column"))

    @rx.var
    def has_load_id(self) -> bool:
        """True if the dataset has a specific load_id column configured."""
        return bool(self.partition_info.get("load_id_column"))

    @rx.var
    def has_date_column(self) -> bool:
        """True if the dataset has a date_column configured for display."""
        return bool(self.partition_info.get("date_column"))

    @rx.var
    def partition_supported_types(self) -> List[str]:
        """Returns the list of supported types (e.g., 'Monthly', 'Daily')."""
        return self.partition_info.get("supported_types", [])

    async def set_partition_load_type(self, value: str):
        """Change partition load type (e.g. from Monthly to Daily) and reload defaults."""
        if not value or value == self.partition_load_type:
            return
        self.partition_load_type = value
        # Reset the specific load id selection so we default to the newest of the new type
        if self.partition_info.get("max_value") is not None:
            self.selected_partitions = {
                self.selected_dataset: [self.partition_info["max_value"]]
            }

        from frontend.state import AppState

        self.page_number = 1
        yield AppState.execute_query()

    @rx.var
    def partition_column_name(self) -> str:
        """Returns the user-friendly partition column name for display."""
        return self.partition_info.get("date_column", "")

    @rx.var
    def partition_available_values(self) -> List[str]:
        """Returns available partition values as strings for the dropdown."""
        vals = self.partition_info.get("available_values", [])
        return [str(v) for v in vals]

    @rx.var
    def current_load_id_display(self) -> str:
        """Returns the currently selected load ID for display."""
        if not self.selected_partitions:
            return ""
        ds = self.selected_dataset
        vals = self.selected_partitions.get(ds, [])
        if vals:
            return str(vals[0])
        return ""

    async def set_current_load_id(self, value: str):
        """User changes the Load ID dropdown — update partition and re-query."""
        if not value or not self.selected_dataset:
            return
        # Convert string back to the original type (int for numeric IDs)
        parsed_value = value
        try:
            if "." in value:
                parsed_value = float(value)
            else:
                parsed_value = int(value)
        except (ValueError, TypeError):
            parsed_value = value

        self.selected_partitions = {
            **self.selected_partitions,
            self.selected_dataset: [parsed_value],
        }
        self.partition_unrestricted = False
        self.page_number = 1
        self.query_results = []

        from frontend.state import AppState

        return AppState.execute_query()

    @rx.var
    def column_types(self) -> Dict[str, str]:
        """Provides a mapping of column_name to its base_type (string, numeric, datetime, etc) to power the dynamic UI."""
        return {c["name"]: c.get("base_type", "string") for c in self.columns}

    def _get_column_metadata_map(self) -> Dict[str, Any]:
        """Returns a flattened map of 'table.column' -> metadata for all involved datasets,
        including derived aggregation columns."""
        meta_map = {}
        # Include primary dataset
        if self.selected_dataset in self._dataset_column_cache:
            for col in self._dataset_column_cache[self.selected_dataset]:
                qualified_name = f"{self.selected_dataset}.{col['name']}"
                meta_map[qualified_name] = col

        # Include all joined datasets
        for join in self.joins:
            right_ds = join["right_dataset"]
            if right_ds in self._dataset_column_cache:
                for col in self._dataset_column_cache[right_ds]:
                    qualified_name = f"{right_ds}.{col['name']}"
                    meta_map[qualified_name] = col

        # Include derived aggregation output names (TC-PIPE-01 type coercion)
        for agg in self.aggregations:
            output_name = agg.get("output_name", "")
            if output_name:
                meta_map[output_name] = {
                    "name": output_name,
                    "data_type": "DOUBLE",
                    "base_type": "numeric",
                    "nullable": True,
                    "is_filterable": True,
                    "is_sortable": True,
                    "is_derived": True,
                }
        return meta_map

    @rx.var
    def columns_changed_from_all(self) -> bool:
        """Returns true if the user changed column selections from 'Select All'."""
        return len(self.visible_columns) < len(self.columns)

    def _get_partition_filters(self) -> Optional[Dict[str, List[Any]]]:
        """
        Build the partition_filters dict for the backend payload.
        Returns None if unrestricted mode is on, no partitions are selected,
        or the dataset has no actual partition column.
        """
        if self.partition_unrestricted:
            return None
        if not self.selected_partitions:
            return None
        # Only send partition filters if the backend confirmed the table is partitioned
        if not self.partition_info or not self.partition_info.get("load_id_column"):
            return None
        return self.selected_partitions
