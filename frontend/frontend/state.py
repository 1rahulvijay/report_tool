import reflex as rx
import httpx
from typing import List, Dict, Any
import os
from .state_modules.aggregation import AggregationState

# The base URL where our FastAPI backend is running
API_BASE_URL = os.getenv("API_BASE_URL", "http://localhost:8080/api/v1")
EXPORT_CSV_TIMEOUT = float(os.getenv("EXPORT_CSV_TIMEOUT", "3000.0"))
EXPORT_EXCEL_MAX_ROWS = int(os.getenv("EXPORT_EXCEL_MAX_ROWS", "100000"))


class AppState(AggregationState):
    """
    The final query execution and export layer.
    Inherits all capabilities (Column, Filter, Join, Aggregation) for a unified UI api.
    """

    async def execute_query(self):
        """Send the current filter/sort state to the backend to get data."""
        if not self.selected_dataset:
            return

        if not self.is_fetching_more:
            self.is_loading = True

        self.error_message = ""

        # Construct the exact Pydantic QueryRequest schema expected by the backend
        payload = {
            "dataset": self.selected_dataset,
            "columns": list(self.visible_columns),
            "joins": self.joins,
            "limit": self.page_size,
            "offset": (self.page_number - 1) * self.page_size,
            "filters": self._get_translated_filters(),
            "group_by": self.aggregation_group_by
            if self.aggregation_group_by
            else None,
            "aggregations": self.aggregations if self.aggregations else None,
            "use_high_perf_hints": self.use_oracle_in_memory,
            "is_virtual_scroll": self.is_virtual_scroll,
            "column_metadata": self._get_column_metadata_map(),
            "partition_filters": self._get_partition_filters(),
        }

        try:
            async with httpx.AsyncClient() as client:
                res = await client.post(f"{API_BASE_URL}/query/preview", json=payload)
                res.raise_for_status()
                data = res.json()

                new_data = data.get("data", [])

                if self.is_virtual_scroll and self.page_number > 1:
                    # Append for infinite scroll
                    self.query_results.extend(new_data)
                else:
                    # Replace for standard pagination or first fetch
                    self.query_results = new_data

                self.total_row_count = data.get("total_row_count", 0)
        except httpx.HTTPStatusError as e:
            if e.response.status_code == 400:
                detail = e.response.json().get("detail", str(e))
                self.error_message = detail
                self.query_results = []
                self.total_row_count = 0
                yield rx.toast.warning(
                    self.error_message, position="bottom-right", duration=10000
                )
            else:
                self.error_message = f"Query Execution Failed: {str(e)}"
                self.query_results = []
                self.total_row_count = 0
                yield rx.toast.error(self.error_message, position="bottom-right")
        except Exception as e:
            self.error_message = f"Query Execution Failed: {str(e)}"
            # Clear stale data so the user sees the error, not old results
            self.query_results = []
            self.total_row_count = 0
            yield rx.toast.error(self.error_message, position="bottom-right")
        finally:
            self.is_loading = False
            self.is_fetching_more = False

    async def toggle_virtual_scroll(self):
        """Switch between pagination and infinite scroll."""
        self.is_virtual_scroll = not self.is_virtual_scroll
        self.page_number = 1
        self.query_results = []
        return self.execute_query()

    async def toggle_oracle_in_memory(self):
        """Toggle the use of Oracle INMEMORY SQL hints."""
        self.use_oracle_in_memory = not self.use_oracle_in_memory
        return self.execute_query()

    async def export_excel(self):
        """
        Triggers a download of the current filtered dataset as an Excel file.

        Handles three response types from the backend:
          1. Sync binary response (≤10k rows) → immediate download
          2. Async job response (10k-100k rows) → poll for completion
          3. Error response (>100k rows) → suggest CSV export
        """
        if not self.selected_dataset or not self.visible_columns:
            self.is_exporting = False
            yield
            return

        self.is_exporting = True
        self.error_message = ""
        self.export_job_id = ""
        self.export_progress = 0
        self.export_status = ""
        yield  # Push state update to client immediately

        # Construct the exact Pydantic QueryRequest schema expected by the backend
        payload = {
            "dataset": self.selected_dataset,
            "columns": list(self.visible_columns),
            "joins": self.joins,
            "filters": self._get_translated_filters(),
            "group_by": self.aggregation_group_by
            if self.aggregation_group_by
            else None,
            "aggregations": self.aggregations if self.aggregations else None,
            "column_metadata": self._get_column_metadata_map(),
            "partition_filters": self._get_partition_filters(),
        }

        try:
            async with httpx.AsyncClient() as client:
                res = await client.post(
                    f"{API_BASE_URL}/query/export",
                    params={"format": "excel"},
                    json=payload,
                    timeout=120.0,
                )
                res.raise_for_status()

                content_type = res.headers.get("content-type", "")

                # Case 1: Sync binary response (small dataset)
                if "spreadsheetml" in content_type or "octet-stream" in content_type:
                    yield rx.download(
                        data=res.content,
                        filename=f"{self.selected_dataset}_export.xlsx",
                    )

                # Case 2: Async job response (JSON with job_id)
                elif "application/json" in content_type:
                    job_data = res.json()
                    job_id = job_data.get("job_id")
                    if job_id:
                        self.export_job_id = job_id
                        self.export_status = "pending"
                        self.export_progress = 0
                        yield  # Show progress UI

                        # Poll until complete
                        async for event in self._poll_export_job(job_id):
                            yield event
                    else:
                        self.error_message = "Unexpected response from export endpoint."

        except httpx.HTTPStatusError as e:
            if e.response.status_code == 400:
                detail = e.response.json().get("detail", str(e))
                self.error_message = detail
                yield rx.toast.warning(
                    self.error_message, position="bottom-right", duration=10000
                )
            else:
                self.error_message = f"Export Failed: {str(e)}"
                yield rx.toast.error(self.error_message, position="bottom-right")
        except Exception as e:
            self.error_message = f"Export Execution Failed: {str(e)}"
            yield rx.toast.error(self.error_message, position="bottom-right")
        finally:
            self.is_exporting = False
            self.export_status = ""
            self.export_job_id = ""
            yield  # Ensure the spinner is cleared

    async def _poll_export_job(self, job_id: str):
        """Polls the export status endpoint until the job is complete or failed."""
        import asyncio

        max_polls = 300  # 5 minutes at 1s interval
        for _ in range(max_polls):
            await asyncio.sleep(1)
            try:
                async with httpx.AsyncClient() as client:
                    res = await client.get(
                        f"{API_BASE_URL}/export/status/{job_id}",
                        timeout=10.0,
                    )
                    res.raise_for_status()
                    status_data = res.json()

                    self.export_status = status_data.get("status", "")
                    self.export_progress = status_data.get("progress_pct", 0)
                    yield  # Update progress UI

                    if self.export_status == "complete":
                        download_url = status_data.get("download_url", "")
                        if download_url:
                            # Download the file
                            dl_res = await client.get(
                                f"http://localhost:8080{download_url}",
                                timeout=120.0,
                            )
                            dl_res.raise_for_status()
                            yield rx.download(
                                data=dl_res.content,
                                filename=f"{self.selected_dataset}_export.xlsx",
                            )
                        return

                    elif self.export_status == "failed":
                        self.error_message = f"Export failed: {status_data.get('error', 'Unknown error')}"
                        yield rx.toast.error(
                            self.error_message, position="bottom-right"
                        )
                        return

            except Exception as e:
                self.error_message = f"Export polling error: {str(e)}"
                yield rx.toast.error(self.error_message, position="bottom-right")
                return

        self.error_message = "Export timed out after 5 minutes."
        yield rx.toast.warning(self.error_message, position="bottom-right")

    async def export_csv(self):
        """Triggers a CSV download — no size limits, streams for large datasets."""
        if not self.selected_dataset or not self.visible_columns:
            return

        self.is_exporting = True
        self.error_message = ""
        yield

        payload = {
            "dataset": self.selected_dataset,
            "columns": list(self.visible_columns),
            "joins": self.joins,
            "filters": self._get_translated_filters(),
            "group_by": self.aggregation_group_by
            if self.aggregation_group_by
            else None,
            "aggregations": self.aggregations if self.aggregations else None,
            "column_metadata": self._get_column_metadata_map(),
            "partition_filters": self._get_partition_filters(),
        }

        try:
            async with httpx.AsyncClient() as client:
                res = await client.post(
                    f"{API_BASE_URL}/query/export",
                    params={"format": "csv"},
                    json=payload,
                    timeout=EXPORT_CSV_TIMEOUT,
                )
                res.raise_for_status()
                yield rx.download(
                    data=res.content,
                    filename=f"{self.selected_dataset}_export.csv",
                )
        except Exception as e:
            self.error_message = f"CSV Export Failed: {str(e)}"
            yield rx.toast.error(self.error_message, position="bottom-right")
        finally:
            self.is_exporting = False
            yield

    async def clear_data(self):
        """
        Trims large dataset arrays from memory.
        Called when leaving the datagrid page.
        """
        self.query_results = []
        self.total_row_count = 0
        self.page_number = 1

    async def set_page_number(self, page: str):
        """Safely set the page number from input string."""
        try:
            val = int(page)
            if 1 <= val <= self.total_pages:
                self.page_number = val
                return self.execute_query()
        except ValueError:
            pass

    async def set_page_size(self, size: str):
        """Safely set the page size from select string."""
        try:
            val = int(size)
            if val > 0:
                self.page_size = val
                self.page_number = 1
                return self.execute_query()
        except ValueError:
            pass

    async def next_page(self, _=None):
        if self.page_number < self.total_pages:
            self.page_number += 1
            return self.execute_query()

    async def prev_page(self, _=None):
        if self.page_number > 1:
            self.page_number -= 1
            return self.execute_query()

    async def first_page(self, _=None):
        if self.page_number != 1:
            self.page_number = 1
            return self.execute_query()

    async def last_page(self, _=None):
        if self.page_number != self.total_pages:
            self.page_number = self.total_pages
            return self.execute_query()

    @rx.var
    def total_pages(self) -> int:
        """Calculates total pages based on row count and page size."""
        if self.total_row_count == 0:
            return 1
        return (self.total_row_count + self.page_size - 1) // self.page_size

    @rx.var
    def has_active_filters(self) -> bool:
        """Safely evaluates whether the active_filters dictionary contains conditions."""
        conditions = self.active_filters.get("conditions", [])
        # We must cast the type hint here because reflex sees this as a list natively
        return len(conditions) > 0

    @rx.var
    def columns_changed(self) -> bool:
        """Determines if the visible columns differ from the full set."""
        return len(self.visible_columns) != len(self.columns)

    @rx.var
    def column_names(self) -> List[str]:
        """
        Returns the active schema for the filter dropdowns.
        If aggregations are active, it ONLY returns the grouped/computed columns.
        If no aggregations, it returns the base schema.
        """
        if self.aggregations:
            names = []
            # Only grouped columns survive aggregation
            for group in self.aggregation_group_by:
                if group not in names:
                    names.append(group)

            # Plus the new calculated metrics
            for agg in self.aggregations:
                if agg.get("output_name"):
                    names.append(agg["output_name"])
                elif agg.get("column"):
                    func = agg.get("function", "sum").upper()
                    names.append(f"{func}_{agg['column']}")
            return names

        return [c["name"] for c in self.columns]

    @rx.var
    def dataset_names(self) -> List[str]:
        """Returns a list of dataset names (sorted) for the frontend select dropdown."""
        names = [ds["name"] for ds in self.datasets]
        return sorted(names)

    @rx.var
    def can_export(self) -> bool:
        """Determines if the export button should be enabled at all."""
        return (len(self.visible_columns) > 0) and (not self.is_exporting)

    @rx.var
    def can_export_excel(self) -> bool:
        """Determines if Excel export should be enabled based on dataset size."""
        return self.can_export and self.total_row_count <= EXPORT_EXCEL_MAX_ROWS

    @rx.var
    def filtered_datasets(self) -> List[str]:
        """Returns the list of dataset names filtered by the sidebar search input."""
        names = self.dataset_names
        if not self.dataset_search_text:
            return names
        search_text = self.dataset_search_text.lower()
        return [name for name in names if search_text in name.lower()]

    @rx.var
    def filtered_columns(self) -> List[Dict[str, Any]]:
        """Returns the list of columns filtered by the sidebar search input."""
        if not self.column_search_text:
            return self.columns
        search_text = self.column_search_text.lower()
        return [col for col in self.columns if search_text in col["name"].lower()]

    @rx.var
    def active_filter_conditions(self) -> List[Dict[str, Any]]:
        """Provides a strongly typed list of conditions for the filter modal UI to iterate over."""
        return self.active_filters.get("conditions", [])

    @rx.var
    def table_headers(self) -> List[str]:
        """Dynamically computes the header names from visible columns maintaining order."""
        # Ensure it maps order based on the master columns list, but also include
        # dynamically created aggregation columns that might not exist in self.columns
        original_cols = [c["name"] for c in self.columns]
        headers = []

        # First add columns that exist in the original schema in defined order
        for col in self.columns:
            if col["name"] in self.visible_columns:
                headers.append(col["name"])

        # Then append any visible columns (like aggregation aliases) that weren't in the original schema
        for vcol in self.visible_columns:
            if vcol not in original_cols and vcol not in headers:
                headers.append(vcol)

        return headers

    @rx.var
    def table_data(self) -> List[List[str]]:
        """Dynamically builds a 2D list of strings for the table component based on visible columns."""
        if not self.query_results:
            return []

        headers = self.table_headers
        data = []
        search_term = self.search_value_text.lower()

        for row in self.query_results:
            # Create a case-insensitive lookup for the row. Strip table prefixes from keys entirely
            # so `LARGE_TABLE_1M_2.ID` just becomes `id` in the lookup dict, and `ID` becomes `id`.
            row_lookup = {}
            for k, v in row.items():
                row_lookup[k.lower()] = v
                if "." in k:
                    row_lookup[k.split(".")[-1].lower()] = v

            row_data = []
            row_matches_search = False
            for h in headers:
                # 1. Exact match
                val = row.get(h)

                # 2. Exact match lower case
                if val is None:
                    val = row_lookup.get(h.lower())

                # 3. Strip table alias from header, exact match
                if val is None and "." in h:
                    local_h = h.split(".")[-1]
                    val = row.get(local_h)

                    # 4. Strip table alias, lowercase match
                    if val is None:
                        val = row_lookup.get(local_h.lower())

                if isinstance(val, float):
                    val_str = f"{val:.2f}"
                else:
                    val_str = str(val) if val is not None else ""

                row_data.append(val_str)
                if search_term and search_term in val_str.lower():
                    row_matches_search = True

            if not search_term or row_matches_search:
                data.append(row_data)

        return data
