import reflex as rx
import httpx
from typing import List, Dict, Any
import asyncio
import time
from .state_modules.aggregation import AggregationState

from .config import (
    API_BASE_URL,
    EXPORT_CSV_TIMEOUT,
    EXPORT_EXCEL_MAX_ROWS,
    QUERY_DEBOUNCE_DELAY,
)


class AppState(AggregationState):
    """
    The final query execution and export layer.
    Inherits all capabilities (Column, Filter, Join, Aggregation) for a unified UI api.
    """

    _last_query_time: float = 0.0

    async def execute_query(self, force: bool = False):
        """Send the current filter/sort state to the backend to get data."""
        if not self.selected_dataset:
            return

        if not force:
            # Debounce logic: wait 300ms. If a newer request arrives, abort this one.
            current_time = time.time()
            self._last_query_time = current_time
            await asyncio.sleep(QUERY_DEBOUNCE_DELAY)

            if self._last_query_time != current_time:
                return

        if not self.is_fetching_more:
            self.is_loading = True
            yield  # Force UI to render spinner before blocking query

        self.error_message = ""

        # Construct the exact Pydantic QueryRequest schema expected by the backend
        translated_filters = self._get_translated_filters()
        import json as _json

        print(
            f"[QUERY DEBUG] translated_filters = {_json.dumps(translated_filters, default=str, indent=2) if translated_filters else 'None'}"
        )

        # Merge header inline filters (col_name -> contains text) into the filter tree
        # GUARD: When aggregations are active, only allow filters on GROUP BY columns
        # to prevent ORA-00979 (not a GROUP BY expression)
        active_hf = dict(self.header_filters)
        if active_hf and self.aggregation_group_by:
            # Normalize GROUP BY column names for matching
            gb_upper = {g.upper().split(".")[-1] for g in self.aggregation_group_by}
            active_hf = {
                k: v
                for k, v in active_hf.items()
                if k.upper().split(".")[-1] in gb_upper
            }
        if active_hf:
            # Build a robust lookup map for column metadata.
            # 1. Qualified names (TABLE.COL)
            # 2. Stripped names (COL) - fallback
            # 3. Normalized names (TRANSFORM_COL -> TRANSFORMCOL)
            lookup_map = {}
            normalized_lookup = {}

            def normalize(s):
                return s.upper().replace(" ", "").replace("_", "").replace(".", "")

            for c in self.columns:
                qualified = c["name"].upper()
                stripped = qualified.split(".")[-1]
                lookup_map[qualified] = c
                if stripped not in lookup_map:
                    lookup_map[stripped] = c

                # Double down on normalization for tricky joins/aliasing
                normalized_lookup[normalize(qualified)] = c
                normalized_lookup[normalize(stripped)] = c

            # Temporarily swap header_filters with the filtered set (for agg-guard)
            original_hf = self.header_filters
            self.header_filters = active_hf
            header_conditions = getattr(
                self, "_execute_header_filters", lambda x, y: []
            )(lookup_map, normalized_lookup)
            self.header_filters = original_hf  # Restore original

            if header_conditions:
                if translated_filters and translated_filters.get("conditions"):
                    # Merge into existing filters with AND
                    translated_filters["conditions"].extend(header_conditions)
                else:
                    translated_filters = {
                        "logic": "AND",
                        "conditions": header_conditions,
                    }

        payload = {
            "dataset": self.selected_dataset,
            "columns": list(self.visible_columns),
            "joins": self.joins,
            "limit": self.page_size,
            "offset": (self.page_number - 1) * self.page_size,
            "filters": translated_filters,
            "group_by": self.aggregation_group_by
            if self.aggregation_group_by
            else None,
            "aggregations": self.aggregations if self.aggregations else None,
            "use_high_perf_hints": self.use_oracle_in_memory,
            "is_virtual_scroll": self.is_virtual_scroll,
            "is_preview": True,
            "column_metadata": self._get_column_metadata_map(),
            "partition_filters": self._get_partition_filters(),
            "partition_load_type": self.partition_load_type
            if self.partition_load_type
            else None,
            "sorting": [
                {"column": self.sort_column, "direction": self.sort_direction.upper()}
            ]
            if self.sort_column and self.sort_direction
            else None,
        }

        try:
            async with httpx.AsyncClient() as client:
                res = await client.post(f"{API_BASE_URL}/query/preview", json=payload)
                res.raise_for_status()
                data = res.json()

                new_data = data.get("data", [])

                if self.is_virtual_scroll and self.page_number > 1:
                    # Append for infinite scroll (reassign to trigger Reflex state update)
                    self.query_results = self.query_results + new_data
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
        async for ev in self.execute_query():
            yield ev

    async def toggle_oracle_in_memory(self):
        """Toggle the use of Oracle INMEMORY SQL hints."""
        self.use_oracle_in_memory = not self.use_oracle_in_memory
        async for ev in self.execute_query():
            yield ev

    async def toggle_sort(self, col_name: str):
        """Toggles sort between ASC, DESC, and None for a column."""
        if self.sort_column == col_name:
            if self.sort_direction == "asc":
                self.sort_direction = "desc"
            elif self.sort_direction == "desc":
                self.sort_column = ""
                self.sort_direction = ""
            else:
                self.sort_direction = "asc"
        else:
            self.sort_column = col_name
            self.sort_direction = "asc"

        self.page_number = 1
        self.query_results = []
        async for ev in self.execute_query(force=True):
            yield ev

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
            "partition_load_type": self.partition_load_type
            if self.partition_load_type
            else None,
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

        from .config import MAX_EXPORT_POLLS, EXPORT_POLLING_INTERVAL

        max_polls = MAX_EXPORT_POLLS
        for _ in range(max_polls):
            await asyncio.sleep(EXPORT_POLLING_INTERVAL)
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
            "partition_load_type": self.partition_load_type
            if self.partition_load_type
            else None,
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
        self.selected_row_indices = []

    async def set_page_number(self, page: str):
        """Safely set the page number from input string."""
        try:
            val = int(page)
            if 1 <= val <= self.total_pages:
                self.page_number = val
                async for ev in self.execute_query():
                    yield ev
        except ValueError:
            pass

    async def set_page_size(self, size: str):
        """Safely set the page size from select string."""
        try:
            val = int(size)
            if val > 0:
                self.page_size = val
                self.page_number = 1
                async for ev in self.execute_query():
                    yield ev
        except ValueError:
            pass

    def set_header_filter(self, col_name: str, value: str):
        """Stores an inline column header filter value WITHOUT triggering a query.
        The query is triggered separately by apply_header_filters (Enter key / blur)."""
        new_filters = {**self.header_filters}
        if value and value.strip():
            new_filters[col_name] = value
        else:
            new_filters.pop(col_name, None)
        self.header_filters = new_filters

    async def apply_header_filters(self):
        """Applies the current header filters by triggering a query.
        Called on Enter key press or input blur."""
        self.page_number = 1
        async for ev in self.execute_query(force=True):
            yield ev

    async def clear_header_filters(self):
        """Clears all inline column header filters."""
        self.header_filters = {}
        self.page_number = 1
        yield
        async for ev in self.execute_query(force=True):
            yield ev

    async def next_page(self, _=None):
        # Guard: Don't allow double-trigger while already fetching more rows
        if self.is_fetching_more:
            return
        if self.page_number < self.total_pages:
            if self.is_virtual_scroll:
                self.is_fetching_more = True
                yield  # Push spinner state to UI before query starts
            self.page_number += 1
            async for ev in self.execute_query(force=True):
                yield ev
        else:
            # No more pages — ensure we reset fetching state cleanly
            self.is_fetching_more = False

    async def prev_page(self, _=None):
        if self.page_number > 1:
            self.page_number -= 1
            async for ev in self.execute_query():
                yield ev

    async def first_page(self, _=None):
        if self.page_number != 1:
            self.page_number = 1
            async for ev in self.execute_query():
                yield ev

    async def last_page(self, _=None):
        if self.page_number != self.total_pages:
            self.page_number = self.total_pages
            async for ev in self.execute_query():
                yield ev

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
        """Returns a list of full dataset names (sorted) for internal use."""
        names = [ds["name"] for ds in self.datasets]
        return sorted(names)

    @rx.var
    def dataset_display_names(self) -> List[str]:
        """Returns display-only names — uses friendly name from config if available."""
        result = []
        for ds in sorted(self.datasets, key=lambda d: d["name"]):
            display = ds.get("display_name", "")
            if not display:
                display = ds["name"].split(".")[-1] if "." in ds["name"] else ds["name"]
            result.append(display)
        return result

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
        """Returns the list of dataset names (full) filtered by the sidebar search input."""
        names = self.dataset_names
        if not self.dataset_search_text:
            return names
        search_text = self.dataset_search_text.lower()
        return [name for name in names if search_text in name.lower()]

    @rx.var
    def filtered_datasets_display(self) -> List[List[str]]:
        """Returns [[full_name, display_name], ...] for filtered datasets."""
        ds_map = {ds["name"]: ds.get("display_name", "") for ds in self.datasets}
        result = []
        for name in self.filtered_datasets:
            display = ds_map.get(name, "")
            if not display:
                display = name.split(".")[-1] if "." in name else name
            result.append([name, display])
        return result

    @rx.var
    def display_selected_dataset(self) -> str:
        """Returns selected dataset's friendly display name."""
        ds = self.selected_dataset
        if not ds:
            return ""
        # Check if we have a display_name from the API
        for d in self.datasets:
            if d["name"] == ds:
                display = d.get("display_name", "")
                if display:
                    return display
        return ds.split(".")[-1] if "." in ds else ds

    @rx.var
    def filtered_columns(self) -> list[dict[str, str]]:
        """Returns columns with display_name added for sidebar iteration."""
        cols = (
            self.columns
            if not self.column_search_text
            else [
                col
                for col in self.columns
                if self.column_search_text.lower() in col["name"].lower()
                or self.column_search_text.lower()
                in col.get("display_name", col["name"]).lower()
            ]
        )
        result = []
        for col in cols:
            name = col["name"]
            display = col.get("display_name", "")
            if not display:
                display = name.split(".")[-1] if "." in name else name
            result.append({"name": name, "display_name": display})
        return result

    @rx.var
    def active_filter_conditions(self) -> List[Dict[str, Any]]:
        """Provides a strongly typed list of conditions for the filter modal UI to iterate over."""
        return self.active_filters.get("conditions", [])

    @rx.var
    def table_headers(self) -> List[Dict[str, str]]:
        """Dynamically computes display-ready header names and their qualified paths, preserving user order."""
        headers = []
        col_map = {c["name"]: c for c in self.columns}

        # Preserve the exact order defined in visible_columns (critical for dynamic aggregations)
        for vcol in self.visible_columns:
            if vcol in col_map:
                name = vcol
                display = col_map[vcol].get("display_name", "")
                if not display:
                    display = name.split(".")[-1] if "." in name else name
                headers.append({"qualified": name, "display": display})
            else:
                display = vcol.split(".")[-1] if "." in vcol else vcol
                headers.append({"qualified": vcol, "display": display})

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
            # so `LARGE_TABLE_1_2.ID` just becomes `id` in the lookup dict, and `ID` becomes `id`.
            row_lookup = {}
            for k, v in row.items():
                row_lookup[k.lower()] = v
                if "." in k:
                    row_lookup[k.split(".")[-1].lower()] = v

            row_data = []
            row_matches_search = False
            for h_dict in headers:
                h = h_dict["qualified"]
                display_h = h_dict["display"]
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

                # 5. Fallback to display header name
                if val is None:
                    val = row.get(display_h)
                    if val is None:
                        val = row_lookup.get(display_h.lower())

                if isinstance(val, (float, int)):
                    if isinstance(val, float):
                        val_str = f"{val:.2f}"
                    else:
                        val_str = str(val)
                else:
                    val_str = str(val) if val is not None else ""

                row_data.append(val_str)
                if search_term and search_term in val_str.lower():
                    row_matches_search = True

            if not search_term or row_matches_search:
                data.append(row_data)

        return data

    @rx.var
    def table_data_indexed(self) -> List[tuple[List[str], int, str]]:
        """Returns the table data enumerable with indices and IDs for the frontend."""
        return [(row, i, self._get_row_id(i)) for i, row in enumerate(self.table_data)]

    def _get_row_id(self, index: int) -> str:
        """Attempts to find a unique ID for a row at the given index."""
        if not (0 <= index < len(self.query_results)):
            return str(index)
        row = self.query_results[index]
        # Look for common ID columns
        id_cols = ["ID", "LOADID", "ORDER_ID", "EMP_ID", "ROWID"]
        for col in id_cols:
            # Check exact and qualified
            val = row.get(col) or row.get(f"{self.selected_dataset}.{col}")
            if val is not None:
                return str(val)
        # Fallback to stringified row content (stable enough for a single view)
        return str(hash(frozenset(row.items())))

    def toggle_row_selection(self, index: int):
        """Toggles a row's selection status using a unique ID."""
        row_id = self._get_row_id(index)
        new_ids = list(self.selected_row_ids)
        if row_id in new_ids:
            new_ids.remove(row_id)
        else:
            new_ids.append(row_id)
        self.selected_row_ids = new_ids

    def toggle_all_page_rows(self):
        """Selects or unselects all rows on the current page."""
        current_page_ids = [self._get_row_id(i) for i in range(len(self.query_results))]
        # If all current page IDs are in selection, remove them
        if all(rid in self.selected_row_ids for rid in current_page_ids):
            self.selected_row_ids = [
                rid for rid in self.selected_row_ids if rid not in current_page_ids
            ]
        else:
            new_ids = list(self.selected_row_ids)
            for rid in current_page_ids:
                if rid not in new_ids:
                    new_ids.append(rid)
            self.selected_row_ids = new_ids

    @rx.var
    def page_all_selected(self) -> bool:
        """True if all rows on the current page are in the selected_row_ids list."""
        if not self.query_results:
            return False
        current_page_ids = [self._get_row_id(i) for i in range(len(self.query_results))]
        return all(rid in self.selected_row_ids for rid in current_page_ids)

    def clear_row_selection(self):
        """Clears all selected rows."""
        self.selected_row_ids = []
