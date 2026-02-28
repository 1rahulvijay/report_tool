from typing import List, Dict, Any
import reflex as rx
import httpx
import json
import os
import plotly.graph_objects as go
from frontend.config import (
    API_BASE_URL,
    PRESET_RAW_QUERY_TIMEOUT,
    VIBRANT_PALETTE,
    COLORS,
)


class PresetState(rx.State):
    """Manages reading presets configuration and querying data for the 4 charts."""

    preset_config: Dict[str, Any] = {}
    chart_data: Dict[str, List[Dict[str, Any]]] = {}
    current_presets: List[Dict[str, Any]] = []
    is_loading_presets: bool = False
    _last_executed_hash: str = ""

    def fetch_presets_config(self):
        """Loads presets.json from the root directory with multi-location search."""
        try:
            # Multi-level search for presets.json to handle different starting directories
            search_paths = [
                os.path.join(os.getcwd(), "..", "presets.json"),
                os.path.join(os.getcwd(), "presets.json"),
                os.path.join(os.getcwd(), "frontend", "..", "presets.json"),
                os.path.abspath("presets.json"),
            ]

            found_path = None
            for p in search_paths:
                if os.path.exists(p):
                    found_path = p
                    break

            if found_path:
                with open(found_path, "r") as f:
                    self.preset_config = json.load(f)
            else:
                self.preset_config = {}
        except Exception as e:
            print(f"Error loading presets config: {e}")
            self.preset_config = {}

    def _get_config_key(self, dataset: str) -> str:
        """Finds the best matching key in preset_config for a given dataset name."""
        if not dataset:
            return ""

        def normalize(s):
            # Normalize for robust matching: uppercase, no spaces, no underscores
            return s.upper().replace(" ", "").replace("_", "")

        ds_norm = normalize(dataset)

        # 1. Exact match
        if dataset in self.preset_config:
            return dataset

        # 2. Normalized exact match or partial match
        for k in self.preset_config.keys():
            k_norm = normalize(k)
            # Match if normalized keys are identical
            if k_norm == ds_norm:
                return k
            # Match if one is a qualified version of the other (bi-directional)
            # e.g. "AURORA_APP.EMPLOYEE" matches "EMPLOYEE"
            if (
                ds_norm.endswith("." + k_norm)
                or k_norm.endswith("." + ds_norm)
                or ds_norm.endswith(k_norm)
                or k_norm.endswith(ds_norm)
            ):
                # Extra safety: ensure it's not a tiny substring match accidentally
                if len(k_norm) > 3 and len(ds_norm) > 3:
                    return k

        return ""

    async def execute_preset_queries(self, force: bool = False):
        """
        Executes the preset queries for the currently selected dataset via the backend.
        Hooks into AppState to respect global Date/Partition filters.
        """
        from frontend.state import AppState

        self.fetch_presets_config()
        app_state = await self.get_state(AppState)
        dataset = app_state.selected_dataset

        part_filters = str(app_state.selected_partitions.get(dataset, []))
        part_col = app_state.partition_column_name or ""
        current_hash = f"{dataset}|{part_filters}|{part_col}"

        if not force and current_hash == self._last_executed_hash:
            return

        self._last_executed_hash = current_hash

        config_key = self._get_config_key(dataset)

        if not config_key:
            print(
                f"[PRESET DEBUG] No config key found for dataset='{dataset}'. Available keys: {list(self.preset_config.keys())}"
            )
            self.chart_data = {}
            self.current_presets = []  # Reset to dummy 4 slots
            self.update_current_presets(dataset)  # Refresh dummy items
            return

        self.is_loading_presets = True
        yield

        presets_for_dataset = self.preset_config[config_key].get("presets", [])

        # Construct custom WHERE clause bridging AppState partition selections to raw SQL presets
        part_filters = app_state.selected_partitions.get(dataset, [])
        part_col = app_state.partition_column_name or ""
        part_type = app_state.partition_load_type or ""
        part_info = app_state.partition_info or {}
        part_type_col = part_info.get("load_type_column", "")

        where_conds = []
        query_params = {}

        if not app_state.partition_unrestricted:
            if part_col and part_filters:
                placeholders = []
                for i, v in enumerate(part_filters):
                    p_name = f"part_{i}"
                    placeholders.append(f":{p_name}")
                    # Preserve numeric types so Oracle NUMBER bindings work intuitively
                    query_params[p_name] = v if isinstance(v, (int, float)) else str(v)

                in_list = ", ".join(placeholders)
                where_conds.append(f'"{part_col.upper()}" IN ({in_list})')

            if part_type_col and part_type:
                where_conds.append(f'"{part_type_col.upper()}" = :part_type')
                query_params["part_type"] = (
                    part_type if isinstance(part_type, (int, float)) else str(part_type)
                )

        if where_conds:
            where_clause = "WHERE " + " AND ".join(where_conds)
        else:
            where_clause = ""

        results = {}
        try:
            async with httpx.AsyncClient() as client:
                for preset in presets_for_dataset:
                    # Robust replacement: handle case sensitivity and whitespace
                    sql_template = preset.get("sql", "")
                    raw_sql = sql_template.replace(
                        "{WHERE_CLAUSE}", where_clause
                    ).replace("{TABLE_NAME}", dataset)

                    # Executing preset query
                    # Call the raw query endpoint
                    print(
                        f"[PRESET SQL DEBUG] id={preset.get('id')}, sql={raw_sql[:200]}, params={query_params}"
                    )
                    res = await client.post(
                        f"{API_BASE_URL}/query/raw",
                        json={
                            "sql": raw_sql,
                            "dataset": dataset,
                            "params": query_params,
                        },
                        timeout=PRESET_RAW_QUERY_TIMEOUT,
                    )

                    if res.status_code == 200:
                        raw_data = res.json().get("data", [])

                        # Normalize keys for Recharts (Oracle returns UPPERCASE by default)
                        # We convert all keys to lowercase to match our config properties which we'll also lowercase
                        normalized_data = []
                        for row in raw_data:
                            normalized_data.append(
                                {k.lower(): v for k, v in row.items()}
                            )

                        results[preset["id"]] = normalized_data
                    else:
                        error_detail = ""
                        try:
                            error_detail = res.text[:500]
                        except Exception:
                            error_detail = f"HTTP {res.status_code}"
                        print(
                            f"Preset query '{preset['id']}' failed (HTTP {res.status_code}): {error_detail}"
                        )
                        results[preset["id"]] = []

            self.chart_data = results
            print(
                f"[PRESET DEBUG] Results for dataset '{dataset}': { {k: len(v) for k, v in results.items()} }"
            )
            self.update_current_presets(dataset)
        except Exception as e:
            from frontend.state import AppState

            app_state = await self.get_state(AppState)
            app_state.error_message = f"Preset Execution Error: {str(e)}"
            print(f"Error executing preset queries: {e}")
            self.chart_data = {}
            yield rx.toast.error(
                f"Preset Error: {str(e)}", position="bottom-right", duration=10000
            )
        finally:
            self.is_loading_presets = False

    def update_current_presets(self, dataset: str):
        """Updates the current_presets list for the given dataset."""
        raw_presets = []
        if dataset:
            config_key = self._get_config_key(dataset)
            if config_key:
                raw_presets = self.preset_config[config_key].get("presets", [])

        # Ensure exactly 4 items for the 2x2 grid
        final = []
        for i in range(4):
            if i < len(raw_presets):
                preset = raw_presets[i].copy()
                data = self.chart_data.get(preset["id"], [])
                # Extract dynamic rendering configurations or provide defaults
                x_axis_col = str(preset.get("x_axis_col", "x_axis")).lower()
                y_axis_cols = preset.get("y_axis_cols", ["y_axis"])
                y_axis_cols = [str(y).lower() for y in y_axis_cols]
                show_legend = preset.get("show_legend", False)
                primary_color = preset.get("primary_color", COLORS["primary"])
                color_palette = preset.get("color_palette", VIBRANT_PALETTE)

                preset["x_axis_col"] = x_axis_col
                preset["y_axis_cols"] = y_axis_cols
                preset["show_legend"] = show_legend
                preset["primary_color"] = primary_color
                preset["color_palette"] = color_palette
                preset["results"] = data

                # Pre-calculate Plotly-ready arrays to avoid client-side Var mapping issues.
                # (Support only the first y_axis_col for Plotly fallbacks for now)
                primary_y_col = y_axis_cols[0] if y_axis_cols else "y_axis"
                x_vals = [r.get(x_axis_col) for r in data]
                y_vals = [r.get(primary_y_col) for r in data]

                layout = {
                    "autosize": True,
                    "margin": {"t": 10, "b": 30, "l": 40, "r": 10},
                    "paper_bgcolor": "rgba(0,0,0,0)",
                    "plot_bgcolor": "rgba(0,0,0,0)",
                    "showlegend": show_legend,
                    "hovermode": False,  # disable tooltip interaction
                    "dragmode": False,  # disable drag zoom
                }

                # Create actual Figure objects to satisfy Reflex 0.8.27 strict typing
                preset["plotly_scatter_fig"] = go.Figure(
                    data=[
                        go.Scatter(
                            x=x_vals,
                            y=y_vals,
                            mode="markers",
                            marker=dict(color=VIBRANT_PALETTE[0]),
                        )
                    ],
                    layout=layout,
                )
                print(
                    f"[PRESET DEBUG] Preset '{preset.get('id')}': x_vals({len(x_vals)}), y_vals({len(y_vals)}), type={preset.get('type')}"
                )

                preset["plotly_bar_fig"] = go.Figure(
                    data=[
                        go.Bar(
                            x=x_vals, y=y_vals, marker=dict(color=VIBRANT_PALETTE[0])
                        )
                    ],
                    layout=layout,
                )

                preset["plotly_area_fig"] = go.Figure(
                    data=[
                        go.Scatter(
                            x=x_vals,
                            y=y_vals,
                            fill="tozeroy",
                            marker=dict(color=VIBRANT_PALETTE[0]),
                        )
                    ],
                    layout=layout,
                )

                preset["plotly_horizontal_bar_fig"] = go.Figure(
                    data=[
                        go.Bar(
                            x=y_vals,
                            y=x_vals,
                            orientation="h",
                            marker=dict(color=VIBRANT_PALETTE[0]),
                        )
                    ],
                    layout=layout,
                )

                preset["plotly_pie_fig"] = go.Figure(
                    data=[
                        go.Pie(
                            labels=x_vals,
                            values=y_vals,
                            marker=dict(colors=color_palette),
                        )
                    ],
                    layout=layout,
                )

                stacked_data = []
                for idx, col in enumerate(y_axis_cols):
                    col_y_vals = [r.get(col) for r in data]
                    color = color_palette[idx % len(color_palette)]
                    stacked_data.append(
                        go.Bar(
                            name=col, x=x_vals, y=col_y_vals, marker=dict(color=color)
                        )
                    )

                stacked_layout = layout.copy()
                stacked_layout["barmode"] = "stack"
                preset["plotly_stacked_bar_fig"] = go.Figure(
                    data=stacked_data, layout=stacked_layout
                )
                final.append(preset)
            else:
                final.append(
                    {
                        "id": f"dummy_{i}",
                        "title": f"Preset {i + 1}",
                        "description": "No configuration found in presets.json",
                        "type": "area",
                        "results": [],
                        "plotly_scatter_fig": go.Figure(),
                        "plotly_bar_fig": go.Figure(),
                        "plotly_area_fig": go.Figure(),
                        "plotly_horizontal_bar_fig": go.Figure(),
                        "plotly_pie_fig": go.Figure(),
                        "plotly_stacked_bar_fig": go.Figure(),
                    }
                )
        self.current_presets = final
