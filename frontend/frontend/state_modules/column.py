import reflex as rx
import httpx
from typing import List, Dict, Any, Optional
import os

# The base URL where our FastAPI backend is running
API_BASE_URL = os.getenv("API_BASE_URL", "http://localhost:8080/api/v1")

from .base import BaseState


class ColumnState(BaseState):
    """Manages the visibility and typing of columns for the selected dataset."""

    async def toggle_column_visibility(self, col_name: str):
        """Toggle column visibility on/off and refresh preview to fetch data for new columns."""
        if col_name in self.visible_columns:
            self.visible_columns.remove(col_name)
        else:
            self.visible_columns.append(col_name)
        from frontend.state import AppState

        yield AppState.execute_query()

    async def select_all_columns(self):
        """Visible all columns available in the active dataset and refresh data."""
        self.visible_columns = [col["name"] for col in self.columns]
        from frontend.state import AppState

        yield AppState.execute_query()

    async def unselect_all_columns(self):
        """Unselect all columns and refresh data."""
        self.visible_columns = []
        from frontend.state import AppState

        yield AppState.execute_query()

    async def toggle_all_columns(self):
        """Toggle all columns visible or hidden."""
        if len(self.visible_columns) == len(self.columns):
            await self.unselect_all_columns()
        else:
            await self.select_all_columns()

    # Advanced Filtering State
    # Represents the root LogicalGroup: {"logic": "AND", "conditions": []}
    active_filters: Dict[str, Any] = {"type": "group", "logic": "AND", "conditions": []}
    is_filter_modal_open: bool = False

    # UI Search State
    column_search_text: str = ""
    dataset_search_text: str = ""
    search_value_text: str = ""

    async def clear_column_filters(self):
        """Resets the visible columns and search string."""
        self.column_search_text = ""
        self.visible_columns = [col["name"] for col in self.columns]
        yield
        from frontend.state import AppState

        yield AppState.execute_query()

    def set_column_search_text(self, text: str):
        self.column_search_text = text

    def set_dataset_search_text(self, text: str):
        self.dataset_search_text = text

    def clear_dataset_search(self):
        self.dataset_search_text = ""

    def set_is_join_modal_open(self, is_open: bool):
        self.is_join_modal_open = is_open

    def set_new_join_type(self, join_type: str):
        self.new_join_type = join_type

    def set_new_join_left_dataset(self, dataset_name: str):
        self.new_join_left_dataset = dataset_name

    def set_search_value_text(self, text: str):
        self.search_value_text = text
