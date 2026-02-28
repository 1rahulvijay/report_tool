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

    # Modal Search State
    join_table_search: str = ""
    join_left_col_search: str = ""
    join_right_col_search: str = ""
    agg_group_by_search: str = ""
    agg_metrics_search: str = ""
    filter_col_search: str = ""

    # IN clause paste modal state
    in_clause_modal_open: bool = False
    in_clause_paste_text: str = ""
    in_clause_filter_path: list[int] = []

    def set_join_table_search(self, text: str):
        self.join_table_search = text

    def set_join_left_col_search(self, text: str):
        self.join_left_col_search = text

    def set_join_right_col_search(self, text: str):
        self.join_right_col_search = text

    def set_agg_group_by_search(self, text: str):
        self.agg_group_by_search = text

    def set_agg_metrics_search(self, text: str):
        self.agg_metrics_search = text

    def set_filter_col_search(self, text: str):
        self.filter_col_search = text

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
