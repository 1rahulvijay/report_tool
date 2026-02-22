import reflex as rx
from frontend.state import AppState
from frontend.components.join_builder import join_modal
from frontend.components.filter_modal import filter_modal
from frontend.components.aggregation_builder import aggregation_modal
from frontend.components.data_vintage import data_vintage_bar


def _render_row(row_data: list) -> rx.Component:
    """Renders a single row in the data grid matching the HTML."""
    return rx.table.row(
        rx.table.cell(
            rx.checkbox(
                class_name="w-4 h-4 rounded border-slate-300 dark:border-slate-700 bg-transparent text-primary",
            ),
            class_name="py-4 px-2 text-center",
        ),
        rx.foreach(
            row_data,
            lambda cell: rx.table.cell(
                cell,
                class_name="py-4 px-4 text-[13px] font-semibold text-slate-700 dark:text-slate-300",
            ),
        ),
        class_name="hover:bg-slate-50/80 dark:hover:bg-slate-900/60 transition-colors group",
    )


def _render_header(col_name: str) -> rx.Component:
    """Renders a column header cell representing the HTML sortable columns."""
    return rx.table.column_header_cell(
        rx.box(
            rx.text(col_name),
            rx.icon(
                tag="arrow-down-up",
                size=14,
                class_name="opacity-0 group-hover:opacity-100 transition-opacity",
            ),
            class_name="flex items-center gap-1.5",
        ),
        class_name="py-5 px-4 table-header-cell font-bold text-slate-400 uppercase tracking-[0.1em] group cursor-pointer hover:text-primary transition-colors",
    )


def datagrid() -> rx.Component:
    """The main dynamic workspace table reflecting the provided layout."""
    return rx.box(
        # Top Action Bar
        rx.box(
            rx.box(
                rx.box(
                    rx.box(
                        rx.icon(
                            tag="database",
                            class_name="text-slate-400 text-lg",
                        ),
                        rx.heading(
                            rx.cond(
                                AppState.selected_dataset == "",
                                "No Dataset",
                                AppState.selected_dataset,
                            ),
                            class_name="text-xl font-bold tracking-tight text-slate-900 dark:text-white uppercase",
                        ),
                        rx.cond(
                            AppState.has_partition_info,
                            rx.menu.root(
                                rx.menu.trigger(
                                    rx.button(
                                        rx.box(
                                            rx.text(
                                                AppState.partition_load_type,
                                                class_name="text-[9px] font-bold text-slate-500 uppercase tracking-widest",
                                            ),
                                            rx.text(
                                                AppState.current_load_id_display,
                                                class_name="text-sm font-bold text-slate-700 dark:text-slate-200",
                                            ),
                                            class_name="flex flex-col items-start leading-none gap-0.5",
                                        ),
                                        rx.icon(
                                            tag="chevron-down",
                                            size=14,
                                            class_name="text-slate-400 ml-1",
                                        ),
                                        class_name="ml-3 flex items-center gap-1 px-3 py-1.5 bg-white dark:bg-slate-900 border border-slate-200 dark:border-slate-700 rounded-lg hover:bg-slate-50 dark:hover:bg-slate-800 transition-colors cursor-pointer shadow-sm",
                                    )
                                ),
                                rx.menu.content(
                                    rx.foreach(
                                        AppState.partition_available_values,
                                        lambda val: rx.menu.item(
                                            val,
                                            on_click=lambda: (
                                                AppState.set_current_load_id(val)
                                            ),
                                            class_name=rx.cond(
                                                val == AppState.current_load_id_display,
                                                "font-bold text-primary bg-primary/5 cursor-pointer",
                                                "cursor-pointer",
                                            ),
                                        ),
                                    )
                                ),
                            ),
                        ),
                        class_name="flex items-center gap-2 mb-1",
                    ),
                    rx.text(
                        "Enterprise Data Management & Real-time Analytics",
                        class_name="text-xs text-slate-400 font-medium",
                    ),
                ),
                rx.box(
                    rx.cond(
                        AppState.use_oracle_in_memory,
                        rx.button(
                            rx.box(
                                rx.icon(tag="zap", size=14, class_name="font-bold"),
                                class_name="flex items-center justify-center w-6 h-6 rounded-full bg-blue-500 text-white shadow-lg shadow-blue-500/40",
                            ),
                            rx.box(
                                rx.text(
                                    "In-Memory",
                                    class_name="text-[10px] font-bold text-slate-900 dark:text-white uppercase tracking-wider",
                                ),
                                rx.text(
                                    "ACTIVE",
                                    class_name="text-[9px] text-blue-500 font-semibold",
                                ),
                                class_name="flex flex-col items-start leading-none",
                            ),
                            on_click=AppState.toggle_oracle_in_memory,
                            class_name="flex items-center gap-2.5 px-4 py-2 bg-white dark:bg-slate-900 border border-slate-200 dark:border-slate-700 rounded-full in-memory-active transition-all group cursor-pointer",
                        ),
                        rx.button(
                            rx.box(
                                rx.icon(tag="zap-off", size=14, class_name="font-bold"),
                                class_name="flex items-center justify-center w-6 h-6 rounded-full bg-slate-400 text-white shadow-lg",
                            ),
                            rx.box(
                                rx.text(
                                    "In-Memory",
                                    class_name="text-[10px] font-bold text-slate-900 dark:text-white uppercase tracking-wider",
                                ),
                                rx.text(
                                    "INACTIVE",
                                    class_name="text-[9px] text-slate-500 font-semibold",
                                ),
                                class_name="flex flex-col items-start leading-none",
                            ),
                            on_click=AppState.toggle_oracle_in_memory,
                            class_name="flex items-center gap-2.5 px-4 py-2 bg-white dark:bg-slate-900 border border-slate-200 dark:border-slate-700 rounded-full transition-all group cursor-pointer",
                        ),
                    ),
                    rx.box(class_name="h-8 w-px bg-slate-200 dark:bg-slate-800 mx-1"),
                    rx.cond(
                        AppState.is_virtual_scroll,
                        rx.button(
                            rx.icon(tag="layers", size=18),
                            "Virtual",
                            on_click=AppState.toggle_virtual_scroll,
                            class_name="px-3.5 py-2 bg-slate-100 dark:bg-slate-800 text-slate-700 dark:text-slate-300 text-xs font-bold rounded-lg hover:bg-slate-200 dark:hover:bg-slate-700 transition-colors flex items-center gap-2 cursor-pointer",
                        ),
                        rx.button(
                            rx.icon(tag="layers", size=18),
                            "Paginated",
                            on_click=AppState.toggle_virtual_scroll,
                            class_name="px-3.5 py-2 bg-slate-100 dark:bg-slate-800 text-slate-700 dark:text-slate-300 text-xs font-bold rounded-lg hover:bg-slate-200 dark:hover:bg-slate-700 transition-colors flex items-center gap-2 cursor-pointer",
                        ),
                    ),
                    rx.menu.root(
                        rx.menu.trigger(
                            rx.button(
                                rx.icon(tag="download", size=18),
                                "Export",
                                class_name="px-3.5 py-2 bg-slate-900 dark:bg-slate-100 text-white dark:text-slate-900 text-xs font-bold rounded-lg hover:bg-slate-800 dark:hover:bg-white transition-colors flex items-center gap-2 shadow-sm cursor-pointer border-none",
                            )
                        ),
                        rx.menu.content(
                            rx.menu.item(
                                "Export as Excel (.xlsx)",
                                on_click=AppState.export_excel,
                                class_name=rx.cond(
                                    AppState.can_export_excel,
                                    "cursor-pointer",
                                    "cursor-not-allowed text-slate-400",
                                ),
                            ),
                            rx.menu.item(
                                "Export as CSV (.csv)",
                                on_click=AppState.export_csv,
                                class_name="cursor-pointer",
                            ),
                        ),
                    ),
                    rx.button(
                        rx.icon(tag="more-vertical", size=20),
                        class_name="p-2 text-slate-400 hover:bg-slate-50 dark:hover:bg-slate-800 rounded-lg transition-all bg-transparent border-none cursor-pointer",
                    ),
                    class_name="flex items-center gap-3",
                ),
                class_name="flex items-center justify-between mb-8",
            ),
            # Filter Action Bar
            rx.box(
                rx.box(
                    rx.box(
                        rx.icon(tag="search", size=18, class_name="text-slate-400"),
                        class_name="absolute inset-y-0 left-0 pl-4 flex items-center pointer-events-none",
                    ),
                    rx.input(
                        placeholder="Global search filters...",
                        value=AppState.search_value_text,
                        on_change=AppState.set_search_value_text,
                        class_name="block w-full pl-11 pr-16 py-2.5 bg-slate-50 dark:bg-slate-900/80 border border-slate-200 dark:border-slate-800 focus:border-primary/50 rounded-xl text-sm focus:ring-4 focus:ring-primary/5 shadow-inner transition-all placeholder:text-slate-400 placeholder:font-medium outline-none",
                    ),
                    rx.box(
                        rx.text(
                            "⌘F",
                            class_name="hidden sm:inline-block px-1.5 py-0.5 text-[10px] font-bold text-slate-400 bg-white dark:bg-slate-800 border border-slate-200 dark:border-slate-700 rounded shadow-sm",
                        ),
                        class_name="absolute inset-y-0 right-0 flex items-center pr-3",
                    ),
                    class_name="relative flex-1 max-w-xl",
                ),
                rx.box(
                    rx.button(
                        rx.icon(
                            tag="git-pull-request", size=18, class_name="text-slate-400"
                        ),
                        "Join Datasets",
                        on_click=AppState.toggle_join_modal,
                        class_name="flex items-center gap-2 text-xs font-bold text-slate-500 hover:text-slate-900 dark:hover:text-white transition-colors bg-transparent border-none cursor-pointer",
                    ),
                    rx.button(
                        rx.icon(tag="filter", size=18, class_name="text-slate-400"),
                        "Filters",
                        on_click=AppState.toggle_filter_modal,
                        class_name="flex items-center gap-2 text-xs font-bold text-slate-500 hover:text-slate-900 dark:hover:text-white transition-colors bg-transparent border-none cursor-pointer",
                    ),
                    rx.button(
                        rx.icon(tag="layers", size=18, class_name="text-slate-400"),
                        "Aggregation Builder",
                        on_click=AppState.toggle_aggregation_modal,
                        class_name="flex items-center gap-2 text-xs font-bold text-slate-500 hover:text-slate-900 dark:hover:text-white transition-colors bg-transparent border-none cursor-pointer",
                    ),
                    rx.cond(
                        AppState.has_active_filters,
                        rx.button(
                            "Clear Filters",
                            on_click=AppState.clear_filters,
                            class_name="flex items-center gap-2 text-xs font-bold text-red-400 hover:text-red-500 transition-colors bg-transparent border-none cursor-pointer",
                        ),
                    ),
                    rx.cond(
                        AppState.joins.length() > 0,
                        rx.button(
                            "Clear Joins",
                            on_click=AppState.reset_joins,
                            class_name="flex items-center gap-2 text-xs font-bold text-red-400 hover:text-red-500 transition-colors bg-transparent border-none cursor-pointer",
                        ),
                    ),
                    rx.cond(
                        AppState.aggregations.length() > 0,
                        rx.button(
                            "Clear Aggregations",
                            on_click=AppState.clear_aggregations,
                            class_name="flex items-center gap-2 text-xs font-bold text-red-400 hover:text-red-500 transition-colors bg-transparent border-none cursor-pointer",
                        ),
                    ),
                    rx.cond(
                        AppState.has_active_filters
                        | (AppState.joins.length() > 0)
                        | (AppState.aggregations.length() > 0),
                        rx.button(
                            rx.icon(tag="x", size=18, class_name="text-red-500"),
                            "Reset All",
                            on_click=AppState.reset_all,
                            class_name="flex items-center gap-2 text-xs font-bold text-red-500 hover:text-red-600 transition-colors bg-transparent border-none cursor-pointer ml-3 border-l pl-3 border-slate-200 dark:border-slate-800",
                        ),
                    ),
                    class_name="flex items-center gap-6 shrink-0",
                ),
                class_name="flex items-center justify-between gap-10",
            ),
            class_name="px-8 pt-8 pb-6 shrink-0 border-b border-slate-100 dark:border-slate-800/50",
        ),
        # Inject modals
        filter_modal(),
        join_modal(),
        aggregation_modal(),
        data_vintage_bar(),
        # The Table Area
        rx.box(
            rx.cond(
                AppState.visible_columns.length() == 0,
                # Empty State
                rx.box(
                    rx.center(
                        rx.vstack(
                            rx.icon(
                                tag="layout-template",
                                size=64,
                                class_name="text-slate-200 mb-2",
                            ),
                            rx.heading(
                                "No columns selected",
                                size="4",
                                class_name="text-slate-400 font-bold",
                            ),
                            rx.text(
                                "Use the left sidebar to select columns you want to display in the grid.",
                                class_name="text-slate-400 text-sm max-w-xs text-center",
                            ),
                            rx.button(
                                "Select All Columns",
                                on_click=AppState.select_all_columns,
                                class_name="mt-4 px-4 py-2 bg-slate-50 hover:bg-slate-100 text-slate-500 rounded-lg text-sm font-medium border border-slate-200 transition-colors cursor-pointer",
                            ),
                            align="center",
                            spacing="1",
                        ),
                        class_name="h-full",
                    ),
                    class_name="flex-1 bg-[#f8fafc] flex items-center justify-center p-20",
                ),
                # Data Grid
                rx.table.root(
                    rx.table.header(
                        rx.table.row(
                            rx.table.column_header_cell(
                                rx.checkbox(
                                    class_name="w-4 h-4 rounded border-slate-300 dark:border-slate-700 bg-transparent text-primary"
                                ),
                                class_name="w-12 py-5 px-2",
                            ),
                            rx.foreach(AppState.table_headers, _render_header),
                            class_name="border-b border-slate-200 dark:border-slate-800",
                        ),
                        class_name="sticky top-0 z-10 glass-header",
                    ),
                    rx.table.body(
                        rx.foreach(AppState.table_data, _render_row),
                        rx.cond(
                            AppState.is_virtual_scroll,
                            rx.cond(
                                AppState.page_number < AppState.total_pages,
                                rx.table.row(
                                    rx.table.cell(
                                        rx.box(
                                            rx.cond(
                                                AppState.is_fetching_more,
                                                rx.spinner(
                                                    size="2", class_name="text-primary"
                                                ),
                                                rx.button(
                                                    "Load More Results",
                                                    on_click=AppState.next_page,
                                                    class_name="font-semibold text-primary hover:underline bg-transparent border-none cursor-pointer",
                                                ),
                                            ),
                                            class_name="flex items-center justify-center p-4 w-full",
                                        ),
                                        col_span=AppState.visible_columns.length() + 1,
                                    ),
                                ),
                            ),
                        ),
                        class_name="divide-y divide-slate-100 dark:divide-slate-800/40",
                    ),
                    class_name="w-full text-left border-collapse min-w-[1000px]",
                ),
            ),
            class_name="flex-1 overflow-auto custom-scrollbar px-8 min-h-0",
        ),
        # Pagination Footer (Hidden in Virtual Mode)
        rx.cond(
            AppState.is_virtual_scroll,
            rx.fragment(),
            rx.box(
                rx.box(
                    rx.text(
                        "Records: ",
                        rx.text(
                            AppState.query_results.length().to(str),
                            as_="span",
                            class_name="text-slate-900 dark:text-white",
                        ),
                        rx.text(
                            " / ",
                            as_="span",
                            class_name="mx-1 opacity-30 text-slate-400",
                        ),
                        rx.text(
                            f"{AppState.total_row_count} total",
                            as_="span",
                            class_name="text-slate-400",
                        ),
                        class_name="text-[11px] text-slate-500 font-bold uppercase tracking-wider",
                    ),
                ),
                rx.box(
                    rx.box(
                        rx.text(
                            "Rows per page",
                            class_name="text-[10px] uppercase tracking-widest text-slate-400",
                        ),
                        rx.select(
                            ["10", "50", "100", "200"],
                            value=AppState.page_size.to(str),
                            on_change=AppState.set_page_size,
                            class_name="bg-transparent border-none py-0 pl-0 pr-6 text-xs font-bold text-slate-700 dark:text-slate-200 focus:ring-0 cursor-pointer",
                        ),
                        class_name="flex items-center gap-4 text-xs font-bold text-slate-500",
                    ),
                    rx.box(class_name="h-4 w-px bg-slate-200 dark:bg-slate-800"),
                    rx.box(
                        rx.box(
                            rx.button(
                                rx.icon(tag="chevrons-left", size=20),
                                on_click=AppState.first_page,
                                class_name="p-1 text-slate-400 hover:text-primary transition-colors disabled:opacity-20 cursor-pointer bg-transparent border-none",
                            ),
                            rx.button(
                                rx.icon(tag="chevron-left", size=20),
                                on_click=AppState.prev_page,
                                class_name="p-1 text-slate-400 hover:text-primary transition-colors disabled:opacity-20 cursor-pointer bg-transparent border-none",
                            ),
                            class_name="flex items-center gap-1",
                        ),
                        rx.box(
                            rx.text(
                                "Page",
                                class_name="text-[10px] uppercase tracking-widest text-slate-400",
                            ),
                            rx.input(
                                value=AppState.page_number.to(str),
                                on_change=AppState.set_page_number,
                                class_name="w-9 h-7 text-center bg-slate-50 dark:bg-slate-900 border-slate-200 dark:border-slate-700 rounded text-slate-900 dark:text-white focus:ring-1 focus:ring-primary focus:border-primary text-xs outline-none",
                            ),
                            rx.text(
                                f"of {AppState.total_pages}",
                                class_name="text-slate-400 text-[10px] uppercase tracking-widest",
                            ),
                            class_name="flex items-center gap-2 text-xs font-bold text-slate-600 dark:text-slate-400",
                        ),
                        rx.box(
                            rx.button(
                                rx.icon(tag="chevron-right", size=20),
                                on_click=AppState.next_page,
                                class_name="p-1 text-slate-400 hover:text-primary transition-colors cursor-pointer bg-transparent border-none",
                            ),
                            rx.button(
                                rx.icon(tag="chevrons-right", size=20),
                                on_click=AppState.last_page,
                                class_name="p-1 text-slate-400 hover:text-primary transition-colors cursor-pointer bg-transparent border-none",
                            ),
                            class_name="flex items-center gap-1",
                        ),
                        class_name="flex items-center gap-4",
                    ),
                    class_name="flex items-center gap-6",
                ),
                class_name="h-16 px-8 border-t border-slate-100 dark:border-slate-800 flex items-center justify-between bg-white dark:bg-[#0f172a] shrink-0 w-full",
            ),
        ),
        class_name="flex-1 overflow-hidden flex flex-col min-h-0 h-full w-full bg-white dark:bg-[#0f172a]",
    )
