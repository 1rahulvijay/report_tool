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
    """Renders a column header cell — table_headers already provides display-ready names."""
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
        # Top Action Bar — Row 1: Global Search + Action Buttons
        rx.box(
            # Search Bar
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
            # Action Buttons: JOIN | FILTERS | BUILDER
            rx.box(
                rx.button(
                    rx.icon(tag="link", size=16, class_name="text-slate-500"),
                    "JOIN",
                    on_click=AppState.toggle_join_modal,
                    class_name="flex items-center gap-2 text-xs font-bold text-slate-600 hover:text-primary transition-colors bg-transparent border-none cursor-pointer tracking-wider uppercase",
                ),
                rx.box(class_name="h-5 w-px bg-slate-200 dark:bg-slate-700"),
                rx.button(
                    rx.icon(tag="filter", size=16, class_name="text-slate-500"),
                    "FILTERS",
                    on_click=AppState.toggle_filter_modal,
                    class_name="flex items-center gap-2 text-xs font-bold text-slate-600 hover:text-primary transition-colors bg-transparent border-none cursor-pointer tracking-wider uppercase",
                ),
                rx.box(class_name="h-5 w-px bg-slate-200 dark:bg-slate-700"),
                rx.button(
                    rx.icon(tag="diamond", size=16, class_name="text-slate-500"),
                    "BUILDER",
                    on_click=AppState.toggle_aggregation_modal,
                    class_name="flex items-center gap-2 text-xs font-bold text-slate-600 hover:text-primary transition-colors bg-transparent border-none cursor-pointer tracking-wider uppercase",
                ),
                class_name="flex items-center gap-5 shrink-0",
            ),
            class_name="flex items-center justify-between gap-10 px-4 py-2.5 border-b border-slate-100 dark:border-slate-800/50 shrink-0",
        ),
        # Inject modals
        filter_modal(),
        join_modal(),
        aggregation_modal(),
        data_vintage_bar(),
        # Loading spinner overlay
        rx.cond(
            AppState.is_loading,
            rx.box(
                rx.box(
                    rx.icon(
                        tag="loader", class_name="animate-spin text-primary", size=24
                    ),
                    rx.text(
                        "Loading data...",
                        class_name="text-sm font-bold text-slate-500 mt-2",
                    ),
                    class_name="flex flex-col items-center justify-center",
                ),
                class_name="absolute inset-0 z-20 flex items-center justify-center bg-white/70 dark:bg-slate-900/70 backdrop-blur-sm",
            ),
        ),
        # The Table Area (scrollable content)
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
            class_name="flex-1 overflow-x-auto overflow-y-auto custom-scrollbar custom-scrollbar-force-x px-3 min-h-0",
        ),
        # Sticky Pagination Footer
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
                class_name="h-16 px-3 border-t border-slate-100 dark:border-slate-800 flex items-center justify-between bg-white dark:bg-[#0f172a] shrink-0 w-full z-20",
            ),
        ),
        class_name="flex-1 overflow-hidden flex flex-col min-h-0 h-full w-full bg-white dark:bg-[#0f172a] relative",
    )
