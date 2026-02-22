import reflex as rx
from frontend.state import AppState


def sidebar() -> rx.Component:
    return rx.box(
        rx.box(
            rx.box(
                rx.text(
                    "Active Dataset",
                    class_name="text-[10px] font-bold text-slate-400 uppercase tracking-widest mb-4 block",
                ),
                rx.box(
                    rx.box(
                        rx.icon(tag="table", size=20),
                        class_name="w-9 h-9 bg-white dark:bg-slate-800 border border-slate-200 dark:border-slate-700 rounded-lg flex items-center justify-center text-primary shadow-sm",
                    ),
                    rx.box(
                        rx.text(
                            rx.cond(
                                AppState.selected_dataset == "",
                                "No Dataset",
                                AppState.selected_dataset,
                            ),
                            class_name="text-xs font-bold text-slate-700 dark:text-slate-200 truncate uppercase",
                        ),
                        rx.text(
                            f"{AppState.total_row_count} records",
                            class_name="text-[10px] text-slate-400",
                        ),
                        class_name="flex flex-col overflow-hidden",
                    ),
                    class_name="bg-slate-50 dark:bg-slate-900/50 border border-slate-200 dark:border-slate-800 rounded-xl p-3.5 flex items-center gap-3 hover:border-primary/30 transition-colors cursor-pointer group",
                ),
                class_name="flex flex-col gap-2 shrink-0 mb-6",
            ),
            rx.box(
                rx.box(
                    rx.text(
                        "TABLE NAMES",
                        class_name="text-[10px] font-bold text-slate-400 uppercase tracking-widest block mb-4",
                    ),
                    class_name="flex flex-col w-full shrink-0",
                ),
                rx.box(
                    rx.icon(
                        tag="search",
                        size=18,
                        class_name="absolute left-3 top-1/2 -translate-y-1/2 text-slate-400 group-focus-within:text-primary transition-colors",
                    ),
                    rx.box(
                        rx.input(
                            placeholder="Find entities...",
                            value=AppState.dataset_search_text,
                            on_change=AppState.set_dataset_search_text,
                            class_name="w-full bg-slate-50 dark:bg-slate-950 border-slate-200 dark:border-slate-800 rounded-lg pl-10 pr-4 py-2 text-xs focus:ring-primary focus:border-primary focus:bg-white dark:focus:bg-slate-900 transition-all outline-none",
                        ),
                        rx.box(
                            rx.icon(tag="refresh-cw", size=14),
                            on_click=AppState.fetch_datasets,
                            class_name="absolute right-2 top-1/2 -translate-y-1/2 p-1 z-10 text-slate-400 hover:text-primary transition-colors cursor-pointer flex items-center justify-center",
                        ),
                        class_name="w-full relative",
                    ),
                    class_name="relative flex items-center group mb-5",
                ),
                rx.box(
                    rx.vstack(
                        rx.foreach(
                            AppState.filtered_datasets,
                            lambda name: rx.box(
                                rx.hstack(
                                    rx.cond(
                                        AppState.selected_dataset == name,
                                        rx.icon(
                                            tag="list", size=18, class_name="shrink-0"
                                        ),
                                        rx.icon(
                                            tag="database",
                                            size=18,
                                            class_name="shrink-0",
                                        ),
                                    ),
                                    rx.text(
                                        name,
                                        class_name="whitespace-nowrap text-left text-[11px] pr-2",
                                    ),
                                    align="center",
                                    spacing="2",
                                ),
                                on_click=lambda: AppState.select_dataset(name),
                                class_name=rx.cond(
                                    AppState.selected_dataset == name,
                                    "flex items-center w-max min-w-full text-left px-3 py-2 rounded-lg bg-blue-50 dark:bg-blue-900/20 text-blue-600 dark:text-blue-400 text-xs font-semibold cursor-pointer border-none",
                                    "flex items-center w-max min-w-full text-left px-3 py-2 rounded-lg hover:bg-slate-50 dark:hover:bg-slate-900/50 text-slate-500 dark:text-slate-400 text-xs transition-colors bg-transparent border-none cursor-pointer",
                                ),
                            ),
                        ),
                        spacing="1",
                        width="100%",
                    ),
                    class_name="flex-1 min-h-0 overflow-y-auto overflow-x-auto custom-scrollbar pr-2 border border-slate-200 dark:border-slate-800 rounded-lg p-2",
                ),
                class_name="h-[35%] flex flex-col mb-4 min-h-0 shrink-0",
            ),
            rx.box(
                rx.cond(
                    AppState.selected_dataset != "",
                    rx.box(
                        rx.box(
                            rx.hstack(
                                rx.text(
                                    "COLUMN NAMES",
                                    class_name="text-[10px] font-bold text-slate-400 uppercase tracking-widest whitespace-nowrap",
                                ),
                                rx.hstack(
                                    rx.cond(
                                        AppState.columns_changed_from_all,
                                        rx.button(
                                            "SELECT ALL",
                                            on_click=AppState.select_all_columns,
                                            class_name="text-[9px] px-1.5 py-1 bg-slate-100 dark:bg-slate-800 hover:bg-slate-200 dark:hover:bg-slate-700 text-slate-600 dark:text-slate-300 font-bold rounded flex items-center justify-center transition-colors cursor-pointer h-5 min-h-[20px] shadow-none outline-none focus:outline-none shrink-0 whitespace-nowrap",
                                        ),
                                        rx.button(
                                            "SELECT ALL",
                                            on_click=AppState.select_all_columns,
                                            class_name="text-[9px] px-1.5 py-1 bg-slate-100 dark:bg-slate-800 hover:bg-slate-200 dark:hover:bg-slate-700 text-slate-600 dark:text-slate-300 font-bold rounded flex items-center justify-center transition-colors cursor-pointer h-5 min-h-[20px] shadow-none outline-none focus:outline-none shrink-0 whitespace-nowrap",
                                        ),
                                    ),
                                    rx.cond(
                                        AppState.columns_changed_from_all,
                                        rx.button(
                                            "RESET",
                                            on_click=AppState.select_all_columns,
                                            class_name="text-[9px] px-1.5 py-1 bg-orange-50 hover:bg-orange-100 text-orange-600 font-bold rounded flex items-center justify-center border border-orange-200 transition-colors cursor-pointer h-5 min-h-[20px] shadow-none outline-none focus:outline-none shrink-0 whitespace-nowrap",
                                        ),
                                        rx.button(
                                            "UNSELECT ALL",
                                            on_click=AppState.unselect_all_columns,
                                            class_name="text-[9px] px-1.5 py-1 bg-slate-100 dark:bg-slate-800 hover:bg-slate-200 dark:hover:bg-slate-700 text-slate-600 dark:text-slate-300 font-bold rounded flex items-center justify-center transition-colors cursor-pointer h-5 min-h-[20px] shadow-none outline-none focus:outline-none shrink-0 whitespace-nowrap",
                                        ),
                                    ),
                                    align="center",
                                    spacing="1",
                                    class_name="shrink-0",
                                ),
                                align="center",
                                justify="between",
                                class_name="w-full mb-4 shrink-0 px-1 py-1 gap-y-2 flex-wrap",
                            ),
                            rx.box(
                                rx.icon(
                                    tag="search",
                                    size=18,
                                    class_name="absolute left-3 top-1/2 -translate-y-1/2 text-slate-400 group-focus-within:text-primary transition-colors",
                                ),
                                rx.input(
                                    placeholder="Search columns...",
                                    value=AppState.column_search_text,
                                    on_change=AppState.set_column_search_text,
                                    class_name="w-full bg-slate-50 dark:bg-slate-950 border-slate-200 dark:border-slate-800 rounded-lg pl-10 pr-4 py-2 text-xs focus:ring-primary focus:border-primary focus:bg-white dark:focus:bg-slate-900 transition-all outline-none",
                                ),
                                class_name="relative group mb-5 shrink-0",
                            ),
                            rx.box(
                                rx.vstack(
                                    rx.foreach(
                                        AppState.filtered_columns, column_toggle_item
                                    ),
                                    spacing="3",
                                    width="100%",
                                ),
                                class_name="space-y-3 flex-1 min-h-0 overflow-y-auto custom-scrollbar pr-2 border border-slate-200 dark:border-slate-800 rounded-lg p-2",
                            ),
                            class_name="flex flex-col min-h-0 h-full",
                        ),
                        class_name="h-full",
                    ),
                    rx.box(),
                ),
                class_name="flex-1 h-[65%] flex flex-col min-h-0",
            ),
            class_name="p-4 flex flex-col h-full overflow-hidden",
        ),
        class_name="w-[250px] border-r border-slate-200 dark:border-slate-800 bg-white dark:bg-[#0b1120] flex flex-col shrink-0 z-10 h-full max-h-screen relative",
    )


def column_toggle_item(column: dict) -> rx.Component:
    name = column["name"]
    is_visible = AppState.visible_columns.contains(name)
    return rx.box(
        rx.box(
            rx.checkbox(
                class_name="w-3.5 h-3.5 rounded border-slate-300 dark:border-slate-700 text-primary focus:ring-primary bg-transparent",
                checked=is_visible,
                on_change=lambda _: AppState.toggle_column_visibility(name),
                is_disabled=rx.cond(
                    AppState.aggregations.length() > 0,
                    ~AppState.aggregation_group_by.contains(name),
                    False,
                ),
            ),
            rx.text(
                name,
                class_name="text-[10px] font-medium text-slate-600 dark:text-slate-400 truncate",
            ),
            class_name="flex items-center gap-3",
        ),
        rx.icon(
            tag="grip-vertical",
            size=14,
            class_name="text-slate-300 opacity-0 group-hover:opacity-100",
        ),
        class_name=rx.cond(
            rx.cond(
                AppState.aggregations.length() > 0,
                ~AppState.aggregation_group_by.contains(name),
                False,
            ),
            "flex items-center justify-between group cursor-not-allowed opacity-50",
            "flex items-center justify-between group cursor-pointer",
        ),
    )
