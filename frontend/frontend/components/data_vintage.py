"""
Data Vintage control bar — shows Load Type badge, Load ID dropdown,
Time Column pill, Re-Fetch button, and All Partitions toggle.
Matches the reference design: horizontal bar with labeled sections separated by dividers.
"""

import reflex as rx
from frontend.state import AppState
from frontend.config import UI_CONFIG


def data_vintage_bar() -> rx.Component:
    """Relocated control bar: Data Vintage (left) and Action Buttons (right)."""
    return rx.box(
        rx.hstack(
            # LEFT SIDE: Persistent container for Data Vintage and Clear Buttons
            rx.hstack(
                rx.cond(
                    AppState.has_partition_info,
                    rx.hstack(
                        # Load Type section
                        rx.cond(
                            AppState.has_load_type
                            & (AppState.partition_load_type != ""),
                            rx.hstack(
                                rx.text(
                                    "LOAD TYPE:",
                                    class_name="text-[11px] font-bold text-slate-500 uppercase tracking-wide whitespace-nowrap",
                                ),
                                rx.select(
                                    AppState.partition_supported_types,
                                    value=AppState.partition_load_type,
                                    on_change=AppState.set_partition_load_type,
                                    class_name="px-3 py-1 bg-amber-50 text-amber-700 border border-amber-200 font-bold text-[11px] rounded-full uppercase tracking-wide cursor-pointer focus:ring-0 min-w-[90px]",
                                    size="1",
                                ),
                                class_name="flex items-center gap-2 shrink-0",
                            ),
                        ),
                        # Divider
                        rx.cond(
                            AppState.has_load_type
                            & (AppState.partition_load_type != ""),
                            rx.box(class_name="w-px h-5 bg-slate-200 mx-2 shrink-0"),
                        ),
                        # Load ID section
                        rx.cond(
                            AppState.has_load_id,
                            rx.hstack(
                                rx.text(
                                    "LOAD ID:",
                                    class_name="text-[11px] font-bold text-slate-500 uppercase tracking-wide whitespace-nowrap",
                                ),
                                rx.select(
                                    AppState.partition_available_values,
                                    value=AppState.current_load_id_display,
                                    on_change=AppState.set_current_load_id,
                                    class_name="text-[11px] font-semibold min-w-[180px] bg-white border border-slate-200 rounded-md py-1",
                                    size="1",
                                ),
                                rx.cond(
                                    AppState.is_loading,
                                    rx.icon(
                                        tag="loader-circle",
                                        size=14,
                                        class_name="animate-spin text-primary",
                                    ),
                                ),
                                class_name="flex items-center gap-2 shrink-0",
                            ),
                        ),
                        # Divider
                        rx.cond(
                            AppState.has_load_id,
                            rx.box(class_name="w-px h-5 bg-slate-200 mx-2 shrink-0"),
                        ),
                        # Time Column pill
                        rx.cond(
                            AppState.has_date_column,
                            rx.hstack(
                                rx.text(
                                    "TIME COLUMN:",
                                    class_name="text-[11px] font-bold text-slate-500 uppercase tracking-wide whitespace-nowrap",
                                ),
                                rx.box(
                                    AppState.partition_column_name,
                                    class_name="px-3 py-0.5 bg-slate-100 text-slate-600 border border-slate-200 font-bold text-[11px] rounded-md uppercase tracking-wide",
                                ),
                                class_name="flex items-center gap-2 shrink-0",
                            ),
                        ),
                        # Divider
                        rx.cond(
                            AppState.has_date_column,
                            rx.box(class_name="w-px h-5 bg-slate-200 mx-2 shrink-0"),
                        ),
                        # Re-fetch button
                        rx.cond(
                            AppState.has_load_id,
                            rx.button(
                                rx.icon(tag="refresh-cw", size=14),
                                "RE-FETCH",
                                on_click=lambda: AppState.execute_query(force=True),
                                class_name="flex items-center gap-1.5 px-3 py-1.5 text-[11px] font-bold text-slate-500 bg-white border border-slate-200 rounded-md hover:bg-slate-50 transition-colors cursor-pointer",
                            ),
                        ),
                        # Divider before partition toggle
                        rx.cond(
                            AppState.has_load_id,
                            rx.box(class_name="w-px h-5 bg-slate-200 mx-2 shrink-0"),
                        ),
                        # Unrestricted Toggle Button
                        rx.cond(
                            AppState.has_load_id,
                            rx.cond(
                                AppState.partition_unrestricted,
                                rx.button(
                                    rx.icon(tag="lock-open", size=14),
                                    "ALL PARTITIONS",
                                    on_click=AppState.toggle_partition_unrestricted,
                                    class_name="px-3 py-1.5 bg-orange-50 text-orange-600 border border-orange-200 rounded-md text-[11px] font-bold flex items-center gap-1.5 cursor-pointer hover:bg-orange-100 transition-colors",
                                ),
                                rx.button(
                                    rx.icon(tag="lock", size=14),
                                    "ALL PARTITIONS",
                                    on_click=AppState.toggle_partition_unrestricted,
                                    class_name="px-3 py-1.5 bg-emerald-50 text-emerald-600 border border-emerald-200 rounded-md text-[11px] font-bold flex items-center gap-1.5 cursor-pointer hover:bg-emerald-100 transition-colors",
                                ),
                            ),
                        ),
                        class_name="flex items-center gap-2 shrink-0",
                    ),
                    rx.fragment(),
                ),
                class_name="flex items-center gap-2 overflow-x-auto custom-scrollbar flex-1",
            ),
            # RIGHT SIDE: Relocated Action Buttons (Always Visible)
            rx.hstack(
                # In-Memory Toggle
                rx.cond(
                    UI_CONFIG["FEATURES"].get("SHOW_IN_MEMORY_TOGGLE", True),
                    rx.cond(
                        AppState.use_oracle_in_memory,
                        rx.button(
                            rx.icon(tag="zap", size=14),
                            "In-Memory",
                            on_click=AppState.toggle_oracle_in_memory,
                            class_name="flex items-center gap-1.5 px-3 py-1.5 text-[11px] font-bold text-blue-600 bg-blue-50 border border-blue-200 rounded-lg cursor-pointer hover:bg-blue-100 transition-colors",
                        ),
                        rx.button(
                            rx.icon(tag="zap-off", size=14),
                            "In-Memory",
                            on_click=AppState.toggle_oracle_in_memory,
                            class_name="flex items-center gap-1.5 px-3 py-1.5 text-[11px] font-bold text-slate-500 bg-slate-50 border border-slate-200 rounded-lg cursor-pointer hover:bg-slate-100 transition-colors",
                        ),
                    ),
                    rx.fragment(),
                ),
                # Virtual Scroll Toggle
                rx.cond(
                    UI_CONFIG["FEATURES"].get("SHOW_VIRTUAL_SCROLL_TOGGLE", True),
                    rx.cond(
                        AppState.is_virtual_scroll,
                        rx.button(
                            rx.icon(tag="layers", size=14),
                            "Virtual",
                            on_click=AppState.toggle_virtual_scroll,
                            class_name="flex items-center gap-1.5 px-3 py-1.5 text-[11px] font-bold text-emerald-600 bg-emerald-50 border border-emerald-200 rounded-lg cursor-pointer hover:bg-emerald-100 transition-colors",
                        ),
                        rx.button(
                            rx.icon(tag="table-2", size=14),
                            "Paginated",
                            on_click=AppState.toggle_virtual_scroll,
                            class_name="flex items-center gap-1.5 px-3 py-1.5 text-[11px] font-bold text-slate-500 bg-slate-50 border border-slate-200 rounded-lg cursor-pointer hover:bg-slate-100 transition-colors",
                        ),
                    ),
                    rx.fragment(),
                ),
                # Export Menu
                rx.cond(
                    UI_CONFIG["FEATURES"].get("SHOW_EXPORT_MENU", True),
                    rx.menu.root(
                        rx.menu.trigger(
                            rx.button(
                                rx.icon(tag="download", size=14),
                                "Export",
                                class_name="flex items-center gap-1.5 px-3 py-1.5 text-[11px] font-bold text-slate-700 bg-slate-900 text-white dark:bg-slate-100 dark:text-slate-900 rounded-lg cursor-pointer border-none hover:bg-slate-800 transition-colors",
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
                    rx.fragment(),
                ),
                # Export Progress
                rx.cond(
                    AppState.is_exporting,
                    rx.box(
                        rx.icon(
                            tag="loader",
                            class_name="animate-spin text-primary",
                            size=14,
                        ),
                        rx.text(
                            rx.cond(
                                AppState.export_status != "",
                                f"{AppState.export_progress}%",
                                "Preparing...",
                            ),
                            class_name="text-[11px] font-bold text-slate-600",
                        ),
                        class_name="flex items-center gap-2 px-2 py-1.5 bg-blue-50/50 border border-blue-100 rounded-lg",
                    ),
                ),
                class_name="flex items-center gap-2 shrink-0 ml-4",
            ),
            justify="between",
            align="center",
            width="100%",
        ),
        class_name="bg-white border-b border-slate-200 px-4 py-2 flex items-center w-full shrink-0",
    )
