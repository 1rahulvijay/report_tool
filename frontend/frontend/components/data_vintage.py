"""
Data Vintage control bar — shows Load Type badge and Load ID dropdown
for partitioned datasets. Only renders when partition_info is available.
"""

import reflex as rx
from frontend.state import AppState


def data_vintage_bar() -> rx.Component:
    """Compact control bar for partition selection, rendered above the datagrid."""
    return rx.cond(
        AppState.has_partition_info,
        rx.box(
            # Left: Icon + Label
            rx.hstack(
                rx.icon(
                    tag="calendar",
                    size=16,
                    class_name="text-slate-500",
                ),
                rx.text(
                    "DATA VINTAGE",
                    class_name="text-xs font-bold text-slate-500 uppercase tracking-wider",
                ),
                class_name="flex items-center gap-2",
            ),
            # Center Control group
            rx.hstack(
                # Load Type (Blue Pill Dropdown) - conditionally render only if it exists
                rx.cond(
                    AppState.has_load_type & (AppState.partition_load_type != ""),
                    rx.box(
                        rx.hstack(
                            rx.text(
                                "LOAD TYPE",
                                class_name="text-[10px] font-bold text-slate-400 uppercase",
                            ),
                            rx.select(
                                AppState.partition_supported_types,
                                value=AppState.partition_load_type,
                                on_change=AppState.set_partition_load_type,
                                class_name="px-3 py-1 bg-blue-50 text-blue-600 border border-blue-200 font-semibold text-xs rounded-full uppercase tracking-wide cursor-pointer focus:ring-0",
                            ),
                            class_name="flex items-center gap-2",
                        )
                    ),
                ),
                rx.cond(
                    AppState.has_load_type & (AppState.partition_load_type != ""),
                    rx.box(class_name="w-px h-6 bg-slate-200 mx-1"),
                ),
                # Load ID Dropdown
                rx.cond(
                    AppState.has_load_id,
                    rx.hstack(
                        rx.text(
                            "LOAD ID",
                            class_name="text-[10px] font-bold text-slate-400 uppercase",
                        ),
                        rx.select(
                            AppState.partition_available_values,
                            value=AppState.current_load_id_display,
                            on_change=AppState.set_current_load_id,
                            class_name="text-xs font-semibold min-w-[140px] bg-white border border-slate-200 rounded-md py-1",
                            size="1",
                        ),
                        class_name="flex items-center gap-2",
                    ),
                ),
                rx.cond(
                    AppState.has_load_id,
                    rx.box(class_name="w-px h-6 bg-slate-200 mx-1"),
                ),
                # Column Name (Gray text + Pill)
                rx.cond(
                    AppState.has_date_column,
                    rx.hstack(
                        rx.text(
                            "COLUMN",
                            class_name="text-[10px] font-bold text-slate-400 uppercase",
                        ),
                        rx.box(
                            AppState.partition_column_name,
                            class_name="px-3 py-1 bg-slate-100 text-slate-600 border border-slate-200 font-semibold text-[11px] rounded-md uppercase tracking-wide",
                        ),
                        class_name="flex items-center gap-2",
                    ),
                ),
                rx.cond(
                    AppState.has_date_column,
                    rx.box(class_name="w-px h-6 bg-slate-200 mx-1"),
                ),
                # Unrestricted Toggle Button
                rx.cond(
                    AppState.has_load_id,
                    rx.cond(
                        AppState.partition_unrestricted,
                        rx.button(
                            rx.icon(tag="unlock", size=14),
                            "ALL PARTITIONS",
                            on_click=AppState.toggle_partition_unrestricted,
                            class_name="px-3 py-1.5 bg-orange-50 text-orange-600 border border-orange-200 rounded-md text-xs font-bold flex items-center gap-2 cursor-pointer hover:bg-orange-100 transition-colors shadow-sm",
                        ),
                        rx.button(
                            rx.icon(tag="lock", size=14),
                            "FILTERED",
                            on_click=AppState.toggle_partition_unrestricted,
                            class_name="px-3 py-1.5 bg-emerald-50 text-emerald-600 border border-emerald-200 rounded-md text-xs font-bold flex items-center gap-2 cursor-pointer hover:bg-emerald-100 transition-colors shadow-sm",
                        ),
                    ),
                ),
                class_name="flex items-center gap-4",
            ),
            class_name="bg-white border-b border-slate-200 px-6 py-3 flex items-center justify-between w-full shadow-sm",
        ),
    )
