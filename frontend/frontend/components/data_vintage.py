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
                    tag="calendar-clock",
                    size=16,
                    class_name="text-indigo-500",
                ),
                rx.text(
                    "Data Vintage",
                    class_name="text-xs font-bold text-slate-500 uppercase tracking-wider",
                ),
                class_name="flex items-center gap-2",
            ),
            # Center: Load Type Badge + Load ID Dropdown
            rx.hstack(
                # Load Type badge (read-only)
                rx.box(
                    rx.hstack(
                        rx.text(
                            "Load Type",
                            class_name="text-[10px] font-medium text-slate-400 uppercase",
                        ),
                        rx.box(
                            AppState.partition_load_type,
                            class_name="px-2 py-0.5 bg-indigo-50 text-indigo-600 text-xs font-semibold rounded-full border border-indigo-100",
                        ),
                        class_name="flex items-center gap-2",
                    ),
                ),
                # Divider
                rx.box(class_name="w-px h-6 bg-slate-200 mx-2"),
                # Load ID selector
                rx.box(
                    rx.hstack(
                        rx.text(
                            "Load ID",
                            class_name="text-[10px] font-medium text-slate-400 uppercase",
                        ),
                        rx.select(
                            AppState.partition_available_values,
                            value=AppState.current_load_id_display,
                            on_change=AppState.set_current_load_id,
                            class_name="text-xs font-semibold min-w-[120px]",
                            size="1",
                        ),
                        class_name="flex items-center gap-2",
                    ),
                ),
                # Divider
                rx.box(class_name="w-px h-6 bg-slate-200 mx-2"),
                # Partition Column info
                rx.hstack(
                    rx.text(
                        "Column",
                        class_name="text-[10px] font-medium text-slate-400 uppercase",
                    ),
                    rx.code(
                        AppState.partition_column_name,
                        class_name="text-[11px] text-slate-500",
                    ),
                    class_name="flex items-center gap-2",
                ),
                # Divider
                rx.box(class_name="w-px h-6 bg-slate-200 mx-2"),
                # Unrestricted toggle
                rx.box(
                    rx.cond(
                        AppState.partition_unrestricted,
                        rx.button(
                            rx.icon(tag="lock-open", size=14),
                            "All Partitions",
                            on_click=AppState.toggle_partition_unrestricted,
                            class_name="px-2.5 py-1 bg-amber-50 text-amber-600 border border-amber-200 rounded-md text-[11px] font-semibold flex items-center gap-1.5 cursor-pointer hover:bg-amber-100 transition-colors",
                        ),
                        rx.button(
                            rx.icon(tag="lock", size=14),
                            "Filtered",
                            on_click=AppState.toggle_partition_unrestricted,
                            class_name="px-2.5 py-1 bg-emerald-50 text-emerald-600 border border-emerald-200 rounded-md text-[11px] font-semibold flex items-center gap-1.5 cursor-pointer hover:bg-emerald-100 transition-colors",
                        ),
                    ),
                ),
                class_name="flex items-center gap-1",
            ),
            class_name="bg-gradient-to-r from-slate-50 to-indigo-50/30 border-b border-indigo-100 px-6 py-2.5 flex items-center justify-between gap-6 w-full",
        ),
    )
