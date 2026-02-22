import reflex as rx
from frontend.state import AppState


def _render_group_by_pill(col_name: str) -> rx.Component:
    """Renders a pill for a selected Group By column."""
    return rx.box(
        rx.icon(
            tag=rx.cond(
                col_name.lower().contains("date"),
                "calendar",
                rx.cond(col_name.lower().contains("region"), "globe", "tag"),
            ),
            class_name="text-slate-400 text-base",
            size=16,
        ),
        rx.text(col_name),
        rx.box(
            rx.icon(tag="x", size=14),
            on_click=lambda: AppState.remove_group_by_column(col_name),
            class_name="ml-1 p-0.5 hover:bg-red-50 hover:text-red-500 rounded-full flex items-center justify-center transition-colors bg-transparent border-none cursor-pointer",
        ),
        class_name="inline-flex items-center gap-1.5 px-3 py-1.5 bg-white border border-border-light rounded-full shadow-sm text-sm font-medium text-slate-700 hover:border-primary/30 transition-colors group",
    )


def _render_aggregation_row(agg: rx.Var, index: int) -> rx.Component:
    """Renders a single aggregation metric row."""
    return rx.box(
        # Source Column
        rx.box(
            rx.box(
                rx.box(
                    rx.icon(
                        tag=rx.cond(
                            agg["column"].lower().contains("sales")
                            | agg["column"].lower().contains("price")
                            | agg["column"].lower().contains("profit"),
                            "dollar-sign",
                            "hash",
                        ),
                        class_name="text-slate-400",
                        size=18,
                    ),
                    class_name="absolute inset-y-0 left-0 pl-3 flex items-center pointer-events-none",
                ),
                rx.radix.select.root(
                    rx.radix.select.trigger(
                        placeholder="Select Column",
                        class_name="w-full h-10 pl-10 pr-10 text-sm bg-white border border-border-light rounded-lg text-slate-700 focus:ring-1 focus:ring-primary focus:border-primary cursor-pointer hover:border-slate-300 transition-colors truncate text-left",
                    ),
                    rx.radix.select.content(
                        rx.radix.select.group(
                            rx.foreach(
                                AppState.numeric_column_names,
                                lambda name: rx.radix.select.item(name, value=name),
                            )
                        ),
                        position="popper",
                    ),
                    value=agg["column"],
                    on_change=lambda val: AppState.update_aggregation_row(
                        index, "column", val
                    ),
                ),
                rx.icon(
                    tag="chevron-down",
                    size=18,
                    class_name="absolute right-2 top-2.5 text-slate-400 pointer-events-none",
                ),
                class_name="relative",
            ),
            class_name="col-span-4",
        ),
        # Function
        rx.box(
            rx.box(
                rx.radix.select.root(
                    rx.radix.select.trigger(
                        class_name="w-full h-10 pl-3 pr-10 text-sm bg-white border border-border-light rounded-lg text-slate-700 focus:ring-1 focus:ring-primary focus:border-primary cursor-pointer hover:border-slate-300 transition-colors font-mono uppercase truncate text-left",
                    ),
                    rx.radix.select.content(
                        rx.radix.select.group(
                            rx.foreach(
                                ["sum", "avg", "count", "max", "min", "distinct_count"],
                                lambda name: rx.radix.select.item(name, value=name),
                            )
                        ),
                        position="popper",
                    ),
                    value=agg["function"],
                    on_change=lambda val: AppState.update_aggregation_row(
                        index, "function", val
                    ),
                ),
                rx.icon(
                    tag="chevron-down",
                    size=18,
                    class_name="absolute right-2 top-2.5 text-slate-400 pointer-events-none",
                ),
                class_name="relative",
            ),
            class_name="col-span-3",
        ),
        # Output Name
        rx.box(
            rx.input(
                placeholder="Enter output name",
                value=agg["output_name"],
                on_change=lambda val: AppState.update_aggregation_row(
                    index, "output_name", val
                ),
                class_name="w-full h-10 px-3 text-sm bg-white border border-border-light rounded-lg text-slate-700 focus:ring-1 focus:ring-primary focus:border-primary placeholder-slate-400 hover:border-slate-300 transition-colors",
            ),
            class_name="col-span-4",
        ),
        # Actions
        rx.box(
            rx.box(
                rx.icon(tag="trash-2", size=18),
                on_click=lambda: AppState.remove_aggregation_row(index),
                class_name="p-2 flex items-center justify-center text-slate-300 hover:text-red-500 hover:bg-red-50 rounded-lg transition-colors bg-transparent border-none cursor-pointer",
            ),
            class_name="col-span-1 flex justify-end",
        ),
        class_name="grid grid-cols-12 gap-4 px-4 py-3 items-center group hover:bg-slate-50 transition-colors",
    )


def aggregation_modal() -> rx.Component:
    """The Aggregation Builder modal matching the HTML design."""
    return rx.cond(
        AppState.is_aggregation_modal_open,
        rx.box(
            # Backdrop
            rx.box(
                class_name="fixed inset-0 bg-primary/20 backdrop-blur-sm z-40 transition-opacity",
                on_click=AppState.toggle_aggregation_modal,
            ),
            # Modal Content
            rx.box(
                # Header
                rx.box(
                    rx.box(
                        rx.heading(
                            "Aggregation Builder",
                            class_name="text-xl font-bold leading-tight text-white",
                        ),
                        rx.text(
                            "Define granularity and summary metrics for your dataset.",
                            class_name="text-slate-200 text-sm mt-1",
                        ),
                    ),
                    rx.box(
                        rx.icon(
                            tag="x",
                            size=20,
                            class_name="text-white/80 group-hover:text-white",
                        ),
                        on_click=AppState.toggle_aggregation_modal,
                        class_name="group p-2 flex items-center justify-center rounded-lg hover:bg-white/10 transition-colors focus:outline-none focus:ring-2 focus:ring-white/50 bg-transparent border-none cursor-pointer",
                    ),
                    class_name="flex items-center justify-between px-6 py-5 border-b border-[#0f172a] bg-[#0f172a] text-white shrink-0",
                ),
                # Body
                rx.box(
                    # Group By Section
                    rx.box(
                        rx.box(
                            rx.box(
                                rx.box(
                                    rx.icon(
                                        tag="layers",
                                        size=20,
                                    ),
                                    class_name="p-1.5 bg-blue-100 rounded text-primary flex items-center justify-center shadow-sm",
                                ),
                                rx.box(
                                    rx.heading(
                                        "Group By",
                                        class_name="text-base font-semibold text-text-main",
                                    ),
                                    rx.text(
                                        "Select columns to define the granularity of the report.",
                                        class_name="text-xs text-text-muted",
                                    ),
                                    class_name="flex flex-col",
                                ),
                                class_name="flex items-center gap-3",
                            ),
                            class_name="flex items-center justify-between",
                        ),
                        rx.box(
                            # Selected Pills
                            rx.box(
                                rx.foreach(
                                    AppState.aggregation_group_by, _render_group_by_pill
                                ),
                                class_name="flex flex-wrap gap-2",
                            ),
                            # Column Selection
                            rx.box(
                                rx.radix.select.root(
                                    rx.radix.select.trigger(
                                        placeholder="+ Add a column to group by...",
                                        class_name="w-full max-w-md h-10 pl-3 pr-8 text-sm bg-white border border-border-light rounded-lg text-slate-500 focus:ring-1 focus:ring-primary focus:border-primary cursor-pointer hover:border-slate-300 transition-colors shadow-sm text-left",
                                    ),
                                    rx.radix.select.content(
                                        rx.radix.select.group(
                                            rx.foreach(
                                                AppState.raw_column_names,
                                                lambda name: rx.radix.select.item(
                                                    name, value=name
                                                ),
                                            )
                                        ),
                                        position="popper",
                                    ),
                                    on_change=AppState.add_group_by_column,
                                ),
                                rx.icon(
                                    tag="chevron-down",
                                    size=18,
                                    class_name="absolute left-[26rem] top-2.5 text-slate-400 pointer-events-none",
                                ),
                                class_name="relative mt-2",
                            ),
                            class_name="p-4 border border-border-light rounded-lg bg-slate-50 min-h-[100px] flex flex-col gap-3",
                        ),
                        class_name="bg-white rounded-xl border border-border-light shadow-sm p-4 space-y-3",
                    ),
                    # Aggregations Section
                    rx.box(
                        rx.box(
                            rx.box(
                                rx.box(
                                    rx.icon(
                                        tag="function-square",
                                        size=20,
                                    ),
                                    class_name="p-1.5 bg-emerald-100 rounded text-emerald-700 flex items-center justify-center shadow-sm",
                                ),
                                rx.heading(
                                    "Aggregations",
                                    class_name="text-lg font-semibold text-text-main",
                                ),
                                class_name="flex items-center gap-3",
                            ),
                            rx.box(
                                rx.hstack(
                                    rx.icon(
                                        tag="plus-circle",
                                        size=16,
                                    ),
                                    rx.text("Add Aggregation"),
                                    align="center",
                                    spacing="1",
                                ),
                                on_click=AppState.add_aggregation_row,
                                class_name="flex items-center gap-2 flex items-center justify-center px-3 py-1.5 rounded-lg text-primary bg-primary/5 hover:bg-primary/10 border border-transparent hover:border-primary/20 text-xs font-medium transition-colors bg-transparent border-none cursor-pointer",
                            ),
                            class_name="flex items-center justify-between",
                        ),
                        rx.box(
                            # Header Row
                            rx.box(
                                rx.box("Source Column", class_name="col-span-4 pl-2"),
                                rx.box("Function", class_name="col-span-3"),
                                rx.box("Output Name", class_name="col-span-4"),
                                rx.box("", class_name="col-span-1"),
                                class_name="grid grid-cols-12 gap-4 px-4 py-3 bg-slate-50 text-xs font-semibold text-text-muted rounded-t-xl uppercase tracking-wider",
                            ),
                            # Dynamic Rows
                            rx.foreach(AppState.aggregations, _render_aggregation_row),
                            class_name="bg-white rounded-xl border border-border-light shadow-sm divide-y divide-slate-100",
                        ),
                        class_name="space-y-3",
                    ),
                    class_name="flex-1 overflow-y-auto overflow-x-hidden bg-slate-50 p-4 space-y-4 custom-scrollbar",
                ),
                # Footer
                rx.box(
                    # Stats Bar
                    rx.box(
                        rx.box(
                            rx.icon(
                                tag="network",
                                size=14,
                                class_name="text-accent",
                            ),
                            rx.box(
                                rx.text(
                                    "Resulting Schema: ",
                                    rx.text(
                                        AppState.aggregation_group_by.length(),
                                        as_="b",
                                    ),
                                    " Dimensions, ",
                                    rx.text(
                                        AppState.aggregations.length(),
                                        as_="b",
                                    ),
                                    " Measures defined.",
                                    as_="span",
                                ),
                                class_name="text-xs text-slate-600",
                            ),
                            class_name="flex items-center gap-2",
                        ),
                        rx.box(
                            rx.box(
                                rx.text("Preview Data "),
                                rx.icon(
                                    tag="eye",
                                    size=14,
                                ),
                                class_name="flex items-center gap-1",
                            ),
                            on_click=AppState.apply_aggregations,
                            class_name="text-primary hover:text-primary-hover font-medium flex items-center justify-center bg-transparent border-none cursor-pointer",
                        ),
                        class_name="bg-slate-50 px-6 py-3 border-b border-slate-100 flex items-center justify-between text-xs",
                    ),
                    # Action Bar
                    rx.box(
                        rx.box(
                            rx.text("Cancel"),
                            on_click=AppState.toggle_aggregation_modal,
                            class_name="px-4 py-2 flex items-center justify-center rounded-lg text-sm font-medium text-slate-700 hover:bg-slate-50 hover:text-slate-900 transition-colors border border-slate-200 hover:border-slate-300 bg-white shadow-sm cursor-pointer",
                        ),
                        rx.box(
                            rx.hstack(
                                rx.icon(
                                    tag="check-check",
                                    size=18,
                                ),
                                rx.text("Apply Aggregation"),
                                align="center",
                                spacing="2",
                            ),
                            on_click=AppState.apply_aggregations,
                            class_name="px-5 py-2 flex items-center justify-center rounded-lg text-sm font-medium text-white bg-primary hover:bg-primary-hover shadow-lg shadow-primary/20 transition-all border border-transparent flex items-center gap-2 cursor-pointer shadow-primary/20",
                        ),
                        class_name="flex items-center justify-between px-6 py-4",
                    ),
                    class_name="flex flex-col border-t border-slate-200 bg-white shrink-0",
                ),
                class_name="relative z-50 w-[800px] max-w-[90vw] max-h-[90vh] flex flex-col bg-surface-light rounded-xl shadow-2xl border border-border-light overflow-hidden",
            ),
            class_name="fixed inset-0 flex items-center justify-center p-4 z-50",
        ),
        rx.fragment(),
    )
