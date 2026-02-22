import reflex as rx
from frontend.state import AppState


def join_modal() -> rx.Component:
    """A sophisticated modal for building multi-table joins."""
    return rx.dialog.root(
        rx.dialog.content(
            # Header
            rx.box(
                rx.hstack(
                    rx.vstack(
                        rx.heading(
                            "Data Join Builder", size="5", class_name="text-white"
                        ),
                        rx.text(
                            "Configure relationships between datasets.",
                            size="2",
                            class_name="text-slate-200 mt-1",
                        ),
                        align_items="start",
                    ),
                    rx.spacer(),
                    rx.dialog.close(
                        rx.box(
                            rx.icon(
                                tag="x",
                                size=20,
                                class_name="text-white/80 hover:text-white",
                            ),
                            class_name="bg-transparent hover:bg-white/10 p-2 flex items-center justify-center rounded-lg transition-colors border-none cursor-pointer",
                        )
                    ),
                    justify="between",
                    align="center",
                ),
                class_name="bg-[#0f172a] px-6 py-5 rounded-t-xl shrink-0",
                width="100%",
            ),
            # Body
            rx.vstack(
                # Table selection section
                rx.box(
                    rx.hstack(
                        # Primary Table
                        rx.vstack(
                            rx.text(
                                "Primary Table (Left)",
                                class_name="text-xs font-bold text-text-muted mb-1 uppercase tracking-tight",
                            ),
                            rx.box(
                                rx.hstack(
                                    rx.box(
                                        rx.icon(
                                            tag="table-2",
                                            size=18,
                                            class_name="text-primary",
                                        ),
                                        class_name="bg-blue-100 p-1.5 rounded flex items-center justify-center",
                                    ),
                                    rx.vstack(
                                        rx.radix.select.root(
                                            rx.radix.select.trigger(
                                                class_name="text-sm font-bold text-slate-900 border-none bg-transparent h-6 p-0 focus:ring-0 cursor-pointer",
                                            ),
                                            rx.radix.select.content(
                                                rx.radix.select.group(
                                                    rx.foreach(
                                                        AppState.join_anchor_datasets,
                                                        lambda name: (
                                                            rx.radix.select.item(
                                                                name, value=name
                                                            )
                                                        ),
                                                    )
                                                ),
                                                position="popper",
                                            ),
                                            value=AppState.new_join_left_dataset,
                                            on_change=AppState.set_new_join_left_dataset,
                                        ),
                                        rx.text(
                                            "Join Anchor",
                                            class_name="text-[10px] text-text-muted",
                                        ),
                                        spacing="0",
                                        align_items="start",
                                    ),
                                    spacing="2",
                                    align="center",
                                ),
                                class_name="p-3 border border-border-light rounded-lg bg-slate-50 w-full h-[60px] flex items-center shadow-sm hover:border-primary/30 transition-colors",
                            ),
                            width="35%",
                            align_items="start",
                        ),
                        # Join Type Icons
                        rx.vstack(
                            rx.text(
                                "Join Type",
                                class_name="text-[9px] font-black text-text-muted mb-1 uppercase tracking-widest",
                            ),
                            rx.hstack(
                                _join_type_item("inner", "Inner", "hash"),
                                _join_type_item("left", "Left", "arrow-left-from-line"),
                                _join_type_item(
                                    "right", "Right", "arrow-right-from-line"
                                ),
                                _join_type_item("outer", "Full", "expand"),
                                spacing="1",
                                class_name="bg-white p-1 rounded-lg border border-border-light shadow-sm",
                            ),
                            align="center",
                            class_name="flex-shrink-0 px-2",
                        ),
                        # Secondary Table
                        rx.vstack(
                            rx.text(
                                "Secondary Table (Right)",
                                class_name="text-xs font-bold text-text-muted mb-1 uppercase tracking-tight shrink-0",
                            ),
                            rx.box(
                                rx.hstack(
                                    rx.box(
                                        rx.icon(
                                            tag="database",
                                            size=18,
                                            class_name="text-emerald-700",
                                        ),
                                        class_name="bg-emerald-100 p-1.5 rounded flex items-center justify-center shrink-0",
                                    ),
                                    rx.box(
                                        rx.radix.select.root(
                                            rx.radix.select.trigger(
                                                placeholder="Select Table",
                                                class_name="w-full box-border",
                                            ),
                                            rx.radix.select.content(
                                                rx.radix.select.group(
                                                    rx.foreach(
                                                        AppState.dataset_names,
                                                        lambda name: (
                                                            rx.radix.select.item(
                                                                name, value=name
                                                            )
                                                        ),
                                                    )
                                                ),
                                                position="popper",
                                            ),
                                            value=AppState.new_join_right_dataset,
                                            on_change=AppState.set_new_join_right_dataset,
                                            size="1",
                                        ),
                                        class_name="flex-1 min-w-0 w-full",
                                    ),
                                    spacing="2",
                                    align="center",
                                    width="100%",
                                    class_name="min-w-0 box-border w-full flex-1 overflow-hidden",
                                ),
                                class_name="p-3 border border-border-light rounded-lg bg-slate-50 w-full h-[60px] flex items-center box-border min-w-0",
                            ),
                            class_name="flex-1 min-w-0 flex flex-col items-start box-border overflow-hidden",
                        ),
                        width="100%",
                        align="center",
                        spacing="2",
                        class_name="flex gap-4",
                    ),
                    class_name="bg-white rounded-xl border border-border-light shadow-sm p-4 w-full shrink-0",
                ),
                # Join Conditions section
                rx.hstack(
                    rx.heading(
                        "Join Conditions",
                        size="3",
                        class_name="text-text-main font-bold",
                    ),
                    rx.spacer(),
                    rx.box(
                        rx.hstack(
                            rx.icon(tag="plus", size=14),
                            rx.text("Add Rule"),
                            align="center",
                            spacing="1",
                        ),
                        on_click=AppState.add_join_condition,
                        class_name="px-2.5 py-1.5 flex items-center justify-center rounded-lg text-primary bg-primary/5 hover:bg-primary/10 border border-transparent hover:border-primary/20 text-[11px] font-bold transition-colors cursor-pointer",
                    ),
                    width="100%",
                    class_name="mb-3 px-1",
                ),
                # Conditions List
                rx.box(
                    rx.vstack(
                        # Header
                        rx.grid(
                            rx.text("Left Table Column", class_name="pl-2"),
                            rx.text("Operator", class_name="text-center"),
                            rx.text("Right Table Column"),
                            rx.fragment(),
                            columns="12",
                            class_name="px-4 py-2 bg-slate-50 text-[9px] font-bold uppercase tracking-widest text-text-muted rounded-t-xl gap-4",
                        ),
                        # rows
                        rx.foreach(
                            AppState.new_join_conditions, _render_join_condition_row
                        ),
                        width="100%",
                        spacing="0",
                        class_name="bg-white rounded-xl border border-border-light shadow-sm divide-y divide-slate-100",
                    ),
                    width="100%",
                ),
                width="100%",
                class_name="flex-1 p-4 overflow-y-auto overflow-x-hidden",
            ),
            # Footer
            rx.vstack(
                # Summary bar
                rx.hstack(
                    rx.hstack(
                        rx.icon(tag="eye", size=14, class_name="text-accent"),
                        rx.text(
                            "Resulting Dataset: ",
                            rx.text(
                                f"{AppState.joins.length() + 1} Tables matched",
                                as_="span",
                                class_name="font-bold",
                            ),
                            class_name="text-xs text-slate-600",
                        ),
                        align="center",
                        spacing="2",
                    ),
                    # Error dynamic error msg
                    rx.cond(
                        AppState.error_message != "",
                        rx.text(
                            AppState.error_message,
                            class_name="text-xs text-red-500 font-medium px-4",
                        ),
                    ),
                    rx.spacer(),
                    rx.box(
                        rx.hstack(
                            rx.text(
                                "View Example Data", class_name="text-xs font-bold"
                            ),
                            rx.icon(tag="arrow-right", size=14),
                            align="center",
                            spacing="1",
                        ),
                        on_click=AppState.fetch_join_preview,
                        class_name="text-primary hover:text-blue-700 flex items-center justify-center bg-transparent border-none cursor-pointer p-0",
                    ),
                    class_name="bg-slate-50 px-6 py-3 border-b border-slate-100",
                    width="100%",
                ),
                # Buttons
                rx.hstack(
                    rx.dialog.close(
                        rx.box(
                            rx.text("Cancel"),
                            class_name="px-4 py-2 flex items-center justify-center rounded-lg text-sm font-medium text-slate-700 hover:bg-slate-50 transition-colors border border-slate-200 bg-white cursor-pointer",
                        )
                    ),
                    rx.spacer(),
                    rx.box(
                        rx.text("Save as Draft"),
                        class_name="px-4 py-2 flex items-center justify-center rounded-lg text-sm font-medium text-primary hover:bg-primary/5 transition-colors border-none bg-transparent cursor-pointer",
                    ),
                    rx.box(
                        rx.hstack(
                            rx.icon(tag="check", size=18),
                            rx.text("Apply Join"),
                            align="center",
                            spacing="2",
                        ),
                        on_click=AppState.apply_join,
                        class_name="px-5 py-2 flex items-center justify-center rounded-lg text-sm font-medium text-white bg-blue-600 hover:bg-blue-700 shadow-lg shadow-blue-600/20 transition-all border-none cursor-pointer",
                    ),
                    padding="1.5rem",
                    width="100%",
                    align="center",
                ),
                width="100%",
                class_name="border-t border-slate-200 bg-white shrink-0",
            ),
            class_name="relative flex flex-col w-[800px] max-w-[90vw] max-h-[90vh] p-0 bg-surface-light rounded-xl shadow-2xl border border-border-light overflow-hidden",
        ),
        open=AppState.is_join_modal_open,
        on_open_change=AppState.set_is_join_modal_open,
    ), join_preview_modal()


def join_preview_modal() -> rx.Component:
    """A floating overlay showing sample data for the current configuration."""
    return rx.dialog.root(
        rx.dialog.content(
            rx.vstack(
                rx.hstack(
                    rx.icon(tag="eye", size=18, class_name="text-primary"),
                    rx.heading("Join Result Preview (Sample)", size="3"),
                    rx.spacer(),
                    rx.dialog.close(
                        rx.box(
                            rx.icon(tag="x", size=18),
                            class_name="bg-transparent flex items-center justify-center border-none cursor-pointer",
                        )
                    ),
                    width="100%",
                    align="center",
                ),
                rx.divider(),
                rx.box(
                    rx.table.root(
                        rx.table.header(
                            rx.table.row(
                                rx.foreach(
                                    AppState.preview_column_names,
                                    lambda col: rx.table.column_header_cell(
                                        col, class_name="text-[10px] font-bold"
                                    ),
                                ),
                            ),
                        ),
                        rx.table.body(
                            rx.foreach(
                                AppState.join_preview_data,
                                lambda row: rx.table.row(
                                    rx.foreach(
                                        AppState.preview_column_names,
                                        lambda col: rx.table.cell(
                                            row[col],
                                            class_name="text-[11px] truncate max-w-[120px]",
                                        ),
                                    ),
                                ),
                            ),
                        ),
                        variant="surface",
                        size="1",
                    ),
                    class_name="max-h-[400px] overflow-auto w-full border border-slate-100 rounded-lg",
                ),
                rx.text(
                    "Note: This shows only the first 10 matching records.",
                    size="1",
                    class_name="text-text-muted italic",
                ),
                spacing="4",
                width="100%",
            ),
            max_width="900px",
            class_name="p-5",
        ),
        open=AppState.is_join_preview_modal_open,
        on_open_change=AppState.toggle_join_preview,
    )


def _join_type_item(type_val: str, label: str, icon_tag: str) -> rx.Component:
    """Renders a selectable join type item."""
    is_active = AppState.new_join_type == type_val
    return rx.box(
        rx.vstack(
            rx.icon(
                tag=icon_tag,
                size=20,
                class_name=rx.cond(is_active, "text-primary", "text-slate-400"),
            ),
            rx.text(
                label,
                class_name=rx.cond(
                    is_active,
                    "text-primary text-[9px] font-bold uppercase",
                    "text-slate-500 text-[9px] font-bold uppercase",
                ),
            ),
            align="center",
            spacing="1",
        ),
        on_click=lambda: AppState.set_new_join_type(type_val),
        class_name=rx.cond(
            is_active,
            "p-1.5 rounded bg-primary/5 border border-primary/20 cursor-pointer transition-all w-[55px]",
            "p-1.5 rounded border border-transparent hover:bg-slate-50 cursor-pointer transition-all w-[55px]",
        ),
    )


def _render_join_condition_row(condition: dict, index: int) -> rx.Component:
    """Renders a single condition row in the list."""
    return rx.grid(
        # Left Column Select
        rx.box(
            rx.radix.select.root(
                rx.radix.select.trigger(
                    placeholder="Left Column",
                    class_name="w-full",
                ),
                rx.radix.select.content(
                    rx.radix.select.group(
                        rx.foreach(
                            AppState.left_side_column_names,
                            lambda name: rx.radix.select.item(name, value=name),
                        )
                    ),
                    position="popper",
                ),
                value=condition["left_column"],
                on_change=lambda val: AppState.update_new_join_condition(
                    index, "left_column", val
                ),
                size="1",
            ),
            class_name="col-span-5",
        ),
        # Operator
        rx.box(
            rx.box(
                "=",
                class_name="h-9 w-9 rounded bg-slate-100 flex items-center justify-center text-slate-500 font-bold text-sm border border-slate-200",
            ),
            class_name="col-span-2 flex justify-center items-center",
        ),
        # Right Column Select
        rx.box(
            rx.radix.select.root(
                rx.radix.select.trigger(
                    placeholder="Right Column",
                    class_name="w-full",
                ),
                rx.radix.select.content(
                    rx.radix.select.group(
                        rx.foreach(
                            AppState.right_side_column_names,
                            lambda name: rx.radix.select.item(name, value=name),
                        )
                    ),
                    position="popper",
                ),
                value=condition["right_column"],
                on_change=lambda val: AppState.update_new_join_condition(
                    index, "right_column", val
                ),
                size="1",
            ),
            class_name="col-span-4",
        ),
        # Delete row button
        rx.box(
            rx.box(
                rx.icon(tag="trash-2", size=18),
                on_click=lambda: AppState.remove_join_condition(index),
                class_name="p-2 flex items-center justify-center text-slate-400 hover:text-red-500 hover:bg-red-50 rounded-lg transition-colors border-none bg-transparent cursor-pointer",
            ),
            class_name="col-span-1 flex justify-end items-center",
        ),
        columns="12",
        spacing="4",
        class_name="px-4 py-3 items-center group hover:bg-slate-50 transition-colors gap-4",
    )
