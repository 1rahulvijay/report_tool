from typing import List, Dict, Any
import reflex as rx
from frontend.state import AppState
from frontend.state_modules.join import JoinState


def _render_logic_toggle(group: Dict[str, Any], path: List[int]) -> rx.Component:
    """Renders the Match ALL/ANY segmented control."""
    return rx.box(
        rx.hstack(
            rx.box(
                rx.hstack(
                    rx.box(
                        rx.text(
                            "Match ALL (AND)",
                            class_name=rx.cond(
                                group["logic"] == "AND",
                                "text-white px-3 py-1.5 rounded-[4px] bg-blue-600 shadow-sm transition-all text-xs font-medium cursor-pointer",
                                "text-slate-600 px-3 py-1.5 rounded-[4px] hover:bg-slate-200 transition-all text-xs font-medium cursor-pointer",
                            ),
                            on_click=lambda: AppState.set_filter_logic(path, "AND"),
                        ),
                        rx.text(
                            "Match ANY (OR)",
                            class_name=rx.cond(
                                group["logic"] == "OR",
                                "text-white px-3 py-1.5 rounded-[4px] bg-blue-600 shadow-sm transition-all text-xs font-medium cursor-pointer",
                                "text-slate-600 px-3 py-1.5 rounded-[4px] hover:bg-slate-200 transition-all text-xs font-medium cursor-pointer",
                            ),
                            on_click=lambda: AppState.set_filter_logic(path, "OR"),
                        ),
                        class_name="bg-slate-100 p-1 rounded-lg flex gap-1 border border-border-light",
                    ),
                    rx.box(class_name="h-px flex-1 bg-slate-200"),
                    align="center",
                    spacing="3",
                    width="100%",
                ),
                class_name="relative z-10 w-full",
            ),
        ),
        width="100%",
    )


def _render_filter_row(rule: Dict[str, Any], path: List[int]) -> rx.Component:
    """Renders an individual condition row."""
    return rx.box(
        rx.hstack(
            # Drag Handle (Visual only for now)
            rx.box(
                rx.icon(
                    tag="grip-vertical",
                    size=18,
                    class_name="text-slate-300 group-hover:text-primary",
                ),
                class_name="cursor-move px-1",
            ),
            # Column Selection
            rx.box(
                rx.radix.select.root(
                    rx.radix.select.trigger(
                        placeholder="Field...",
                        class_name="w-[200px] h-10 text-sm focus:ring-1 focus:ring-primary border-border-light rounded-lg",
                    ),
                    rx.radix.select.content(
                        rx.radix.select.group(
                            rx.foreach(
                                AppState.column_names,
                                lambda name: rx.radix.select.item(name, value=name),
                            )
                        ),
                        position="popper",
                    ),
                    value=rule["column"],
                    on_change=lambda val: AppState.update_filter_item(
                        path, "column", val
                    ),
                ),
                class_name="w-1/3 min-w-[200px]",
            ),
            # Operator Selection
            rx.box(
                rx.match(
                    rule["datatype"],
                    (
                        "number",
                        rx.radix.select.root(
                            rx.radix.select.trigger(
                                placeholder="Operator...",
                                class_name="w-[140px] h-10 text-sm focus:ring-1 focus:ring-primary border-border-light rounded-lg",
                            ),
                            rx.radix.select.content(
                                rx.radix.select.group(
                                    rx.foreach(
                                        JoinState.OPERATOR_MAP["number"],
                                        lambda op: rx.radix.select.item(op, value=op),
                                    )
                                ),
                                position="popper",
                            ),
                            value=rule["operator"],
                            on_change=lambda val: AppState.update_filter_item(
                                path, "operator", val
                            ),
                        ),
                    ),
                    (
                        "date",
                        rx.radix.select.root(
                            rx.radix.select.trigger(
                                placeholder="Operator...",
                                class_name="w-[140px] h-10 text-sm focus:ring-1 focus:ring-primary border-border-light rounded-lg",
                            ),
                            rx.radix.select.content(
                                rx.radix.select.group(
                                    rx.foreach(
                                        JoinState.OPERATOR_MAP["date"],
                                        lambda op: rx.radix.select.item(op, value=op),
                                    )
                                ),
                                position="popper",
                            ),
                            value=rule["operator"],
                            on_change=lambda val: AppState.update_filter_item(
                                path, "operator", val
                            ),
                        ),
                    ),
                    # Default (String)
                    rx.radix.select.root(
                        rx.radix.select.trigger(
                            placeholder="Operator...",
                            class_name="w-[140px] h-10 text-sm focus:ring-1 focus:ring-primary border-border-light rounded-lg",
                        ),
                        rx.radix.select.content(
                            rx.radix.select.group(
                                rx.foreach(
                                    JoinState.OPERATOR_MAP["string"],
                                    lambda op: rx.radix.select.item(op, value=op),
                                )
                            ),
                            position="popper",
                        ),
                        value=rule["operator"],
                        on_change=lambda val: AppState.update_filter_item(
                            path, "operator", val
                        ),
                    ),
                ),
                class_name="w-1/4 min-w-[140px]",
            ),
            # Value Input
            rx.cond(
                ~rx.Var.create(
                    ["is null", "is not null", "is empty", "is not empty"]
                ).contains(rule["operator"]),
                rx.box(
                    rx.match(
                        rule["datatype"],
                        (
                            "date",
                            rx.cond(
                                rule["operator"] == "between",
                                rx.input(
                                    type="text",
                                    value=rule["value"],
                                    placeholder="YYYY-MM-DD to YYYY-MM-DD (comma separated)",
                                    on_change=lambda val: AppState.update_filter_item(
                                        path, "value", val
                                    ),
                                    class_name="w-full h-10 text-sm border-border-light rounded-lg focus:ring-1 focus:ring-primary",
                                ),
                                rx.input(
                                    type="date",
                                    value=rule["value"],
                                    on_change=lambda val: AppState.update_filter_item(
                                        path, "value", val
                                    ),
                                    class_name="w-full h-10 text-sm border-border-light rounded-lg focus:ring-1 focus:ring-primary",
                                ),
                            ),
                        ),
                        (
                            "timestamp",
                            rx.cond(
                                rule["operator"] == "between",
                                rx.input(
                                    type="text",
                                    value=rule["value"],
                                    placeholder="YYYY-MM-DD HH:MM:SS to YYYY-MM-DD HH:MM:SS",
                                    on_change=lambda val: AppState.update_filter_item(
                                        path, "value", val
                                    ),
                                    class_name="w-full h-10 text-sm border-border-light rounded-lg focus:ring-1 focus:ring-primary",
                                ),
                                rx.input(
                                    type="datetime-local",
                                    value=rule["value"],
                                    on_change=lambda val: AppState.update_filter_item(
                                        path, "value", val
                                    ),
                                    class_name="w-full h-10 text-sm border-border-light rounded-lg focus:ring-1 focus:ring-primary",
                                ),
                            ),
                        ),
                        (
                            "number",
                            rx.cond(
                                rule["operator"] == "between",
                                rx.input(
                                    type="text",
                                    value=rule["value"],
                                    placeholder="Min to Max (comma separated)",
                                    on_change=lambda val: AppState.update_filter_item(
                                        path, "value", val
                                    ),
                                    class_name="w-full h-10 text-sm border-border-light rounded-lg focus:ring-1 focus:ring-primary",
                                ),
                                rx.input(
                                    type="number",
                                    value=rule["value"],
                                    placeholder="Value...",
                                    on_change=lambda val: AppState.update_filter_item(
                                        path, "value", val
                                    ),
                                    class_name="w-full h-10 text-sm border-border-light rounded-lg focus:ring-1 focus:ring-primary",
                                ),
                            ),
                        ),
                        # Default (String)
                        rx.input(
                            type="text",
                            value=rule["value"],
                            placeholder="Value...",
                            on_change=lambda val: AppState.update_filter_item(
                                path, "value", val
                            ),
                            class_name="w-full h-10 text-sm border-border-light rounded-lg focus:ring-1 focus:ring-primary",
                        ),
                    ),
                    class_name="flex-1",
                ),
                rx.fragment(),
            ),
            # Delete Button
            rx.box(
                rx.icon(tag="trash-2", size=18),
                on_click=AppState.remove_filter_item(path),
                class_name="p-2 flex items-center justify-center text-slate-400 hover:text-red-600 hover:bg-red-50 rounded-lg transition-colors bg-transparent border-none cursor-pointer",
            ),
            align="center",
            spacing="3",
            width="100%",
            class_name="p-3 pr-4 bg-white rounded-lg border border-border-light shadow-sm relative z-10 hover:border-primary/50 transition-colors group",
        ),
        class_name="relative z-10",
    )


def _render_nested_group(
    group: Dict[str, Any], path: List[Any], depth: int = 0
) -> rx.Component:
    """Recursive component to render nested filter groups with a depth limit to avoid compiler recursion error."""
    # Base case for the compiler to stop recursion during static analysis
    if depth > 5:
        return rx.box(
            rx.text(
                "Recursion Limit Reached",
                class_name="text-xs text-red-500 font-bold p-2",
            ),
            class_name="border border-red-200 bg-red-50 rounded-lg ml-4 mt-2",
        )

    return rx.box(
        # Group Connector Line (Visual)
        rx.box(class_name="absolute left-8 top-16 bottom-8 w-0.5 bg-slate-200 -z-0"),
        # Logic Toggle
        _render_logic_toggle(group, path),
        # Conditions/Groups Container
        rx.vstack(
            rx.foreach(
                group["conditions"].to(List[Dict[str, Any]]),
                lambda item, idx: rx.box(
                    rx.cond(
                        item["type"] == "group",
                        _render_nested_group(item, path + [idx], depth + 1),
                        _render_filter_row(item, path + [idx]),
                    ),
                    width="100%",
                ),
            ),
            spacing="3",
            width="100%",
            class_name="pl-2 mt-4",
        ),
        # Actions at the group level
        rx.hstack(
            rx.box(
                rx.hstack(
                    rx.icon(tag="plus", size=16),
                    rx.text("Add Condition"),
                    align="center",
                    spacing="1",
                ),
                on_click=AppState.add_filter_rule(path),
                class_name="px-3 py-2 flex items-center justify-center rounded-lg text-blue-600 bg-blue-50 hover:bg-blue-100 border border-transparent hover:border-blue-200 text-sm font-medium transition-colors cursor-pointer",
            ),
            rx.box(
                rx.hstack(
                    rx.icon(tag="git-branch", size=16),
                    rx.text("Add Group"),
                    align="center",
                    spacing="1",
                ),
                on_click=AppState.add_filter_group(path),
                class_name="px-3 py-2 flex items-center justify-center rounded-lg text-slate-600 hover:bg-slate-100 text-sm font-medium transition-colors border border-transparent hover:border-slate-200 cursor-pointer",
            ),
            spacing="3",
            class_name="mt-4 pl-2",
        ),
        class_name=rx.cond(
            rx.Var.create(path).length() > 0,
            "relative flex flex-col border border-border-light rounded-xl bg-slate-50 p-3 gap-3 mt-2 ml-4 shadow-sm",
            "relative flex flex-col border border-border-light rounded-xl bg-white p-4 gap-3 shadow-sm",
        ),
    )


def filter_modal() -> rx.Component:
    """The main Advanced Filters dialog."""
    return rx.dialog.root(
        rx.dialog.content(
            # Header
            rx.box(
                rx.hstack(
                    rx.box(
                        rx.heading(
                            "Advanced Filters",
                            class_name="text-white text-xl font-bold leading-tight",
                        ),
                        rx.text(
                            "Build complex queries to segment your dataset.",
                            class_name="text-slate-200 text-sm mt-1",
                        ),
                    ),
                    rx.spacer(),
                    rx.box(
                        rx.icon(tag="x", size=20),
                        on_click=AppState.toggle_filter_modal,
                        class_name="p-2 flex items-center justify-center rounded-lg hover:bg-white/10 transition-colors text-white bg-transparent border-none cursor-pointer focus:outline-none",
                    ),
                    width="100%",
                    align="center",
                ),
                class_name="px-6 py-5 border-b border-[#0f172a] bg-[#0f172a] rounded-t-xl shrink-0",
            ),
            # Body (Scrollable)
            rx.box(
                _render_nested_group(AppState.active_filters, [], 0),
                class_name="flex-1 overflow-y-auto overflow-x-hidden p-4 bg-slate-50 custom-scrollbar",
            ),
            # Footer
            rx.hstack(
                rx.hstack(
                    rx.icon(tag="filter", size=18, class_name="text-primary"),
                    rx.text(
                        "Filtering results down to ",
                        rx.text(
                            AppState.total_row_count.to(str),
                            as_="span",
                            class_name="text-slate-900 font-bold tabular-nums",
                        ),
                        " rows",
                        class_name="text-sm text-slate-600",
                    ),
                    align="center",
                    spacing="2",
                ),
                rx.spacer(),
                rx.hstack(
                    rx.box(
                        rx.text("Clear All"),
                        on_click=AppState.clear_filters,
                        class_name="px-4 py-2 flex items-center justify-center rounded-lg text-sm font-medium text-slate-700 hover:bg-slate-50 border border-slate-200 bg-white shadow-sm cursor-pointer",
                    ),
                    rx.box(
                        rx.text("Apply Filters"),
                        on_click=AppState.apply_filters,
                        class_name="px-5 py-2 flex items-center justify-center rounded-lg text-sm font-medium text-white bg-blue-600 hover:bg-blue-700 shadow-lg shadow-blue-500/20 transition-all border border-transparent cursor-pointer",
                    ),
                    spacing="3",
                ),
                class_name="px-6 py-4 border-t border-slate-200 bg-white rounded-b-xl shrink-0",
            ),
            class_name="relative z-50 w-[800px] max-w-[90vw] max-h-[90vh] p-0 flex flex-col bg-surface-light rounded-xl shadow-2xl border border-border-light overflow-hidden",
        ),
        open=AppState.is_filter_modal_open,
        on_open_change=AppState.set_is_filter_modal_open,
    )
