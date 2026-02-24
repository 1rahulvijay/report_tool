from typing import List, Dict, Any
import reflex as rx
from frontend.state import AppState
from frontend.state_modules.join import JoinState


def _inline_search(placeholder: str, value, on_change) -> rx.Component:
    """Compact search input to embed inside dropdown content panes."""
    return rx.box(
        rx.icon(
            tag="search",
            size=14,
            class_name="absolute left-2.5 top-1/2 -translate-y-1/2 text-slate-400",
        ),
        rx.el.input(
            placeholder=placeholder,
            value=value,
            on_change=on_change,
            class_name="w-full bg-slate-50 border border-slate-200 rounded pl-8 pr-3 py-1.5 text-xs focus:ring-primary focus:border-primary outline-none",
        ),
        class_name="relative px-2 pt-2 pb-1",
    )


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
            # Drag Handle
            rx.box(
                rx.icon(
                    tag="grip-vertical",
                    size=18,
                    class_name="text-slate-300 group-hover:text-primary",
                ),
                class_name="cursor-move px-1",
            ),
            # Column Selection — uses display-pair var
            rx.box(
                rx.radix.select.root(
                    rx.radix.select.trigger(
                        placeholder="Field...",
                        class_name="w-[200px] h-10 text-sm focus:ring-1 focus:ring-primary border-border-light rounded-lg",
                    ),
                    rx.radix.select.content(
                        _inline_search(
                            "Search columns...",
                            AppState.filter_col_search,
                            AppState.set_filter_col_search,
                        ),
                        rx.radix.select.group(
                            rx.foreach(
                                AppState.filtered_filter_col_display,
                                lambda pair: rx.radix.select.item(
                                    pair[1], value=pair[0]
                                ),
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
                    ["is null", "is not null", "is empty", "is not empty", "max", "min"]
                ).contains(rule["operator"]),
                rx.box(
                    rx.match(
                        rule["datatype"],
                        (
                            "date",
                            rx.cond(
                                rule["operator"] == "between",
                                # Dual date pickers for date range
                                rx.hstack(
                                    rx.input(
                                        type="date",
                                        on_change=lambda val: (
                                            AppState.update_filter_between_date(
                                                path, "start", val
                                            )
                                        ),
                                        class_name="flex-1 h-10 text-sm border-border-light rounded-lg focus:ring-1 focus:ring-primary",
                                    ),
                                    rx.text(
                                        "to",
                                        class_name="text-xs text-slate-400 font-medium px-1 self-center",
                                    ),
                                    rx.input(
                                        type="date",
                                        on_change=lambda val: (
                                            AppState.update_filter_between_date(
                                                path, "end", val
                                            )
                                        ),
                                        class_name="flex-1 h-10 text-sm border-border-light rounded-lg focus:ring-1 focus:ring-primary",
                                    ),
                                    spacing="2",
                                    width="100%",
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
                                # Dual datetime pickers for timestamp range
                                rx.hstack(
                                    rx.input(
                                        type="datetime-local",
                                        on_change=lambda val: (
                                            AppState.update_filter_between_date(
                                                path, "start", val
                                            )
                                        ),
                                        class_name="flex-1 h-10 text-sm border-border-light rounded-lg focus:ring-1 focus:ring-primary",
                                    ),
                                    rx.text(
                                        "to",
                                        class_name="text-xs text-slate-400 font-medium px-1 self-center",
                                    ),
                                    rx.input(
                                        type="datetime-local",
                                        on_change=lambda val: (
                                            AppState.update_filter_between_date(
                                                path, "end", val
                                            )
                                        ),
                                        class_name="flex-1 h-10 text-sm border-border-light rounded-lg focus:ring-1 focus:ring-primary",
                                    ),
                                    spacing="2",
                                    width="100%",
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
    """Recursive component to render nested filter groups."""
    if depth > 5:
        return rx.box(
            rx.text(
                "Recursion Limit Reached",
                class_name="text-xs text-red-500 font-bold p-2",
            ),
            class_name="border border-red-200 bg-red-50 rounded-lg ml-4 mt-2",
        )

    return rx.box(
        rx.box(class_name="absolute left-8 top-16 bottom-8 w-0.5 bg-slate-200 -z-0"),
        _render_logic_toggle(group, path),
        rx.vstack(
            rx.foreach(
                group["conditions"].to(List[Dict[str, Any]]),
                lambda item, idx: rx.box(
                    rx.cond(
                        item["type"] == "group",
                        _render_nested_group(item, [path, idx], depth + 1),
                        _render_filter_row(item, [path, idx]),
                    ),
                    width="100%",
                ),
            ),
            spacing="3",
            width="100%",
            class_name="pl-2 mt-4",
        ),
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
            spacing="3",
            class_name="mt-4 pl-2",
        ),
        class_name=rx.cond(
            rx.Var.create(path).length() > 0,
            "relative flex flex-col border border-slate-200 rounded-xl bg-slate-50 p-3 gap-3 mt-2 ml-4 shadow-sm",
            "relative flex flex-col border border-slate-200 rounded-xl bg-white p-4 gap-3 shadow-sm",
        ),
    )


def filter_modal() -> rx.Component:
    """The main Advanced Filters dialog."""
    return rx.cond(
        AppState.is_filter_modal_open,
        rx.box(
            # Backdrop
            rx.box(
                class_name="fixed inset-0 bg-black/50 backdrop-blur-sm z-40 transition-opacity",
                on_click=AppState.toggle_filter_modal,
            ),
            # Modal Content
            rx.box(
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
                class_name="relative z-50 w-[800px] max-w-[90vw] max-h-[90vh] p-0 flex flex-col bg-white rounded-xl shadow-2xl border border-slate-200 overflow-hidden",
            ),
            class_name="fixed inset-0 flex items-center justify-center p-4 z-50",
        ),
        rx.fragment(),
    )
