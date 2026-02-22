"""Welcome to Reflex! This file outlines the steps to create a basic app."""

import reflex as rx
from frontend.state import AppState
from frontend.components.sidebar import sidebar
from frontend.components.header import topnav
from frontend.components.datagrid import datagrid


class Style:
    """Aurora base styles"""

    bg_color = "#f8fafc"
    text_primary = "#0f172a"
    accent = "#3b82f6"


def index() -> rx.Component:
    """The main entry point for the application matching the Ultra-Compact HTML layout."""
    return rx.box(
        # Top Navigation stays fixed at the top
        topnav(),
        # Main Layout horizontally split below header
        rx.hstack(
            sidebar(),
            rx.cond(
                AppState.is_loading & (AppState.selected_dataset == ""),
                rx.center(
                    rx.spinner(size="3"),
                    class_name="flex-1 flex flex-col min-w-0 bg-background-light dark:bg-background-dark relative items-center justify-center",
                ),
                # Otherwise, render the main datagrid workspace
                datagrid(),
            ),
            class_name="flex flex-1 overflow-hidden h-full",
            width="100%",
        ),
        # On load we trigger the API metadata fetch
        class_name="bg-background-light dark:bg-background-dark text-slate-900 dark:text-slate-100 h-screen max-h-screen w-screen flex flex-col overflow-hidden",
    )


app = rx.App(
    stylesheets=[
        "/custom.css",
    ],
)
app.add_page(
    index, title="Aurora | Enterprise Data Explorer", on_load=AppState.fetch_datasets
)
