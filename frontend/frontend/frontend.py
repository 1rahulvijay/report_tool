"""Welcome to Reflex! This file outlines the steps to create a basic app."""

import reflex as rx
from frontend.state import AppState
from frontend.components.sidebar import sidebar
from frontend.components.header import topnav
from frontend.components.datagrid import datagrid
from frontend.pages.presets import presets_page
from frontend.config import COLORS, UI_CONFIG


class Style:
    """Aurora base styles"""

    bg_color = COLORS["bg_light"]
    text_primary = COLORS["text_dark"]
    accent = COLORS["primary"]


def index() -> rx.Component:
    """The main entry point for the application matching the Ultra-Compact HTML layout."""
    return rx.box(
        # Top Navigation stays fixed at the top
        topnav(),
        # Main Layout horizontally split below header
        rx.hstack(
            sidebar(show_columns=True),
            rx.cond(
                AppState.is_loading & (AppState.selected_dataset == ""),
                rx.center(
                    rx.spinner(size="3"),
                    class_name="flex-1 flex flex-col min-w-0 bg-background-light dark:bg-background-dark relative items-center justify-center",
                ),
                # Otherwise, render the main datagrid workspace
                datagrid(),
            ),
            class_name="flex flex-1 overflow-hidden h-full min-h-0",
            width="100%",
        ),
        # On load we trigger the API metadata fetch
        class_name=f"bg-[{COLORS['bg_light']}] dark:bg-[{COLORS['bg_dark']}] text-slate-900 dark:text-slate-100 h-screen max-h-screen w-screen flex flex-col overflow-hidden",
    )


app = rx.App(
    stylesheets=[
        "/custom.css",
    ],
)
app.add_page(
    index,
    title=f"{UI_CONFIG['APP_NAME']} | Enterprise Data Explorer",
    on_load=AppState.fetch_datasets,
)
app.add_page(
    presets_page,
    route="/presets",
    title=f"{UI_CONFIG['APP_NAME']} | Dashboard Presets",
    on_load=AppState.fetch_datasets,
)
