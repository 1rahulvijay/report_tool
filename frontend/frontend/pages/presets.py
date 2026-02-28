import reflex as rx
from frontend.state import AppState
import plotly.graph_objects as go
from frontend.state_modules.preset_state import PresetState
from frontend.components.sidebar import sidebar
from frontend.components.header import topnav
from frontend.components.data_vintage import data_vintage_bar
from frontend.components.charts import custom_chart
from frontend.config import COLORS, UI_CONFIG, VIBRANT_PALETTE


def _render_chart_tile(preset: rx.Var) -> rx.Component:
    """Renders an individual chart tile with compact layout."""
    preset_dict_val = preset.to(dict)
    title = preset_dict_val.get("title", "Chart")
    description = preset_dict_val.get("description", "")
    chart_type = preset_dict_val.get("type", "area")

    # Extract explicit charting parameters
    x_axis_col = preset_dict_val.get("x_axis_col", "x_axis")
    y_axis_cols = preset_dict_val.get("y_axis_cols", ["y_axis"])
    show_legend = preset_dict_val.get("show_legend", False)
    primary_color = preset_dict_val.get("primary_color", COLORS["primary"])
    color_palette = preset_dict_val.get("color_palette", VIBRANT_PALETTE)

    engine = preset_dict_val.get("engine", "recharts")
    results = preset_dict_val.get("results", []).to(list)
    plotly_scatter_fig = preset_dict_val.get("plotly_scatter_fig", go.Figure()).to(
        go.Figure
    )
    plotly_bar_fig = preset_dict_val.get("plotly_bar_fig", go.Figure()).to(go.Figure)
    plotly_area_fig = preset_dict_val.get("plotly_area_fig", go.Figure()).to(go.Figure)
    plotly_horizontal_bar_fig = preset_dict_val.get(
        "plotly_horizontal_bar_fig", go.Figure()
    ).to(go.Figure)
    plotly_pie_fig = preset_dict_val.get("plotly_pie_fig", go.Figure()).to(go.Figure)
    plotly_stacked_bar_fig = preset_dict_val.get(
        "plotly_stacked_bar_fig", go.Figure()
    ).to(go.Figure)

    return rx.box(
        rx.box(
            rx.heading(
                title,
                size="3",  # Smaller heading
                class_name="text-slate-800 dark:text-slate-100 font-bold mb-0.5",
            ),
            rx.text(
                description,
                class_name=f"text-[9px] text-[{COLORS['text_muted']}] mb-1 leading-tight",
            ),
            class_name="px-4 pt-3 pb-1 flex-none",
        ),
        rx.box(
            rx.cond(
                PresetState.is_loading_presets,
                rx.center(
                    rx.spinner(size="3"),
                    class_name="w-full h-full flex items-center justify-center text-slate-300",
                ),
                rx.cond(
                    results.length() > 0,
                    rx.box(
                        custom_chart(
                            results,
                            chart_type,
                            x_axis_col=x_axis_col,
                            y_axis_cols=y_axis_cols,
                            show_legend=show_legend,
                            engine=engine,
                            title=title,
                            primary_color=primary_color,
                            color_palette=color_palette,
                            height="100%",  # Responsive to flex container
                            plotly_scatter_fig=plotly_scatter_fig,
                            plotly_bar_fig=plotly_bar_fig,
                            plotly_area_fig=plotly_area_fig,
                            plotly_horizontal_bar_fig=plotly_horizontal_bar_fig,
                            plotly_pie_fig=plotly_pie_fig,
                            plotly_stacked_bar_fig=plotly_stacked_bar_fig,
                        ),
                        class_name="w-full h-full flex-grow px-2 pb-2 min-h-0",
                    ),
                    rx.center(
                        rx.vstack(
                            rx.icon(
                                tag="bar-chart-3", size=24, class_name="text-slate-200"
                            ),
                            rx.text(
                                "No data",
                                class_name="text-[10px] text-slate-400 font-medium italic",
                            ),
                            align="center",
                            spacing="1",
                        ),
                        class_name="w-full h-full flex flex-col items-center justify-center bg-slate-50 dark:bg-slate-800/20 rounded-b-xl",
                    ),
                ),
            ),
            class_name="flex flex-col h-full w-full p-2",
        ),
        class_name=f"bg-[{COLORS['card_bg']}] dark:bg-slate-900 border border-[{COLORS['card_border']}] dark:border-slate-800 rounded-xl shadow-sm flex flex-col h-full overflow-hidden",
    )


def presets_page() -> rx.Component:
    """The Preset Visualizations page - strictly 4 charts on one screen."""
    return rx.box(
        # Top Navigation stays fixed at the top
        topnav(),
        # Main Layout horizontally split below header
        rx.hstack(
            sidebar(show_columns=False),
            rx.box(
                data_vintage_bar(),
                rx.cond(
                    AppState.selected_dataset == "",
                    # Empty State - No Dataset Selected
                    rx.center(
                        rx.vstack(
                            rx.icon(
                                tag="bar-chart-3",
                                size=64,
                                class_name="text-slate-200 mb-2",
                            ),
                            rx.heading(
                                "Preset Visualizations",
                                size="4",
                                class_name="text-slate-400 font-bold",
                            ),
                            rx.text(
                                "Select a dataset from the sidebar to view its configured dashboard presets.",
                                class_name="text-slate-400 text-sm max-w-xs text-center",
                            ),
                            align="center",
                            spacing="1",
                        ),
                        class_name=f"h-full bg-[{COLORS['datagrid_bg_light']}] dark:bg-slate-950 flex items-center justify-center p-20",
                    ),
                    # Dashboard Layout - Exactly 4 charts
                    rx.box(
                        rx.box(
                            rx.hstack(
                                rx.box(
                                    rx.heading(
                                        "Preset Visualizations",
                                        size="5",
                                        class_name="text-slate-800 dark:text-slate-100 font-extrabold",
                                    ),
                                    rx.text(
                                        "Curated visual insights for the active dataset.",
                                        class_name="text-slate-500 text-[10px]",
                                    ),
                                ),
                                rx.spacer(),
                                align="center",
                                width="100%",
                                class_name="mb-4",
                            ),
                            rx.grid(
                                rx.foreach(
                                    PresetState.current_presets.to(list),
                                    _render_chart_tile,
                                ),
                                columns="2",
                                spacing="4",
                                class_name="w-full flex-1 min-h-0",
                            ),
                            class_name="flex flex-col h-full",
                        ),
                        class_name=f"flex-1 bg-[{COLORS['datagrid_bg_light']}] dark:bg-slate-950 overflow-hidden p-6 h-full {UI_CONFIG['SCROLLBAR_STYLE']}",
                    ),
                ),
                class_name="flex flex-col flex-1 min-w-0 bg-background-light dark:bg-background-dark overflow-hidden h-full",
            ),
            class_name="flex flex-1 overflow-hidden h-full min-h-0",
            width="100%",
        ),
        class_name="bg-background-light dark:bg-background-dark text-slate-900 dark:text-slate-100 h-screen max-h-screen w-screen flex flex-col overflow-hidden",
    )
