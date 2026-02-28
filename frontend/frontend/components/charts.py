import reflex as rx
from frontend.config import COLORS, VIBRANT_PALETTE, CHART_DEFAULTS


def _render_recharts(
    data: rx.Var,
    chart_type: rx.Var,
    x_axis_col: str,
    y_axis_cols: list[str],
    show_legend: bool,
    primary_color: str,
    color_palette: rx.Var,
    height: int | str,
    stroke_width: int,
    fill_opacity: float,
) -> rx.Component:
    """Internal helper to render Recharts components with premium aesthetics."""
    # Premium Grid Styling
    grid = rx.recharts.cartesian_grid(
        stroke_dasharray="3 3",
        vertical=False,
        stroke=COLORS["grid"],
        opacity=0.6,
    )

    # Clean axes - increase bottom padding slightly for longer labels, disable tick truncation
    xaxis = rx.recharts.x_axis(
        data_key=x_axis_col,
        font_size=CHART_DEFAULTS["font_size"],
        tick_line=False,
        axis_line=False,
        tick={"fill": COLORS["text_muted"], "dy": 8},
        height=35,  # Ensure labels don't get cut off vertically
        padding={"left": 10, "right": 10},
    )

    yaxis = rx.recharts.y_axis(
        font_size=CHART_DEFAULTS["font_size"],
        tick_line=False,
        axis_line=False,
        tick={"fill": COLORS["text_muted"]},
        width=50,  # Widen the Y-axis so larger numbers don't truncate
    )

    # Modern Glassmorphism Tooltip
    tooltip = rx.recharts.tooltip(
        content_style={
            "borderRadius": "8px",
            "border": f"1px solid {COLORS['border']}",
            "backgroundColor": "rgba(255, 255, 255, 0.9)",
            "backdropFilter": "blur(8px)",
            "boxShadow": "0 4px 6px -1px rgb(0 0 0 / 0.1), 0 2px 4px -2px rgb(0 0 0 / 0.1)",
            "padding": "8px 12px",
            "fontSize": f"{CHART_DEFAULTS['tooltip_font_size']}px",
            "fontWeight": "500",
            "color": COLORS["text_dark"],
        }
    )

    legend = rx.cond(
        show_legend,
        rx.recharts.legend(
            vertical_align="bottom",
            height=30,
            wrapper_style={
                "paddingTop": "8px",
                "fontSize": f"{CHART_DEFAULTS['font_size']}px",
            },
        ),
        rx.fragment(),  # Empty placeholder
    )

    # Select primary y-axis column for simple charts
    # We guarantee y_axis_cols has at least 1 element in preset_state.py, but it comes back as a Union sequence Var.
    # Casting to list first solves the Var TypeError for item access over Union Types.
    primary_y = y_axis_cols.to(list)[0]

    return rx.match(
        chart_type,
        (
            "bar",
            rx.recharts.bar_chart(
                rx.recharts.bar(
                    rx.foreach(
                        color_palette.to(list),
                        lambda color, i: rx.recharts.cell(fill=color),
                    ),
                    data_key=primary_y,
                    fill=primary_color,
                    radius=[6, 6, 0, 0],
                    bar_size=32,
                    animation_duration=800,
                ),
                xaxis,
                yaxis,
                grid,
                tooltip,
                legend,
                data=data,
                height=height,
                width="100%",
                margin={"top": 20, "right": 10, "left": -10, "bottom": 0},
            ),
        ),
        (
            "horizontal_bar",
            rx.recharts.bar_chart(
                rx.recharts.bar(
                    rx.foreach(
                        color_palette.to(list),
                        lambda color, i: rx.recharts.cell(fill=color),
                    ),
                    data_key=primary_y,
                    fill=primary_color,
                    radius=[0, 6, 6, 0],  # Rounded corners on the right
                    bar_size=24,
                    animation_duration=800,
                ),
                rx.recharts.x_axis(
                    type_="number",
                    hide=False,
                    font_size=CHART_DEFAULTS["font_size"],
                    tick={"fill": COLORS["text_muted"]},
                ),
                rx.recharts.y_axis(
                    data_key=x_axis_col,
                    type_="category",
                    width=80,
                    font_size=CHART_DEFAULTS["font_size"],
                    tick_line=False,
                    axis_line=False,
                    tick={"fill": COLORS["text_muted"]},
                ),
                tooltip,
                legend,
                data=data,
                layout="vertical",
                height=height,
                width="100%",
                margin={"top": 10, "right": 20, "left": 10, "bottom": 0},
            ),
        ),
        (
            "stacked_bar",
            rx.recharts.bar_chart(
                rx.foreach(
                    rx.Var.create(VIBRANT_PALETTE).to(list),
                    lambda color, i: rx.cond(
                        i < y_axis_cols.to(list).length(),
                        rx.recharts.bar(
                            data_key=y_axis_cols.to(list)[i],
                            fill=color,
                            stack_id="a",
                            animation_duration=800,
                        ),
                        rx.fragment(),
                    ),
                ),
                xaxis,
                yaxis,
                grid,
                tooltip,
                legend,
                data=data,
                height=height,
                width="100%",
                margin={"top": 20, "right": 10, "left": -10, "bottom": 0},
            ),
        ),
        (
            "line",
            rx.recharts.line_chart(
                rx.recharts.line(
                    data_key=primary_y,
                    stroke=primary_color,
                    stroke_width=stroke_width + 1,
                    dot={
                        "r": 4,
                        "fill": COLORS["white"],
                        "stroke": primary_color,
                        "strokeWidth": 2,
                    },
                    active_dot={"r": 6, "stroke": primary_color, "strokeWidth": 0},
                    animation_duration=800,
                ),
                xaxis,
                yaxis,
                grid,
                tooltip,
                legend,
                data=data,
                height=height,
                width="100%",
                margin={"top": 20, "right": 10, "left": -10, "bottom": 0},
            ),
        ),
        (
            "pie",
            rx.recharts.pie_chart(
                rx.recharts.pie(
                    rx.foreach(
                        color_palette.to(list),
                        lambda color, i: rx.recharts.cell(fill=color),
                    ),
                    data=data,
                    data_key=primary_y,
                    name_key=x_axis_col,
                    cx="50%",
                    cy="50%",
                    outer_radius="80%",
                    inner_radius="0%",
                    padding_angle=0,
                    stroke="none",
                    animation_duration=800,
                ),
                tooltip,
                legend,
                height=height,
                width="100%",
                margin={"top": 10, "right": 10, "left": 10, "bottom": 10},
            ),
        ),
        (
            "donut",
            rx.recharts.pie_chart(
                rx.recharts.pie(
                    rx.foreach(
                        color_palette.to(list),
                        lambda color, i: rx.recharts.cell(fill=color),
                    ),
                    data=data,
                    data_key=primary_y,
                    name_key=x_axis_col,
                    cx="50%",
                    cy="50%",
                    outer_radius="80%",
                    inner_radius="60%",
                    padding_angle=3,
                    stroke="none",
                    animation_duration=800,
                ),
                tooltip,
                legend,
                height=height,
                width="100%",
                margin={"top": 10, "right": 10, "left": 10, "bottom": 10},
            ),
        ),
        (
            "scatter",
            rx.recharts.scatter_chart(
                rx.recharts.x_axis(
                    data_key=x_axis_col,
                    type_="number",
                    name=x_axis_col,
                    font_size=CHART_DEFAULTS["font_size"],
                ),
                rx.recharts.y_axis(
                    data_key=primary_y,
                    type_="number",
                    name=primary_y,
                    font_size=CHART_DEFAULTS["font_size"],
                ),
                rx.recharts.scatter(
                    data=data, fill=primary_color, animation_duration=800
                ),
                grid,
                tooltip,
                legend,
                height=height,
                width="100%",
                margin={"top": 20, "right": 20, "bottom": 20, "left": 20},
            ),
        ),
        (
            "column",
            rx.recharts.bar_chart(
                rx.recharts.bar(
                    data_key=primary_y,
                    fill=primary_color,
                    radius=[6, 6, 0, 0],
                    bar_size=32,
                    animation_duration=800,
                ),
                xaxis,
                yaxis,
                grid,
                tooltip,
                legend,
                data=data,
                height=height,
                width="100%",
                margin={"top": 20, "right": 10, "left": -10, "bottom": 0},
            ),
        ),
        # Default: Area (with premium gradient)
        rx.recharts.area_chart(
            # Define gradient payload
            rx.el.svg.defs(
                rx.el.svg.linear_gradient(
                    rx.el.svg.stop(
                        offset="5%", stop_color=primary_color, stop_opacity=0.3
                    ),
                    rx.el.svg.stop(
                        offset="95%", stop_color=primary_color, stop_opacity=0
                    ),
                    id="colorGradient",
                    x1="0",
                    y1="0",
                    x2="0",
                    y2="1",
                )
            ),
            rx.recharts.area(
                data_key=primary_y,
                stroke=primary_color,
                fill="url(#colorGradient)",  # Use the gradient
                stroke_width=stroke_width,
                type="monotone",
                animation_duration=800,
            ),
            xaxis,
            yaxis,
            grid,
            tooltip,
            data=data,
            height=height,
            width="100%",
            margin={"top": 20, "right": 10, "left": -10, "bottom": 0},
        ),
    )


def _render_plotly(
    chart_type: rx.Var,
    height: int | str,
    scatter_fig: rx.Var,
    bar_fig: rx.Var,
    area_fig: rx.Var,
    horizontal_bar_fig: rx.Var,
    pie_fig: rx.Var,
    stacked_bar_fig: rx.Var,
) -> rx.Component:
    """Internal helper to render Plotly components using pre-constructed Figure objects from State."""
    css_height = f"{height}px" if isinstance(height, int) else height
    return rx.match(
        chart_type,
        (
            "scatter",
            rx.plotly(
                data=scatter_fig,
                height=css_height,
                style={"width": "100%"},
                config={"displayModeBar": False},
            ),
        ),
        (
            "bar",
            rx.plotly(
                data=bar_fig,
                height=css_height,
                style={"width": "100%"},
                config={"displayModeBar": False},
            ),
        ),
        (
            "area",
            rx.plotly(
                data=area_fig,
                height=css_height,
                style={"width": "100%"},
                config={"displayModeBar": False},
            ),
        ),
        (
            "horizontal_bar",
            rx.plotly(
                data=horizontal_bar_fig,
                height=css_height,
                style={"width": "100%"},
                config={"displayModeBar": False},
            ),
        ),
        (
            "pie",
            rx.plotly(
                data=pie_fig,
                height=css_height,
                style={"width": "100%"},
                config={"displayModeBar": False},
            ),
        ),
        (
            "stacked_bar",
            rx.plotly(
                data=stacked_bar_fig,
                height=css_height,
                style={"width": "100%"},
                config={"displayModeBar": False},
            ),
        ),
        # Default fallback to area
        rx.plotly(
            data=area_fig,
            height=css_height,
            style={"width": "100%"},
            config={"displayModeBar": False},
        ),
    )


def custom_chart(
    data: rx.Var,
    chart_type: rx.Var,
    x_axis_col: rx.Var = "x_axis",
    y_axis_cols: rx.Var = ["y_axis"],
    show_legend: rx.Var = False,
    engine: str = "recharts",
    title: str = "",
    primary_color: str = COLORS["primary"],
    color_palette: rx.Var = VIBRANT_PALETTE,
    height: int | str = CHART_DEFAULTS["height"],
    stroke_width: int = CHART_DEFAULTS["stroke_width"],
    fill_opacity: float = CHART_DEFAULTS["fill_opacity"],
    # For Plotly, we pass pre-calculated Figure objects to avoid Var-in-dict compilation issues
    plotly_scatter_fig: rx.Var = None,
    plotly_bar_fig: rx.Var = None,
    plotly_area_fig: rx.Var = None,
    plotly_horizontal_bar_fig: rx.Var = None,
    plotly_pie_fig: rx.Var = None,
    plotly_stacked_bar_fig: rx.Var = None,
) -> rx.Component:
    """
    Main entry point for rendering a chart.
    """
    return rx.box(
        rx.cond(
            engine == "plotly",
            _render_plotly(
                chart_type,
                height,
                plotly_scatter_fig,
                plotly_bar_fig,
                plotly_area_fig,
                plotly_horizontal_bar_fig,
                plotly_pie_fig,
                plotly_stacked_bar_fig,
            ),
            _render_recharts(
                data,
                chart_type,
                x_axis_col,
                y_axis_cols,
                show_legend,
                primary_color,
                color_palette,
                height,
                stroke_width,
                fill_opacity,
            ),
        ),
        class_name="w-full h-full",
    )
