import os

# ─── API & Network Configuration ───────────────────────────────────────────
API_BASE_URL = os.getenv("API_BASE_URL", "http://localhost:8080/api/v1")
EXPORT_CSV_TIMEOUT = float(os.getenv("EXPORT_CSV_TIMEOUT", "3000.0"))
EXPORT_EXCEL_MAX_ROWS = int(os.getenv("EXPORT_EXCEL_MAX_ROWS", "100000"))

# ─── UI Color Palette ──────────────────────────────────────────────────────
COLORS = {
    "primary": "#6366f1",  # Indigo 500
    "primary_dark": "#4f46e5",  # Indigo 600
    "success": "#10b981",  # Emerald 500
    "warning": "#f59e0b",  # Amber 500
    "danger": "#ef4444",  # Red 500
    "accent": "#8b5cf6",  # Violet 500
    "info": "#06b6d4",
    "grid": "#e2e8f0",
    "text_muted": "#64748b",
    "text_dark": "#0f172a",
    "border": "#e2e8f0",
    "bg_light": "#f8fafc",
    "bg_dark": "#0f172a",
    # Specific UI Section Colors
    "header_bg": "#020617",
    "sidebar_bg_dark": "#0b1120",
    "datagrid_bg_dark": "#0f172a",
    "white": "#ffffff",
    "datagrid_bg_light": "#f8fafc",
    "card_bg": "#ffffff",
    "card_border": "#e2e8f0",
}

VIBRANT_PALETTE = [
    "#6366f1",  # Indigo
    "#10b981",  # Emerald
    "#f59e0b",  # Amber
    "#ef4444",  # Red
    "#8b5cf6",  # Violet
    "#06b6d4",  # Cyan
    "#f472b6",  # Pink
    "#fb923c",  # Orange
]

# ─── UI Layout Configuration ───────────────────────────────────────────────
UI_CONFIG = {
    "APP_NAME": "Data Engine",
    "NAVBAR_HEIGHT": "3rem",  # h-12
    "SIDEBAR_WIDTH": "280px",
    "CONTAINER_MAX_WIDTH": "1400px",
    "MODAL_WIDTH": "800px",
    "SCROLLBAR_STYLE": "custom-scrollbar",
    "ROUTING_LINKS": [
        {"name": "Explorer", "icon": "database", "path": "/"},
        {"name": "Presets", "icon": "layout-dashboard", "path": "/presets"},
    ],
    "FEATURES": {
        "SHOW_JOIN_BUTTON": True,
        "SHOW_BUILDER_BUTTON": True,
        "SHOW_EXPORT_MENU": True,
        "SHOW_VIRTUAL_SCROLL_TOGGLE": True,
        "SHOW_IN_MEMORY_TOGGLE": True,
    },
}

# ─── Chart Defaults ────────────────────────────────────────────────────────
CHART_DEFAULTS = {
    "height": 320,
    "stroke_width": 2,
    "fill_opacity": 0.15,
    "font_size": 10,
    "tooltip_font_size": 11,
}

# ─── Data Management & Pagination ──────────────────────────────────────────
PAGINATION = {
    "default_page_size": 20,
    "page_size_options": ["10", "20", "50", "100"],
}

QUERY_DEBOUNCE_DELAY = 0.3  # Seconds
EXPORT_POLLING_INTERVAL = 1.0  # Seconds
MAX_EXPORT_POLLS = 300  # 5 minutes at 1s interval
PRESET_RAW_QUERY_TIMEOUT = 30.0  # Seconds
