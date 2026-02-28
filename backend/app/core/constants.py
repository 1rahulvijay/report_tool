# Excel Export Styling
EXCEL_STYLES = {
    "header_bg": "#0f172a",  # Dark Slate 900
    "header_font": "#ffffff",  # White
    "body_bg": "#ffffff",  # White
    "body_alt_bg": "#f8fafc",  # Slate 50
    "body_font": "#000000",  # Black
    "border_color": "#e2e8f0",  # Slate 200
    "accent_tab": "#1e3a8a",  # Dark Blue
}

# CSV Configuration
CSV_ENCODING = "utf-8-sig"  # Includes BOM for Excel compatibility

# Export Chunking
EXPORT_CHUNK_SIZE = 10000
STREAM_BUFFER_SIZE = 65536
