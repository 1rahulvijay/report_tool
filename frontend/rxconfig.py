import reflex as rx

config = rx.Config(
    app_name="frontend",
    plugins=[
        rx.plugins.SitemapPlugin(),
        rx.plugins.TailwindV3Plugin(),
    ],
    frontend_packages=[],
    # Expose on local network so devices on same WiFi can access (like Streamlit/Flask --host 0.0.0.0)
    backend_host="0.0.0.0",
)

# To access from other devices on the same network:
# 1. Run: reflex run   (backend will bind to 0.0.0.0)
# 2. Find your PC's LAN IP (e.g. ipconfig on Windows → 192.168.1.x)
# 3. On other devices, open http://YOUR_LAN_IP:3000
# If the frontend can't reach the Reflex backend from another device, set before running:
#   set REFLEX_API_URL=http://YOUR_LAN_IP:8000
#   set REFLEX_DEPLOY_URL=http://YOUR_LAN_IP:3000
# Note: The Aurora FastAPI backend (port 8080) is separate; run it with uvicorn --host 0.0.0.0
# and update API_BASE_URL in state.py to http://YOUR_LAN_IP:8080 if you need it from other devices.
