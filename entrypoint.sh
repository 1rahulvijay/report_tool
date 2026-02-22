#!/bin/bash

# Start Nginx (Reverse Proxy)
echo "Starting Nginx..."
nginx -c /app/nginx.conf -g 'daemon off;' &

# Start the FastAPI Backend
echo "Starting Aurora Backend (FastAPI)..."
cd /app/backend
uvicorn app.main:app --host 127.0.0.1 --port 8080 --workers 4 &

# Start the Reflex Backend
echo "Starting Aurora Reflex Backend..."
cd /app/frontend
reflex run --env prod --backend-only
