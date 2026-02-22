@echo off
echo ===================================================
echo Starting Aurora Nexus (Backend + Frontend)
echo ===================================================

:: 1. Start the FastAPI + Oracle Backend in a new window
echo Starting Backend...
start "Aurora Backend" cmd /k "cd /d %~dp0\backend && uvicorn app.main:app --port 8080 --reload"

:: 2. Wait a few seconds to let the backend start up
timeout /t 3 /nobreak > nul

:: 3. Start the Reflex Frontend in a new window
echo Starting Frontend...
start "Aurora Frontend" cmd /k "cd /d %~dp0\frontend && set PYTHONIOENCODING=utf-8&& reflex run"

echo Both services are starting up! 
echo The frontend will be available at http://localhost:3000 (or http://localhost:3006 depending on your port bindings).
