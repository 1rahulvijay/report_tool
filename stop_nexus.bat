@echo off
echo ===================================================
echo Stopping Aurora Nexus (Backend + Frontend)
echo ===================================================

echo Stopping Python processes...
taskkill /F /IM python.exe /T 2>nul
taskkill /F /IM uvicorn.exe /T 2>nul

echo Stopping Node.js / Web processes...
taskkill /F /IM node.exe /T 2>nul

echo Stopping any residual react-router or bun processes...
taskkill /F /IM react-router.exe /T 2>nul
taskkill /F /IM bun.exe /T 2>nul

echo.
echo All services stopped successfully.
pause
