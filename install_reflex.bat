@echo off
echo ===================================================
echo     Aurora Nexus - Clean Install Reflex
echo ===================================================
echo.

echo [1/3] Initializing fresh Reflex environment...
cd /d %~dp0\frontend
reflex init

echo [2/3] Installing strictly compatible dependencies...
cd .web
:: jsesc@3.0.2 is historically required to prevent Babel build errors 
call npm install jsesc@3.0.2 --legacy-peer-deps
call npm install --legacy-peer-deps
echo Running npm audit fix to resolve vulnerabilities...
call npm audit fix
cd ..

echo [3/3] Exporting production build to verify installation...
set PYTHONIOENCODING=utf-8
reflex export

echo.
echo ===================================================
echo Reflex installation and frontend build are complete!
echo You can now run start_nexus.bat to launch the app.
echo ===================================================
pause
