@echo off
cd /d "%~dp0"
echo ===================================================
echo     Aurora Nexus - Clean Reflex Dependencies
echo ===================================================
echo.
if exist "frontend\.web" rmdir /s /q "frontend\.web"
if exist "frontend\.reflex" rmdir /s /q "frontend\.reflex"
if exist "frontend\.states" rmdir /s /q "frontend\.states"

echo Removing Node Modules globally...
for /f "delims=" %%d in ('dir /s /b /ad node_modules 2^>nul') do (
    rmdir /s /q "%%d"
)

echo Removing stray package files and zip backups...
if exist "package.json" del /f /q "package.json"
if exist "package-lock.json" del /f /q "package-lock.json"
if exist "frontend.zip" del /f /q "frontend.zip"
if exist "backend.zip" del /f /q "backend.zip"
if exist "frontend\frontend.zip" del /f /q "frontend\frontend.zip"
if exist "frontend\backend.zip" del /f /q "frontend\backend.zip"
if exist "frontend\package.json" del /f /q "frontend\package.json"
if exist "frontend\package-lock.json" del /f /q "frontend\package-lock.json"

echo.
echo Reflex dependencies have been successfully cleaned!
pause
