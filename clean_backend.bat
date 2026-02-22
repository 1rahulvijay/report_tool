@echo off
cd /d "%~dp0"
echo ===================================================
echo     Aurora Nexus - Clean Python ^& Backend Caches
echo ===================================================
echo.
for /f "delims=" %%d in ('dir /s /b /ad __pycache__ 2^>nul') do (
    rmdir /s /q "%%d"
)

echo Removing pytest cache directories...
for /f "delims=" %%d in ('dir /s /b /ad .pytest_cache 2^>nul') do (
    rmdir /s /q "%%d"
)

echo Removing leftover coverage data...
if exist ".coverage" del /f /q ".coverage"
if exist "backend\.coverage" del /f /q "backend\.coverage"

echo Purging temporary Excel/CSV export buffers...
if exist "%TMP%\aurora_exports" (
    rmdir /s /q "%TMP%\aurora_exports"
)

echo.
echo ===================================================
echo Python and Backend caches are clean!
pause
