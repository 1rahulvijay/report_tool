@echo off
echo ===================================================
echo     Reflex Frontend Complete Rebuild Script
echo ===================================================
echo.

echo [1/5] Removing build artifacts (.web, __pycache__, .reflex)...
if exist .web rmdir /s /q .web
if exist __pycache__ rmdir /s /q __pycache__
if exist .reflex rmdir /s /q .reflex
echo Artifacts removed successfully.
echo.

echo [2/5] Initializing Reflex project...
call reflex init
echo.

echo [3/5] Patching jsesc dependency in .web...
if exist .web (
    cd .web
    call npm install jsesc@3.0.2 --legacy-peer-deps
    cd ..
    echo Dependency patch applied successfully.
) else (
    echo WARNING: .web directory not found. Patching skipped.
)
echo.

echo [4/5] Exporting Reflex app for production build (this may take a moment)...
call reflex export
echo.

echo [5/5] Starting the frontend development environment...
echo The app will be available at http://localhost:3000/
call reflex run

pause
