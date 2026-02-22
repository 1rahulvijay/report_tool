@echo off
SETLOCAL EnableDelayedExpansion

echo ===================================================
echo   Aurora Nexus - Corporate Environment Setup
echo ===================================================
echo.

:: --- PROXY CONFIGURATION ---
echo [0/4] Proxy Configuration
echo -------------------------
set /p USE_PROXY="Do you need to configure a corporate proxy? (y/n): "
if /i "%USE_PROXY%"=="y" (
    set /p PROXY_HOST="Enter Proxy Host (e.g. proxy.company.com): "
    set /p PROXY_PORT="Enter Proxy Port (e.g. 8080): "
    set /p PROXY_USER="Enter Username: "
    set /p PROXY_PASS="Enter Password: "
    
    set "PROXY_URL=http://!PROXY_USER!:!PROXY_PASS!@!PROXY_HOST!:!PROXY_PORT!"
    
    echo Setting NPM Proxy...
    call npm config set proxy !PROXY_URL!
    call npm config set https-proxy !PROXY_URL!
    
    echo Setting Environment Variables for PIP...
    set "HTTP_PROXY=!PROXY_URL!"
    set "HTTPS_PROXY=!PROXY_URL!"
    
    echo Proxy configured!
)
echo.

:: --- ARTIFACTORY CONFIGURATION ---
echo [0.5/4] JFrog Artifactory Configuration
echo ---------------------------------------
set /p USE_ARTIFACTORY="Do you need to use a private JFrog Artifactory? (y/n): "
if /i "%USE_ARTIFACTORY%"=="y" (
    set /p ARTI_URL="Enter Artifactory Base URL (e.g. https://artifactory.company.com/artifactory): "
    set /p ARTI_NPM_PATH="Enter NPM Repository Key (e.g. npm-repo): "
    set /p ARTI_PYPI_PATH="Enter PyPI Repository Key (e.g. pypi-repo): "
    
    echo Configuring NPM Registry...
    call npm config set registry !ARTI_URL!/api/npm/!ARTI_NPM_PATH!/
    
    echo Configuring PIP Index...
    :: Set as environment variable for the current session installations
    set "PIP_INDEX_URL=!ARTI_URL!/api/pypi/!ARTI_PYPI_PATH!/simple"
    
    echo Artifactory configured!
)
echo.

:: 1. Check for Python
python --version >nul 2>&1
if !errorlevel! neq 0 (
    echo [ERROR] Python not found! Please install Python 3.9+ from python.org
    pause
    exit /b 1
)

:: 2. Setup Virtual Environment (Recommended)
echo [1/4] Creating Virtual Environment...
python -m venv venv
call venv\Scripts\activate.bat

:: 3. Install Backend Dependencies
echo [2/4] Installing Backend Dependencies...
cd backend
python -m pip install --upgrade pip
pip install -r requirements.txt
cd ..

:: 4. Install Frontend Dependencies
echo [3/4] Installing Frontend Dependencies...
cd frontend
pip install -r requirements.txt
echo Initializing Reflex (This may take a minute)...
reflex init
cd ..

:: 5. Final Configuration Check
echo.
echo [4/4] Configuration Check
echo -------------------------
if not exist "backend\.env" (
    echo [WARNING] backend\.env not found!
    echo Creating template...
    echo DB_ENGINE=oracledb > backend\.env
    echo ORACLE_USER=YOUR_PROD_USER >> backend\.env
    echo ORACLE_PASSWORD=YOUR_PROD_PASSWORD >> backend\.env
    echo ORACLE_DSN=YOUR_PROD_DSN >> backend\.env
)

echo.
echo ===================================================
echo   SETUP COMPLETE!
echo ===================================================
echo.
echo NEXT STEPS:
echo 1. Open 'backend\.env' and update your Oracle credentials.
echo 2. Run 'start_nexus.ps1' or 'start_nexus.bat' to launch.
echo.
pause
