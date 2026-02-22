$ErrorActionPreference = "Stop"

Write-Host "===================================================" -ForegroundColor Cyan
Write-Host "Starting Aurora Nexus (Backend + Frontend)" -ForegroundColor Green
Write-Host "===================================================" -ForegroundColor Cyan

# 1. Start the FastAPI + Oracle Backend in a new window
Write-Host "Starting Backend..." -ForegroundColor Yellow
Start-Process powershell.exe -ArgumentList "-NoExit", "-Command", "cd '$PSScriptRoot\backend'; uvicorn app.main:app --port 8080 --reload"

# 2. Wait a few seconds to let the backend start up
Start-Sleep -Seconds 3

# 3. Start the Reflex Frontend in a new window
Write-Host "Starting Frontend..." -ForegroundColor Yellow
Start-Process powershell.exe -ArgumentList "-NoExit", "-Command", "cd '$PSScriptRoot\frontend'; `$env:PYTHONIOENCODING='utf-8'; reflex run"

Write-Host "Both services are starting up in new windows!" -ForegroundColor Green
Write-Host "Press any key to close this window..."
$null = $Host.UI.RawUI.ReadKey("NoEcho,IncludeKeyDown")