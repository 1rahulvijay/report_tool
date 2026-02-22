$ErrorActionPreference = "Continue"

Write-Host "===================================================" -ForegroundColor Cyan
Write-Host "Stopping Aurora Nexus (Backend + Frontend)" -ForegroundColor Yellow
Write-Host "===================================================" -ForegroundColor Cyan

# 1. Kill Python processes (FastAPI and Reflex backend)
Write-Host "Stopping Python processes..." -ForegroundColor Gray
Stop-Process -Name "python" -Force -ErrorAction SilentlyContinue

# 2. Kill Node/Bun/Web processes (Reflex frontend)
Write-Host "Stopping Node.js / Web processes..." -ForegroundColor Gray
Stop-Process -Name "node" -Force -ErrorAction SilentlyContinue

# 3. Kill any residual react-router or bun processes
Stop-Process -Name "react-router" -Force -ErrorAction SilentlyContinue

Write-Host "All services stopped successfully." -ForegroundColor Green
Write-Host "Press any key to close..."
$null = $Host.UI.RawUI.ReadKey("NoEcho,IncludeKeyDown")
