# scripts/start_dashboard.ps1
# Starts the evaluation bridge API and Vite dev server
#
# Prerequisites:
#   Python:  fastapi, uvicorn  (pip install fastapi uvicorn)
#   Node.js: https://nodejs.org  (LTS recommended)
#
# First run: Node dependencies are installed automatically from ui/package.json

$Root = Split-Path $PSScriptRoot -Parent
$Venv = Join-Path $Root ".venv\Scripts\Activate.ps1"
$UiDir = Join-Path $Root "ui"

# Activate venv
if (Test-Path $Venv) {
    & $Venv
} else {
    Write-Warning "No .venv found at $Venv — make sure the project venv is active."
}

# Check Node.js availability
if (-not (Get-Command npm -ErrorAction SilentlyContinue)) {
    Write-Error "npm not found. Install Node.js LTS from https://nodejs.org then re-run this script."
    exit 1
}

# Install Node deps if needed
if (-not (Test-Path (Join-Path $UiDir "node_modules"))) {
    Write-Host "Installing Node dependencies..."
    Push-Location $UiDir
    npm install
    Pop-Location
}

# Start the Python bridge API in a background job
$ApiJob = Start-Job -ScriptBlock {
    param($root)
    Set-Location $root
    & "$root\.venv\Scripts\python.exe" -m uvicorn ui.server:app --port 8765 --reload
} -ArgumentList $Root

Write-Host "Bridge API starting on http://localhost:8765 (job id: $($ApiJob.Id))"
Write-Host "Starting Vite dev server..."

Push-Location $UiDir
npm run dev
Pop-Location

# Cleanup: stop API job when Vite exits
Stop-Job  $ApiJob
Remove-Job $ApiJob
