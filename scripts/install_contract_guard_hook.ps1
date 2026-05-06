$ErrorActionPreference = 'Stop'

$repoRoot = Split-Path -Parent $PSScriptRoot
Set-Location $repoRoot

git config core.hooksPath .githooks
Write-Host "Configured git hooks path to .githooks"
Write-Host "Contract guard will now run on pre-commit."
