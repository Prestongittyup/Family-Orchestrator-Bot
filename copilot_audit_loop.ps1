$manifest = Get-Content .\audit_manifest.txt
$progressFile = ".\audit_progress.txt"

if (!(Test-Path $progressFile)) {
    New-Item $progressFile -ItemType File | Out-Null
}

$progress = Get-Content $progressFile

# normalize
$remaining = $manifest | Where-Object { $_ -and ($_ -notin $progress) }

Write-Host "Total remaining files: $($remaining.Count)"

$batchSize = 10
$batch = $remaining | Select-Object -First $batchSize

Write-Host "`n=== NEXT COPILOT BATCH ===`n"

Write-Host "PASTE THIS INTO COPILOT CHAT:`n"

Write-Host @"
You are continuing a strict repository audit.

RULES:
- Only process these files
- Do NOT skip any file
- Do NOT summarize the repo
- Must include line-level findings when issues exist
- Append results to audit_log.md (conceptually)
- After completion, confirm file completion list

FILES:
$($batch -join "`n")

FORMAT:
## FILE: <path>
### Overview
### Issues (line-level)
### Security Risks
### Performance Issues
### Notes

After finishing, explicitly list completed files.
"@

Write-Host "`n=== AFTER COPILOT RESPONDS ==="
Write-Host "Run this to update progress:`n"

$batch | ForEach-Object { "Add-Content $progressFile `"$($_)`"" } | ForEach-Object { Write-Host $_ }