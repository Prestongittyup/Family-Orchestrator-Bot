<#
.SYNOPSIS
  Production-safe Git workflow for initializing, committing, pushing, and verifying a repository against GitHub.
.DESCRIPTION
  This script ensures the current folder is a git repository, verifies or creates the main branch, configures origin, stages and commits changes, protects sensitive files, and verifies push success.
.PARAMETER GitHubUser
  GitHub account or organization name for the repository remote.
.PARAMETER GitHubRepo
  Repository name on GitHub.
.EXAMPLE
  .\git-setup-verify.ps1 -GitHubUser Contoso -GitHubRepo "family-orchestration-bot"
#>

param(
    [Parameter(Mandatory=$true)]
    [string]$GitHubUser,

    [Parameter(Mandatory=$true)]
    [string]$GitHubRepo
)

Set-StrictMode -Version Latest
$ErrorActionPreference = 'Stop'

function Write-Step([string]$text) {
    Write-Host "[STEP] $text"
}

function Write-Warn([string]$text) {
    Write-Host "[WARN] $text" -ForegroundColor Yellow
}

function Write-ErrorAndExit([string]$text) {
    Write-Host "[ERROR] $text" -ForegroundColor Red
    exit 1
}

function Normalize-GitHubUrl([string]$url) {
    if (-not $url) { return $null }
    $normalized = $url.Trim()
    $normalized = $normalized -replace '^git@github\.com:(.+)$', 'https://github.com/$1'
    $normalized = $normalized -replace '^ssh://git@github\.com/(.+)$', 'https://github.com/$1'
    return $normalized.TrimEnd('/')
}

function Guard-SensitiveFiles() {
    Write-Step 'Scanning for sensitive filenames before pushing'
    $patterns = 'credentials','token','oauth','\.env'
    $sensitiveFiles = Get-ChildItem -Path . -Recurse -Force -File -ErrorAction SilentlyContinue |
        Where-Object { $_.FullName -notmatch '\\.git\\' } |
        Where-Object {
            $name = $_.Name.ToLowerInvariant()
            foreach ($pattern in $patterns) {
                if ($name -match $pattern) { return $true }
            }
            return $false
        }

    if ($sensitiveFiles) {
        Write-Warn 'Potential sensitive files detected. Push is blocked until you review them.'
        $sensitiveFiles | ForEach-Object { Write-Host "  - $($_.FullName)" }
        Write-Host
        Write-Host 'If these files are intentional, move them outside the repository or add them to .gitignore before pushing.'
        exit 1
    }
}

function Run-Git([string[]]$args) {
    $processInfo = New-Object System.Diagnostics.ProcessStartInfo
    $processInfo.FileName = 'git'
    $processInfo.Arguments = $args -join ' '
    $processInfo.RedirectStandardOutput = $true
    $processInfo.RedirectStandardError = $true
    $processInfo.UseShellExecute = $false
    $processInfo.CreateNoWindow = $true

    $process = New-Object System.Diagnostics.Process
    $process.StartInfo = $processInfo
    $process.Start() | Out-Null
    $stdout = $process.StandardOutput.ReadToEnd()
    $stderr = $process.StandardError.ReadToEnd()
    $process.WaitForExit()

    return @{ ExitCode = $process.ExitCode; StdOut = $stdout.Trim(); StdErr = $stderr.Trim() }
}

# Ensure current directory is a git repository.
Write-Step 'Checking whether current directory is a git repository'
$insideGit = Run-Git @('rev-parse','--is-inside-work-tree')
if ($insideGit.ExitCode -ne 0) {
    Write-Step 'Not a repository yet; initializing git repository'
    $init = Run-Git @('init')
    if ($init.ExitCode -ne 0) {
        Write-ErrorAndExit "git init failed: $($init.StdErr)"
    }
    Write-Host $init.StdOut
} else {
    Write-Host 'Already inside a git repository.'
}

# Ensure the current branch is main, creating it if necessary.
Write-Step 'Confirming the current branch is main'
$branchResult = Run-Git @('rev-parse','--abbrev-ref','HEAD')
if ($branchResult.ExitCode -ne 0) {
    Write-ErrorAndExit "Unable to determine current branch: $($branchResult.StdErr)"
}

$currentBranch = $branchResult.StdOut
if ($currentBranch -eq 'HEAD') {
    Write-Warn 'Detached HEAD detected; creating or checking out main explicitly.'
}

$mainExists = Run-Git @('show-ref','--verify','--quiet','refs/heads/main')
if ($currentBranch -ne 'main') {
    if ($mainExists.ExitCode -ne 0) {
        Write-Step 'Creating local branch main from current HEAD'
        $createMain = Run-Git @('branch','main')
        if ($createMain.ExitCode -ne 0) {
            Write-ErrorAndExit "Unable to create main branch: $($createMain.StdErr)"
        }
    }

    Write-Step 'Switching to main'
    $checkout = Run-Git @('checkout','main')
    if ($checkout.ExitCode -ne 0) {
        Write-ErrorAndExit "git checkout main failed: $($checkout.StdErr)"
    }
    Write-Host $checkout.StdOut
}

# Configure origin remote if missing.
$expectedRemote = "https://github.com/$GitHubUser/$GitHubRepo.git"
Write-Step 'Verifying origin remote'
$remoteUrlResult = Run-Git @('remote','get-url','origin')
if ($remoteUrlResult.ExitCode -ne 0) {
    Write-Step 'No origin remote found; adding origin.'
    $addRemote = Run-Git @('remote','add','origin',$expectedRemote)
    if ($addRemote.ExitCode -ne 0) {
        Write-ErrorAndExit "Unable to add origin remote: $($addRemote.StdErr)"
    }
} else {
    $currentRemote = Normalize-GitHubUrl($remoteUrlResult.StdOut)
    $expectedNormalized = Normalize-GitHubUrl($expectedRemote)
    if ($currentRemote -ne $expectedNormalized) {
        Write-Warn "Existing origin remote does not match expected GitHub URL."
        Write-Host "  Current:  $currentRemote"
        Write-Host "  Expected: $expectedNormalized"
        Write-Host 'Please verify that origin is correct before pushing. If the repository should use a different remote, update origin manually.'
        exit 1
    }
}

Write-Step 'Listing configured remotes'
$remoteList = Run-Git @('remote','-v')
if ($remoteList.ExitCode -ne 0) {
    Write-ErrorAndExit "git remote -v failed: $($remoteList.StdErr)"
}
Write-Host $remoteList.StdOut

# Stage all tracked and untracked changes.
Write-Step 'Staging all changes (respecting .gitignore)'
$addResult = Run-Git @('add','-A')
if ($addResult.ExitCode -ne 0) {
    Write-ErrorAndExit "git add failed: $($addResult.StdErr)"
}

$stagedFiles = Run-Git @('diff','--cached','--name-only')
if ($stagedFiles.ExitCode -ne 0) {
    Write-ErrorAndExit "git diff --cached failed: $($stagedFiles.StdErr)"
}

if (-not [string]::IsNullOrWhiteSpace($stagedFiles.StdOut)) {
    $commitMessage = 'Initial clean commit'
    Write-Step "Committing staged changes with message: '$commitMessage'"
    $commit = Run-Git @('commit','-m',$commitMessage)
    if ($commit.ExitCode -ne 0) {
        Write-ErrorAndExit "git commit failed: $($commit.StdErr)"
    }
    Write-Host $commit.StdOut
} else {
    Write-Step 'No staged changes detected; skipping commit.'
}

# Push changes to GitHub with safety guardrails.
Guard-SensitiveFiles
Write-Step 'Pushing main to origin'
$upstreamResult = Run-Git @('rev-parse','--abbrev-ref','--symbolic-full-name','@{u}')
$pushArgs = @('push','origin','main')
if ($upstreamResult.ExitCode -ne 0) {
    Write-Step 'No upstream configured for main; pushing with -u to set upstream'
    $pushArgs = @('push','-u','origin','main')
}

$pushResult = Run-Git $pushArgs
if ($pushResult.ExitCode -ne 0) {
    Write-Warn 'Push failed. Checking for common failure reasons.'
    Write-Host $pushResult.StdErr

    if ($pushResult.StdErr -match 'pre-receive hook declined|remote rejected|protected branch|permission denied|failed to push') {
        Write-ErrorAndExit 'Push rejected by remote rules or secrets policy. Review GitHub branch protection and repository secrets rules before retrying.'
    }

    Write-ErrorAndExit "git push failed: $($pushResult.StdErr)"
}
Write-Host $pushResult.StdOut

# Verification after push.
Write-Step 'Verifying local branch tracking and synchronization'
$branchStatus = Run-Git @('branch','-vv')
if ($branchStatus.ExitCode -ne 0) {
    Write-ErrorAndExit "git branch -vv failed: $($branchStatus.StdErr)"
}
Write-Host $branchStatus.StdOut

Write-Step 'Fetching latest state from origin'
$fetchResult = Run-Git @('fetch','origin')
if ($fetchResult.ExitCode -ne 0) {
    Write-ErrorAndExit "git fetch origin failed: $($fetchResult.StdErr)"
}

$localHead = Run-Git @('rev-parse','HEAD')
$remoteHead = Run-Git @('rev-parse','origin/main')
if ($localHead.ExitCode -ne 0 -or $remoteHead.ExitCode -ne 0) {
    Write-ErrorAndExit 'Unable to compare local and origin/main heads.'
}

if ($localHead.StdOut -eq $remoteHead.StdOut) {
    Write-Host '[SUCCESS] Local main is in sync with origin/main.'
} else {
    Write-Warn 'Local main and origin/main are not in sync.'
    Write-Host "  Local HEAD:  $($localHead.StdOut)"
    Write-Host "  Origin HEAD: $($remoteHead.StdOut)"
    Write-Host 'Use git status and git log --oneline origin/main..main to inspect differences.'
}

Write-Host
Write-Host '=== Manual GitHub verification instructions ==='
Write-Host '1. Open the repository page in your browser:'
Write-Host "   https://github.com/$GitHubUser/$GitHubRepo"
Write-Host '2. Confirm the latest commit hash matches local output:'
Write-Host '   git log -1 --oneline'
Write-Host '3. Confirm the repository files are present as expected.'
Write-Host '4. Confirm no sensitive files or secrets are visible in the repo tree.'
Write-Host
Write-Host 'A successful push on GitHub looks like:'
Write-Host '  - The branch name is main.'
Write-Host '  - The latest commit appears at the top of the repo page.'
Write-Host '  - The commit message is correct.'
Write-Host '  - The file list matches the local repository contents.'

Write-Host 'If you enabled branch protection, verify the push completed without remote rejection.'
Write-Host 'If there are still concerns, inspect the GitHub Actions / Pull Requests page for any repository rules failures.'
