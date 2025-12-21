# Dev script for buz_app
#
# Default: git fetch/pull current branch
# Deploy: merge current branch to main, push, cleanup
#
# Usage:
#   .\dev              # Pull latest on current branch
#   .\dev -Deploy      # Merge to main, push, delete branch
#   .\dev -Help        # Show help

param(
    [switch]$Help,
    [switch]$Deploy
)

$baseDir = $PSScriptRoot

# ============================================================================
# Help
# ============================================================================

if ($Help) {
    Write-Host ""
    Write-Host "DEV.PS1 - Buz App Dev Script" -ForegroundColor Cyan
    Write-Host "=============================" -ForegroundColor Cyan
    Write-Host ""
    Write-Host "Manage git branches for local development." -ForegroundColor Gray
    Write-Host ""
    Write-Host "USAGE:" -ForegroundColor Yellow
    Write-Host "  .\dev              Pull latest on current branch"
    Write-Host "  .\dev -Deploy      Merge to main, push, delete branch"
    Write-Host "  .\dev -Help        Show this help"
    Write-Host ""
    Write-Host "DEPLOY WORKFLOW:" -ForegroundColor Yellow
    Write-Host "  1. Fetches latest from origin"
    Write-Host "  2. If on feature branch: merge to main, push, delete branch"
    Write-Host "  3. If on main: select a remote feature branch to merge"
    Write-Host ""
    Write-Host "AFTER DEPLOY:" -ForegroundColor Yellow
    Write-Host "  Run on prod server:  ./deploy.sh"
    Write-Host ""
    exit 0
}

# ============================================================================
# Deploy mode
# ============================================================================

if ($Deploy) {
    Write-Host ""
    Write-Host "========================================" -ForegroundColor Cyan
    Write-Host "  DEPLOY TO MAIN" -ForegroundColor Cyan
    Write-Host "========================================" -ForegroundColor Cyan
    Write-Host ""

    Push-Location $baseDir
    try {
        # Fetch latest
        Write-Host "Fetching latest from origin..." -ForegroundColor Cyan
        git fetch origin --prune 2>&1 | Out-Null

        # Get current branch
        $currentBranch = git branch --show-current 2>&1
        $featureBranch = $null
        $isRemoteBranch = $false

        if ($currentBranch -eq 'main') {
            # On main - find remote branches to merge
            $remoteBranches = git branch -r 2>&1 | Where-Object {
                $_ -notmatch 'HEAD' -and $_ -notmatch 'origin/main$'
            } | ForEach-Object { $_.Trim() }

            if (-not $remoteBranches -or $remoteBranches.Count -eq 0) {
                Write-Host "No feature branches found on remote." -ForegroundColor Yellow
                Write-Host "Nothing to deploy." -ForegroundColor Gray
                exit 0
            }

            # Convert to array if single item
            if ($remoteBranches -is [string]) {
                $remoteBranches = @($remoteBranches)
            }

            if ($remoteBranches.Count -eq 1) {
                $featureBranch = $remoteBranches[0]
                Write-Host "Found 1 feature branch: " -NoNewline -ForegroundColor Cyan
                Write-Host $featureBranch -ForegroundColor Yellow
            } else {
                Write-Host "Remote feature branches:" -ForegroundColor Cyan
                Write-Host ""
                for ($i = 0; $i -lt $remoteBranches.Count; $i++) {
                    $branch = $remoteBranches[$i]
                    $commitInfo = git log --oneline -1 $branch 2>&1
                    $commitDate = git log -1 --format='%cr' $branch 2>&1
                    Write-Host "  $($i + 1)) " -NoNewline -ForegroundColor White
                    Write-Host $branch -ForegroundColor Yellow
                    Write-Host "     $commitDate - $commitInfo" -ForegroundColor Gray
                    Write-Host ""
                }
                Write-Host "  0) Cancel" -ForegroundColor DarkGray
                Write-Host ""

                do {
                    $selection = Read-Host "Select branch to merge [1-$($remoteBranches.Count), or 0 to cancel]"
                    if ($selection -eq '0' -or $selection -eq '') {
                        Write-Host "Cancelled." -ForegroundColor Yellow
                        exit 0
                    }
                    $selNum = 0
                    if ([int]::TryParse($selection, [ref]$selNum) -and $selNum -ge 1 -and $selNum -le $remoteBranches.Count) {
                        $featureBranch = $remoteBranches[$selNum - 1]
                        break
                    }
                    Write-Host "Invalid selection. Try again." -ForegroundColor Red
                } while ($true)
            }
            $isRemoteBranch = $true
            Write-Host ""
        } else {
            $featureBranch = $currentBranch
            Write-Host "Feature branch: " -NoNewline -ForegroundColor Cyan
            Write-Host $featureBranch -ForegroundColor Yellow
            Write-Host ""
        }

        # Show recent commits
        Write-Host "Recent commits to deploy:" -ForegroundColor Cyan
        git log --oneline -5 $featureBranch 2>&1 | ForEach-Object { Write-Host "  $_" -ForegroundColor Gray }
        Write-Host ""

        # Checkout main if not already there
        if ($currentBranch -ne 'main') {
            Write-Host "Switching to main..." -ForegroundColor Cyan
            git checkout main 2>&1 | Out-Null
            if ($LASTEXITCODE -ne 0) {
                Write-Host "Failed to checkout main" -ForegroundColor Red
                git checkout $currentBranch 2>&1 | Out-Null
                exit 1
            }
        }

        Write-Host "Pulling latest main..." -ForegroundColor Cyan
        $pullResult = git pull origin main 2>&1
        Write-Host "  $pullResult" -ForegroundColor Gray

        # Merge feature branch
        Write-Host ""
        $displayBranch = $featureBranch -replace '^origin/', ''
        Write-Host "Merging $displayBranch into main..." -ForegroundColor Cyan
        $mergeResult = git merge $featureBranch --no-edit 2>&1
        if ($LASTEXITCODE -ne 0) {
            Write-Host "Merge failed!" -ForegroundColor Red
            Write-Host "$mergeResult" -ForegroundColor Red
            Write-Host ""
            Write-Host "Resolve conflicts manually, then run:" -ForegroundColor Yellow
            Write-Host "  git add . && git commit" -ForegroundColor Gray
            Write-Host "  git push origin main" -ForegroundColor Gray
            Write-Host "  git push origin --delete $displayBranch" -ForegroundColor Gray
            exit 1
        }
        Write-Host "Merge successful" -ForegroundColor Green

        # Push main
        Write-Host ""
        Write-Host "Pushing main to origin..." -ForegroundColor Cyan
        $pushResult = git push origin main 2>&1
        if ($LASTEXITCODE -ne 0) {
            Write-Host "Push failed: $pushResult" -ForegroundColor Red
            exit 1
        }
        Write-Host "Push successful" -ForegroundColor Green

        # Delete local feature branch
        Write-Host ""
        if (-not $isRemoteBranch) {
            Write-Host "Deleting local branch: $featureBranch" -ForegroundColor Cyan
            $deleteLocal = git branch -d $featureBranch 2>&1
            if ($LASTEXITCODE -eq 0) {
                Write-Host "Deleted local branch" -ForegroundColor Green
            } else {
                Write-Host "Could not delete local branch: $deleteLocal" -ForegroundColor Yellow
            }
        }

        # Delete remote feature branch
        Write-Host "Deleting remote branch: origin/$displayBranch" -ForegroundColor Cyan
        $deleteRemote = git push origin --delete $displayBranch 2>&1
        if ($LASTEXITCODE -eq 0) {
            Write-Host "Deleted remote branch" -ForegroundColor Green
        } else {
            Write-Host "Could not delete remote branch (may already be deleted)" -ForegroundColor Yellow
        }

        Write-Host ""
        Write-Host "========================================" -ForegroundColor Cyan
        Write-Host "  DEPLOY COMPLETE" -ForegroundColor Green
        Write-Host "========================================" -ForegroundColor Cyan
        Write-Host ""
        Write-Host "Now run on prod:" -ForegroundColor Cyan
        Write-Host "  ./deploy.sh" -ForegroundColor White
        Write-Host ""

    } finally {
        Pop-Location
    }

    exit 0
}

# ============================================================================
# Default: fetch/pull
# ============================================================================

Write-Host ""
Write-Host "========================================" -ForegroundColor Cyan
Write-Host "  GIT PULL" -ForegroundColor Cyan
Write-Host "========================================" -ForegroundColor Cyan
Write-Host ""

Push-Location $baseDir
try {
    # Show current branch
    $currentBranch = git branch --show-current 2>&1
    Write-Host "Current branch: " -NoNewline -ForegroundColor Cyan
    Write-Host $currentBranch -ForegroundColor Yellow
    Write-Host ""

    # Fetch and prune
    Write-Host "Fetching from origin (with prune)..." -ForegroundColor Cyan
    $fetchResult = git fetch origin --prune 2>&1
    if ($LASTEXITCODE -ne 0) {
        Write-Host "Fetch failed: $fetchResult" -ForegroundColor Red
    }

    # If on a feature branch that no longer exists on remote, switch to main
    if ($currentBranch -ne 'main') {
        $remoteExists = git branch -r --list "origin/$currentBranch" 2>&1
        if (-not $remoteExists) {
            Write-Host ""
            Write-Host "Branch '$currentBranch' no longer exists on remote (already deployed?)." -ForegroundColor Yellow
            Write-Host "Switching to main..." -ForegroundColor Cyan
            git checkout main 2>&1 | Out-Null
            if ($LASTEXITCODE -eq 0) {
                $deleteResult = git branch -D $currentBranch 2>&1
                if ($LASTEXITCODE -eq 0) {
                    Write-Host "Switched to main, deleted stale branch: $currentBranch" -ForegroundColor Green
                } else {
                    Write-Host "Switched to main (could not delete $currentBranch)" -ForegroundColor Yellow
                }
                $currentBranch = 'main'
            } else {
                Write-Host "Failed to switch to main, staying on $currentBranch" -ForegroundColor Red
            }
            Write-Host ""
        }
    }

    # If on main, check for feature branches
    if ($currentBranch -eq 'main') {
        $featureBranches = git branch -r 2>&1 | Where-Object {
            $_ -notmatch 'HEAD' -and $_ -notmatch 'origin/main$'
        } | ForEach-Object { $_.Trim() }

        if ($featureBranches) {
            if ($featureBranches -is [string]) {
                $featureBranches = @($featureBranches)
            }

            Write-Host ""
            Write-Host "Feature branches available:" -ForegroundColor Yellow

            if ($featureBranches.Count -eq 1) {
                $branch = $featureBranches[0]
                $localName = $branch -replace '^origin/', ''
                $commitInfo = git log --oneline -1 $branch 2>&1
                $commitDate = git log -1 --format='%cr' $branch 2>&1
                Write-Host "  $branch" -ForegroundColor Yellow
                Write-Host "  $commitDate - $commitInfo" -ForegroundColor Gray
                Write-Host ""

                $switch = Read-Host "Switch to this branch? [Y/n]"
                if ($switch -eq '' -or $switch -match '^[Yy]') {
                    Write-Host ""
                    Write-Host "Switching to $localName..." -ForegroundColor Cyan
                    $localExists = git branch --list $localName 2>&1
                    if ($localExists) {
                        git checkout $localName 2>&1 | Out-Null
                    } else {
                        git checkout -b $localName --track $branch 2>&1 | Out-Null
                    }
                    $currentBranch = $localName
                    Write-Host "Switched to $currentBranch" -ForegroundColor Green
                }
            } else {
                Write-Host ""
                for ($i = 0; $i -lt $featureBranches.Count; $i++) {
                    $branch = $featureBranches[$i]
                    $commitInfo = git log --oneline -1 $branch 2>&1
                    $commitDate = git log -1 --format='%cr' $branch 2>&1
                    Write-Host "  $($i + 1)) " -NoNewline -ForegroundColor White
                    Write-Host $branch -ForegroundColor Yellow
                    Write-Host "     $commitDate - $commitInfo" -ForegroundColor Gray
                }
                Write-Host ""
                Write-Host "  0) Stay on main" -ForegroundColor DarkGray
                Write-Host ""

                $selection = Read-Host "Switch to branch? [0-$($featureBranches.Count), default: 0]"
                if ($selection -ne '' -and $selection -ne '0') {
                    $selNum = 0
                    if ([int]::TryParse($selection, [ref]$selNum) -and $selNum -ge 1 -and $selNum -le $featureBranches.Count) {
                        $branch = $featureBranches[$selNum - 1]
                        $localName = $branch -replace '^origin/', ''
                        Write-Host ""
                        Write-Host "Switching to $localName..." -ForegroundColor Cyan
                        $localExists = git branch --list $localName 2>&1
                        if ($localExists) {
                            git checkout $localName 2>&1 | Out-Null
                        } else {
                            git checkout -b $localName --track $branch 2>&1 | Out-Null
                        }
                        $currentBranch = $localName
                        Write-Host "Switched to $currentBranch" -ForegroundColor Green
                    }
                }
            }
            Write-Host ""
        }
    }

    # Show remote branches
    Write-Host ""
    Write-Host "Remote branches:" -ForegroundColor Cyan
    $remoteBranches = git branch -r 2>&1 | Where-Object { $_ -notmatch 'HEAD' }
    foreach ($branch in $remoteBranches) {
        $branchName = $branch.Trim()
        if ($branchName -match "origin/$currentBranch$") {
            Write-Host "  $branchName" -ForegroundColor Green -NoNewline
            Write-Host " (current)" -ForegroundColor Gray
        } elseif ($branchName -match 'origin/main$') {
            Write-Host "  $branchName" -ForegroundColor White
        } else {
            Write-Host "  $branchName" -ForegroundColor DarkGray
        }
    }

    # Check for stale local branches
    $localBranches = git branch 2>&1
    $staleBranches = @()
    foreach ($branch in $localBranches) {
        $branchName = $branch.Trim().TrimStart('* ')
        if ($branchName -ne 'main' -and $branchName -ne $currentBranch) {
            $remoteExists = git branch -r --list "origin/$branchName" 2>&1
            if (-not $remoteExists) {
                $staleBranches += $branchName
            }
        }
    }

    if ($staleBranches.Count -gt 0) {
        Write-Host ""
        Write-Host "Stale local branches (no remote):" -ForegroundColor Yellow
        foreach ($branch in $staleBranches) {
            Write-Host "  $branch" -ForegroundColor DarkYellow
        }
        Write-Host ""
        $cleanup = Read-Host "Delete these stale branches? [y/N]"
        if ($cleanup -match '^[Yy]') {
            foreach ($branch in $staleBranches) {
                $deleteResult = git branch -D $branch 2>&1
                if ($LASTEXITCODE -eq 0) {
                    Write-Host "  Deleted: $branch" -ForegroundColor Green
                } else {
                    Write-Host "  Failed to delete $branch" -ForegroundColor Red
                }
            }
        }
    }

    Write-Host ""

    # Pull current branch
    Write-Host "Pulling $currentBranch..." -ForegroundColor Cyan
    $gitResult = git pull 2>&1
    if ($LASTEXITCODE -eq 0) {
        Write-Host "  $gitResult" -ForegroundColor Green
    } else {
        Write-Host "  Git pull failed: $gitResult" -ForegroundColor Red
        Write-Host "  Continuing anyway..." -ForegroundColor Yellow
    }

} finally {
    Pop-Location
}

Write-Host ""
Write-Host "========================================" -ForegroundColor Cyan
Write-Host "  DONE" -ForegroundColor Green
Write-Host "========================================" -ForegroundColor Cyan
Write-Host ""
