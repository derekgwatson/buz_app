#!/bin/bash
# Dev script for buz_app
#
# Default: git fetch/pull current branch
# Deploy: merge current branch to main, push, cleanup
#
# Usage:
#   ./dev.sh              # Pull latest on current branch
#   ./dev.sh -d           # Deploy: merge to main, push, delete branch
#   ./dev.sh --deploy     # Same as -d
#   ./dev.sh -h           # Show help

set -e

# Colors
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
CYAN='\033[0;36m'
GRAY='\033[0;90m'
NC='\033[0m' # No Color

SCRIPT_DIR=$(dirname "$(realpath "$0")")
cd "$SCRIPT_DIR"

# ============================================================================
# Help
# ============================================================================

show_help() {
    echo ""
    echo -e "${CYAN}DEV.SH - Buz App Dev Script${NC}"
    echo -e "${CYAN}===========================${NC}"
    echo ""
    echo -e "${GRAY}Manage git branches for local development.${NC}"
    echo ""
    echo -e "${YELLOW}USAGE:${NC}"
    echo "  ./dev.sh              Pull latest on current branch"
    echo "  ./dev.sh -d           Deploy: merge to main, push, delete branch"
    echo "  ./dev.sh --deploy     Same as -d"
    echo "  ./dev.sh -h, --help   Show this help"
    echo ""
    echo -e "${YELLOW}DEPLOY WORKFLOW:${NC}"
    echo "  1. Fetches latest from origin"
    echo "  2. If on feature branch: merge to main, push, delete branch"
    echo "  3. If on main: select a remote feature branch to merge"
    echo ""
    echo -e "${YELLOW}AFTER DEPLOY:${NC}"
    echo "  Run on prod server:  ./deploy.sh"
    echo ""
    exit 0
}

# ============================================================================
# Parse args
# ============================================================================

DEPLOY=false

while [[ $# -gt 0 ]]; do
    case $1 in
        -d|--deploy)
            DEPLOY=true
            shift
            ;;
        -h|--help)
            show_help
            ;;
        *)
            echo -e "${RED}Unknown option: $1${NC}"
            echo "Use -h for help"
            exit 1
            ;;
    esac
done

# ============================================================================
# Deploy mode
# ============================================================================

if $DEPLOY; then
    echo ""
    echo -e "${CYAN}========================================${NC}"
    echo -e "${CYAN}  DEPLOY TO MAIN${NC}"
    echo -e "${CYAN}========================================${NC}"
    echo ""

    # Fetch latest
    echo -e "${CYAN}Fetching latest from origin...${NC}"
    git fetch origin --prune

    # Get current branch
    current_branch=$(git branch --show-current)
    feature_branch=""
    is_remote_branch=false

    if [[ "$current_branch" == "main" ]]; then
        # On main - find remote branches to merge
        mapfile -t remote_branches < <(git branch -r | grep -v 'HEAD' | grep -v 'origin/main$' | sed 's/^[[:space:]]*//')

        if [[ ${#remote_branches[@]} -eq 0 ]]; then
            echo -e "${YELLOW}No feature branches found on remote.${NC}"
            echo -e "${GRAY}Nothing to deploy.${NC}"
            exit 0
        fi

        if [[ ${#remote_branches[@]} -eq 1 ]]; then
            feature_branch="${remote_branches[0]}"
            echo -e "${CYAN}Found 1 feature branch: ${YELLOW}$feature_branch${NC}"
        else
            echo -e "${CYAN}Remote feature branches:${NC}"
            echo ""
            for i in "${!remote_branches[@]}"; do
                branch="${remote_branches[$i]}"
                commit_info=$(git log --oneline -1 "$branch" 2>/dev/null || echo "")
                commit_date=$(git log -1 --format='%cr' "$branch" 2>/dev/null || echo "")
                echo -e "  $((i+1))) ${YELLOW}$branch${NC}"
                echo -e "     ${GRAY}$commit_date - $commit_info${NC}"
                echo ""
            done
            echo -e "  0) Cancel"
            echo ""

            while true; do
                read -rp "Select branch to merge [1-${#remote_branches[@]}, or 0 to cancel]: " selection
                if [[ "$selection" == "0" || -z "$selection" ]]; then
                    echo -e "${YELLOW}Cancelled.${NC}"
                    exit 0
                fi
                if [[ "$selection" =~ ^[0-9]+$ ]] && [[ "$selection" -ge 1 ]] && [[ "$selection" -le ${#remote_branches[@]} ]]; then
                    feature_branch="${remote_branches[$((selection-1))]}"
                    break
                fi
                echo -e "${RED}Invalid selection. Try again.${NC}"
            done
        fi
        is_remote_branch=true
        echo ""
    else
        feature_branch="$current_branch"
        echo -e "${CYAN}Feature branch: ${YELLOW}$feature_branch${NC}"
        echo ""
    fi

    # Show recent commits
    echo -e "${CYAN}Recent commits to deploy:${NC}"
    git log --oneline -5 "$feature_branch" 2>/dev/null | while read -r line; do
        echo -e "  ${GRAY}$line${NC}"
    done
    echo ""

    # Checkout main if not already there
    if [[ "$current_branch" != "main" ]]; then
        echo -e "${CYAN}Switching to main...${NC}"
        git checkout main
    fi

    echo -e "${CYAN}Pulling latest main...${NC}"
    git pull origin main

    # Merge feature branch
    echo ""
    display_branch="${feature_branch#origin/}"
    echo -e "${CYAN}Merging $display_branch into main...${NC}"
    if ! git merge "$feature_branch" --no-edit; then
        echo -e "${RED}Merge failed!${NC}"
        echo ""
        echo -e "${YELLOW}Resolve conflicts manually, then run:${NC}"
        echo -e "${GRAY}  git add . && git commit${NC}"
        echo -e "${GRAY}  git push origin main${NC}"
        echo -e "${GRAY}  git push origin --delete $display_branch${NC}"
        exit 1
    fi
    echo -e "${GREEN}Merge successful${NC}"

    # Push main
    echo ""
    echo -e "${CYAN}Pushing main to origin...${NC}"
    if ! git push origin main; then
        echo -e "${RED}Push failed${NC}"
        exit 1
    fi
    echo -e "${GREEN}Push successful${NC}"

    # Delete local feature branch (if it exists locally and we weren't on main)
    echo ""
    if ! $is_remote_branch; then
        echo -e "${CYAN}Deleting local branch: $feature_branch${NC}"
        if git branch -d "$feature_branch" 2>/dev/null; then
            echo -e "${GREEN}Deleted local branch${NC}"
        else
            echo -e "${YELLOW}Could not delete local branch${NC}"
        fi
    fi

    # Delete remote feature branch
    echo -e "${CYAN}Deleting remote branch: origin/$display_branch${NC}"
    if git push origin --delete "$display_branch" 2>/dev/null; then
        echo -e "${GREEN}Deleted remote branch${NC}"
    else
        echo -e "${YELLOW}Could not delete remote branch (may already be deleted)${NC}"
    fi

    echo ""
    echo -e "${CYAN}========================================${NC}"
    echo -e "${GREEN}  DEPLOY COMPLETE${NC}"
    echo -e "${CYAN}========================================${NC}"
    echo ""
    echo -e "${CYAN}Now run on prod:${NC}"
    echo "  ./deploy.sh"
    echo ""
    exit 0
fi

# ============================================================================
# Default: fetch/pull
# ============================================================================

echo ""
echo -e "${CYAN}========================================${NC}"
echo -e "${CYAN}  GIT PULL${NC}"
echo -e "${CYAN}========================================${NC}"
echo ""

# Show current branch
current_branch=$(git branch --show-current)
echo -e "${CYAN}Current branch: ${YELLOW}$current_branch${NC}"
echo ""

# Fetch and prune
echo -e "${CYAN}Fetching from origin (with prune)...${NC}"
git fetch origin --prune

# If on a feature branch that no longer exists on remote, switch to main
if [[ "$current_branch" != "main" ]]; then
    if ! git branch -r --list "origin/$current_branch" | grep -q .; then
        echo ""
        echo -e "${YELLOW}Branch '$current_branch' no longer exists on remote (already deployed?).${NC}"
        echo -e "${CYAN}Switching to main...${NC}"
        git checkout main
        git branch -D "$current_branch" 2>/dev/null && echo -e "${GREEN}Deleted stale branch: $current_branch${NC}"
        current_branch="main"
        echo ""
    fi
fi

# If on main, check for feature branches
if [[ "$current_branch" == "main" ]]; then
    mapfile -t feature_branches < <(git branch -r | grep -v 'HEAD' | grep -v 'origin/main$' | sed 's/^[[:space:]]*//')

    if [[ ${#feature_branches[@]} -gt 0 ]]; then
        echo ""
        echo -e "${YELLOW}Feature branches available:${NC}"
        echo ""

        if [[ ${#feature_branches[@]} -eq 1 ]]; then
            branch="${feature_branches[0]}"
            local_name="${branch#origin/}"
            commit_info=$(git log --oneline -1 "$branch" 2>/dev/null || echo "")
            commit_date=$(git log -1 --format='%cr' "$branch" 2>/dev/null || echo "")
            echo -e "  ${YELLOW}$branch${NC}"
            echo -e "  ${GRAY}$commit_date - $commit_info${NC}"
            echo ""

            read -rp "Switch to this branch? [Y/n] " switch
            if [[ -z "$switch" || "$switch" =~ ^[Yy] ]]; then
                echo ""
                echo -e "${CYAN}Switching to $local_name...${NC}"
                if git branch --list "$local_name" | grep -q .; then
                    git checkout "$local_name"
                else
                    git checkout -b "$local_name" --track "$branch"
                fi
                current_branch="$local_name"
                echo -e "${GREEN}Switched to $current_branch${NC}"
            fi
        else
            for i in "${!feature_branches[@]}"; do
                branch="${feature_branches[$i]}"
                commit_info=$(git log --oneline -1 "$branch" 2>/dev/null || echo "")
                commit_date=$(git log -1 --format='%cr' "$branch" 2>/dev/null || echo "")
                echo -e "  $((i+1))) ${YELLOW}$branch${NC}"
                echo -e "     ${GRAY}$commit_date - $commit_info${NC}"
            done
            echo ""
            echo -e "  0) Stay on main"
            echo ""

            read -rp "Switch to branch? [0-${#feature_branches[@]}, default: 0] " selection
            if [[ -n "$selection" && "$selection" != "0" ]]; then
                if [[ "$selection" =~ ^[0-9]+$ ]] && [[ "$selection" -ge 1 ]] && [[ "$selection" -le ${#feature_branches[@]} ]]; then
                    branch="${feature_branches[$((selection-1))]}"
                    local_name="${branch#origin/}"
                    echo ""
                    echo -e "${CYAN}Switching to $local_name...${NC}"
                    if git branch --list "$local_name" | grep -q .; then
                        git checkout "$local_name"
                    else
                        git checkout -b "$local_name" --track "$branch"
                    fi
                    current_branch="$local_name"
                    echo -e "${GREEN}Switched to $current_branch${NC}"
                fi
            fi
        fi
        echo ""
    fi
fi

# Show remote branches
echo ""
echo -e "${CYAN}Remote branches:${NC}"
git branch -r | grep -v 'HEAD' | while read -r branch; do
    branch=$(echo "$branch" | sed 's/^[[:space:]]*//')
    if [[ "$branch" == "origin/$current_branch" ]]; then
        echo -e "  ${GREEN}$branch${NC} ${GRAY}(current)${NC}"
    elif [[ "$branch" == "origin/main" ]]; then
        echo -e "  $branch"
    else
        echo -e "  ${GRAY}$branch${NC}"
    fi
done

# Check for stale local branches
mapfile -t local_branches < <(git branch | sed 's/^[* ]*//')
stale_branches=()
for branch in "${local_branches[@]}"; do
    if [[ "$branch" != "main" && "$branch" != "$current_branch" ]]; then
        if ! git branch -r --list "origin/$branch" | grep -q .; then
            stale_branches+=("$branch")
        fi
    fi
done

if [[ ${#stale_branches[@]} -gt 0 ]]; then
    echo ""
    echo -e "${YELLOW}Stale local branches (no remote):${NC}"
    for branch in "${stale_branches[@]}"; do
        echo -e "  ${YELLOW}$branch${NC}"
    done
    echo ""
    read -rp "Delete these stale branches? [y/N] " cleanup
    if [[ "$cleanup" =~ ^[Yy] ]]; then
        for branch in "${stale_branches[@]}"; do
            if git branch -D "$branch" 2>/dev/null; then
                echo -e "  ${GREEN}Deleted: $branch${NC}"
            else
                echo -e "  ${RED}Failed to delete: $branch${NC}"
            fi
        done
    fi
fi

echo ""

# Pull current branch
echo -e "${CYAN}Pulling $current_branch...${NC}"
if git pull; then
    echo -e "${GREEN}Pull successful${NC}"
else
    echo -e "${RED}Git pull failed${NC}"
    echo -e "${YELLOW}Continuing anyway...${NC}"
fi

echo ""
echo -e "${CYAN}========================================${NC}"
echo -e "${GREEN}  DONE${NC}"
echo -e "${CYAN}========================================${NC}"
echo ""
