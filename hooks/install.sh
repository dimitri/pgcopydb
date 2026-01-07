#!/bin/bash
#
# Install git hooks for pgcopydb development
#

HOOKS_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
GIT_HOOKS_DIR="$(git rev-parse --git-dir)/hooks"

echo "Installing git hooks to $GIT_HOOKS_DIR"

# Install pre-commit hook
cp "$HOOKS_DIR/pre-commit" "$GIT_HOOKS_DIR/pre-commit"
chmod +x "$GIT_HOOKS_DIR/pre-commit"

echo "✓ Git hooks installed successfully"
echo ""
echo "The following hooks are now active:"
echo "  - pre-commit: Checks for banned APIs and code style issues"
echo ""
echo "To bypass hooks temporarily, use: git commit --no-verify"
