#!/bin/bash

# TegDB Lint Fixer
# Automatically fixes clippy linting issues where possible

set -e

echo "🔧 Fixing clippy linting issues..."

# Check if cargo is available
if ! command -v cargo &> /dev/null; then
    echo "❌ Error: cargo is not installed or not in PATH"
    exit 1
fi

# Check if rustup is available for component installation
if ! command -v rustup &> /dev/null; then
    echo "❌ Error: rustup is not installed or not in PATH"
    exit 1
fi

# Ensure clippy is installed
echo "📦 Ensuring clippy is installed..."
rustup component add clippy

# Run clippy with auto-fix where possible
echo "🔍 Running cargo clippy --fix..."
if cargo clippy --all-targets --all-features --fix --allow-dirty --allow-staged; then
    echo "✅ Clippy auto-fixes applied successfully!"
else
    echo "⚠️  Some clippy issues could not be auto-fixed."
    echo "📋 Running clippy again to show remaining issues..."
    cargo clippy --all-targets --all-features -- -D warnings || {
        echo "❌ There are still clippy warnings/errors that need manual fixing."
        echo "💡 Run 'cargo clippy --all-targets --all-features' to see all issues."
        exit 1
    }
fi

echo ""
echo "💡 Tip: Run 'cargo clippy --all-targets --all-features -- -D warnings' to check for remaining issues." 