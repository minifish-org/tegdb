#!/bin/bash

# TegDB Format Fixer
# Automatically fixes code formatting issues using cargo fmt

set -e

echo "🔧 Fixing code formatting with cargo fmt..."

# Check if cargo fmt is available
if ! command -v cargo &> /dev/null; then
    echo "❌ Error: cargo is not installed or not in PATH"
    exit 1
fi

# Run cargo fmt to fix formatting
echo "📝 Running cargo fmt --all..."
cargo fmt --all

echo "✅ Code formatting fixed successfully!"
echo ""
echo "💡 Tip: You can also run 'cargo fmt --all -- --check' to check formatting without fixing it." 