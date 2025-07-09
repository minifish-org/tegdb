#!/bin/bash

# TegDB Code Quality Fixer
# Automatically fixes formatting and linting issues

set -e

echo "🚀 TegDB Code Quality Fixer"
echo "=========================="
echo ""

# Store the original directory
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

# Run format fixer
echo "📝 Step 1: Fixing code formatting..."
if ./fix_format.sh; then
    echo "✅ Formatting fixed successfully!"
else
    echo "❌ Formatting fix failed!"
    exit 1
fi

echo ""

# Run lint fixer
echo "🔍 Step 2: Fixing clippy linting issues..."
if ./fix_lint.sh; then
    echo "✅ Linting fixed successfully!"
else
    echo "❌ Linting fix failed!"
    exit 1
fi

echo ""
echo "🎉 All code quality fixes completed successfully!"
echo ""
echo "💡 Next steps:"
echo "   - Review the changes with 'git diff'"
echo "   - Run tests with './run_all_tests.sh'"
echo "   - Commit your changes" 