#!/usr/bin/env bash

# TegDB local CI parity script
#
# Runs the critical checks that GitHub Actions enforces so you can
# catch failures before pushing changes.
# - Auto-fixes issues first when possible
# - Minimizes output for cleaner logs
# - Stops immediately when CI would break

set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$REPO_ROOT"

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

# Use +stable to match CI exactly (CI uses 'stable' toolchain)
# This ensures we catch the same errors CI would catch, even if your default toolchain differs
# The +stable syntax will automatically install stable if not available
CARGO_CMD="cargo +stable"

run_step() {
    local description="$1"
    shift
    echo -e "${GREEN}==>${NC} ${description}"
    if "$@" >/dev/null 2>&1; then
        echo -e "${GREEN}✓${NC} ${description} completed"
    else
        echo -e "${RED}✗${NC} ${description} failed - CI would break"
        exit 1
    fi
}

run_step_show_warnings() {
    local description="$1"
    shift
    echo -e "${GREEN}==>${NC} ${description}"
    local output
    if output=$("$@" 2>&1); then
        echo -e "${GREEN}✓${NC} ${description} completed"
    else
        echo -e "${RED}✗${NC} ${description} failed - CI would break"
        echo "$output" | grep -E "(warning|error)" || echo "$output"
        exit 1
    fi
}


echo -e "${YELLOW}🔧 Auto-fixing issues first...${NC}"

# Auto-fix issues first (these don't break CI, just improve code)
echo -e "${GREEN}==>${NC} Auto-fixing formatting"
${CARGO_CMD} fmt --all 2>&1 | grep -E "(Diff at|Formatting|reformatted)" || echo -e "${GREEN}✓${NC} No formatting changes needed"

echo -e "${GREEN}==>${NC} Auto-fixing clippy suggestions"
# Run clippy fix, but don't fail if it can't fix everything (some errors can't be auto-fixed)
if output=$(${CARGO_CMD} clippy --all-targets --all-features --fix --allow-dirty --allow-staged 2>&1); then
    if echo "$output" | grep -qE "(Fixed|warning|error)"; then
        echo "$output" | grep -E "(Fixed|warning|error)" | head -20
        echo -e "${GREEN}✓${NC} Clippy auto-fixes applied (see output above for details)"
    else
        echo -e "${GREEN}✓${NC} No clippy auto-fixes needed"
    fi
else
    # Clippy fix failed - this might indicate errors that can't be auto-fixed
    # Show the errors but don't fail yet - the verification step will catch them
    echo "$output" | grep -E "(error|warning)" | head -20 || echo "$output" | tail -20
    echo -e "${YELLOW}⚠${NC}  Clippy auto-fix encountered issues (will be verified in next step)"
fi

echo -e "\n${YELLOW}🔍 Verifying CI-critical checks (will fail fast)...${NC}"

# Critical checks that would break CI - fail immediately
# Use +stable to match CI exactly
run_step "Verifying formatting is clean" ${CARGO_CMD} fmt --all -- --check
run_step_show_warnings "Verifying clippy is clean" ${CARGO_CMD} clippy --all-targets --all-features -- -D warnings
run_step "Building with all features" ${CARGO_CMD} build --all-features
run_step "Building documentation" ${CARGO_CMD} doc --no-deps --document-private-items
run_step "Running doc tests" ${CARGO_CMD} test --doc

echo -e "\n${YELLOW}🧪 Running comprehensive test suite...${NC}"
run_step "Running full test suite" ./run_all_tests.sh --ci

echo -e "\n${YELLOW}📚 Running key examples...${NC}"
examples=(
    simple_usage
    comprehensive_database_test
    streaming_api_demo
    arithmetic_expressions
    sqlite_like_usage
    iot_optimization_demo
    planner_demo
)

for example in "${examples[@]}"; do
    run_step "Running example: ${example}" ${CARGO_CMD} run --example "${example}"
done

echo -e "\n${GREEN}🎉 All pre-push checks completed successfully!${NC}"
echo -e "${GREEN}✅ Ready to push - CI will pass${NC}"
