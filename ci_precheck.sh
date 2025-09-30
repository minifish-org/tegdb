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

run_step_verbose() {
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

echo -e "${YELLOW}🔧 Auto-fixing issues first...${NC}"

# Auto-fix issues first (these don't break CI, just improve code)
echo -e "${GREEN}==>${NC} Auto-fixing formatting"
cargo fmt --all >/dev/null 2>&1 && echo -e "${GREEN}✓${NC} Formatting fixed" || echo -e "${YELLOW}⚠${NC} No formatting changes needed"

echo -e "${GREEN}==>${NC} Auto-fixing clippy suggestions"
cargo clippy --all-targets --all-features --fix --allow-dirty --allow-staged >/dev/null 2>&1 && echo -e "${GREEN}✓${NC} Clippy suggestions fixed" || echo -e "${YELLOW}⚠${NC} No clippy fixes needed"

echo -e "\n${YELLOW}🔍 Verifying CI-critical checks (will fail fast)...${NC}"

# Critical checks that would break CI - fail immediately
run_step "Verifying formatting is clean" cargo fmt --all -- --check
run_step "Verifying clippy is clean" cargo clippy --all-targets --all-features -- -D warnings
run_step "Building with all features" cargo build --all-features
run_step "Building documentation" cargo doc --no-deps --document-private-items
run_step "Running doc tests" cargo test --doc

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
    run_step "Running example: ${example}" cargo run --example "${example}"
done

echo -e "\n${GREEN}🎉 All pre-push checks completed successfully!${NC}"
echo -e "${GREEN}✅ Ready to push - CI will pass${NC}"
