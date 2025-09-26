#!/usr/bin/env bash

# TegDB local CI parity script
#
# Runs the critical checks that GitHub Actions enforces so you can
# catch failures before pushing changes.

set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$REPO_ROOT"

run_step() {
    local description="$1"
    shift
    echo "==> ${description}"
    "$@"
    echo "   ${description} completed"
}

run_step "Auto-fixing formatting" cargo fmt --all
run_step "Auto-fixing clippy suggestions (all targets, all features)" \
    cargo clippy --all-targets --all-features --fix --allow-dirty --allow-staged

# Verify fixes were successful (what CI will check)
run_step "Verifying formatting is clean" cargo fmt --all -- --check
run_step "Verifying clippy is clean" \
    cargo clippy --all-targets --all-features -- -D warnings
run_step "Building documentation" cargo doc --no-deps --document-private-items
run_step "Building with all features" cargo build --all-features
run_step "Running full test suite" ./run_all_tests.sh --ci
run_step "Running doc tests" cargo test --doc

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

echo "All pre-push checks completed successfully."
