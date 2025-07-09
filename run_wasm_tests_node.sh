#!/bin/bash

# Script to run WASM tests for TegDB
# This runs tests in a browser environment

set -e

echo "=== Building WASM target ==="
cargo build --target wasm32-unknown-unknown --all-features

echo ""
echo "=== Running WASM tests ==="
echo "Note: This will run tests in a browser environment"
echo ""

# Check if wasm-bindgen-test-runner is available
if ! command -v wasm-bindgen-test-runner &> /dev/null; then
    echo "Installing wasm-bindgen-cli..."
    cargo install wasm-bindgen-cli
fi

# Find the WASM test files
WASM_TEST_FILES=$(find target/wasm32-unknown-unknown/debug/deps -name "*.wasm" -type f)

if [ -z "$WASM_TEST_FILES" ]; then
    echo "No WASM test files found. Make sure you've built the WASM target first."
    exit 1
fi

# Run each WASM test file
for wasm_file in $WASM_TEST_FILES; do
    echo "Running tests in: $(basename $wasm_file)"
    wasm-bindgen-test-runner "$wasm_file"
done

echo ""
echo "=== WASM tests completed ===" 