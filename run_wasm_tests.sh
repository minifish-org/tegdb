#!/bin/bash

# Script to run WASM tests for TegDB
# This script builds the WASM target and runs the tests

set -e

echo "=== Building WASM target ==="
cargo build --target wasm32-unknown-unknown --all-features

echo ""
echo "=== Running WASM tests ==="
echo "Note: This will run tests in a browser environment"
echo ""

# Find the WASM test files
WASM_TEST_FILES=$(find target/wasm32-unknown-unknown/debug/deps -name "*.wasm" -type f)

if [ -z "$WASM_TEST_FILES" ]; then
    echo "No WASM test files found. Make sure you've built the WASM target first."
    exit 1
fi

# Run each WASM test file
for wasm_file in $WASM_TEST_FILES; do
    echo "Running tests in: $(basename $wasm_file)"
    
    # Check if wasm-bindgen-test-runner is available
    if command -v wasm-bindgen-test-runner &> /dev/null; then
        wasm-bindgen-test-runner "$wasm_file"
    else
        echo "wasm-bindgen-test-runner not found. Installing..."
        cargo install wasm-bindgen-cli
        wasm-bindgen-test-runner "$wasm_file"
    fi
done

echo ""
echo "=== WASM tests completed ===" 