#!/bin/bash

# Script to build WASM and generate JavaScript bindings

set -e

echo "=== Building WASM target ==="
cargo build --target wasm32-unknown-unknown --all-features

echo ""
echo "=== Generating JavaScript bindings ==="

# Find the WASM file
WASM_FILE=$(find target/wasm32-unknown-unknown/debug/deps -name "tegdb-*.wasm" -type f | head -1)

if [ -z "$WASM_FILE" ]; then
    echo "No WASM file found. Make sure you've built the WASM target first."
    exit 1
fi

echo "Found WASM file: $WASM_FILE"

# Create output directory
mkdir -p target/wasm32-unknown-unknown/debug/deps

# Generate JavaScript bindings
wasm-bindgen "$WASM_FILE" \
    --out-dir target/wasm32-unknown-unknown/debug/deps \
    --target web

echo ""
echo "=== JavaScript bindings generated ==="
echo "Files created:"
ls -la target/wasm32-unknown-unknown/debug/deps/tegdb.*

echo ""
echo "=== WASM build complete ==="
echo "You can now run the browser test runner!" 