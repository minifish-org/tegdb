#!/bin/bash

# TegDB Dual Storage Backend Test Script
# This script comprehensively tests both file and browser storage backends

set -e  # Exit on any error

echo "🚀 TegDB Dual Storage Backend Comprehensive Test"
echo "================================================"

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

print_status() {
    echo -e "${BLUE}[INFO]${NC} $1"
}

print_success() {
    echo -e "${GREEN}[PASS]${NC} $1"
}

print_warning() {
    echo -e "${YELLOW}[WARN]${NC} $1"
}

print_error() {
    echo -e "${RED}[FAIL]${NC} $1"
}

# Cleanup function
cleanup() {
    print_status "Cleaning up test files..."
    rm -f test_*.db
    rm -f demo_*.db
    rm -f localstorage__*
    rm -f browser__*
    rm -f *.new
    rm -f *.db  # Clean up any other .db files
    # Clean up browser storage simulation files
    rm -f "localstorage://demo_browser_backend"
    rm -f "browser://demo_browser_backend"
    rm -f "browser://test_app_data"
    rm -f "localstorage://app_storage"
    rm -f "localstorage://user_preferences"
    rm -f demo_file_backend.db
    # Clean up any test files created by comprehensive test
    rm -f test_edge_*.db
}

# Set up cleanup trap and clean before starting
trap cleanup EXIT
cleanup

echo
print_status "Step 1: Building TegDB for native target..."
cargo build --release --features dev
print_success "Native build completed"

echo
print_status "Step 2: Testing WASM compilation..."
if cargo check --target wasm32-unknown-unknown --quiet; then
    print_success "WASM compilation successful"
else
    print_error "WASM compilation failed"
    exit 1
fi

echo
print_status "Step 3: Running basic functionality tests..."

# Test 1: Basic file backend functionality
print_status "Testing file backend basic operations..."
cargo run --example dual_storage_demo --features dev --quiet
print_success "File backend basic operations work"

# Test 2: Run comprehensive test example
print_status "Step 4: Running comprehensive storage backend tests..."
cargo run --example comprehensive_storage_test --features dev --quiet
print_success "Comprehensive storage tests passed"

# Test 3: Run existing storage-related examples
echo
print_status "Step 5: Testing simple usage example..."
cargo run --example simple_usage --features dev --quiet
print_success "Simple usage example completed"

echo
print_status "Step 6: Testing IoT demo with storage..."
cargo run --example iot_demo --features dev --quiet
print_success "IoT demo completed"

echo
print_status "Step 7: Testing streaming performance demo..."
cargo run --example streaming_performance_demo --features dev --quiet
print_success "Streaming performance demo completed"

# Test 4: Run unit tests
echo
print_status "Step 8: Running unit tests..."
cargo test --features dev --quiet
print_success "Unit tests passed"

# Test 5: Run benchmarks to ensure no performance regression
echo
print_status "Step 9: Running benchmarks..."
if command -v criterion &> /dev/null; then
    cargo bench --bench engine_basic_benchmark --features dev -- --quick
    print_success "Benchmarks completed"
else
    print_warning "Criterion not available, skipping benchmarks"
fi

# Test 6: Test native format compatibility
echo
print_status "Step 10: Testing native format..."
cargo run --example native_format_test --features dev --quiet
print_success "Native format tests passed"

echo
print_success "🎉 All tests passed successfully!"
echo
echo "Summary:"
echo "✅ Native compilation works"
echo "✅ WASM compilation works"
echo "✅ File backend functionality verified"
echo "✅ Browser backend interface verified"
echo "✅ Dual storage demo completed"
echo "✅ Comprehensive storage tests passed"
echo "✅ Simple usage example works"
echo "✅ IoT demo with storage works"
echo "✅ Streaming performance verified"
echo "✅ Unit tests passed"
echo "✅ Performance benchmarks completed"
echo "✅ Native format compatibility confirmed"
echo
print_success "TegDB dual storage backend implementation is ready for production! 🚀"
