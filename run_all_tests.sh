#!/bin/bash

# Comprehensive test script for TegDB
# Runs both native (file backend) and WASM (browser backend) tests
# Supports both local development and CI environments

set -e

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Configuration
CI_MODE=${CI_MODE:-false}
SKIP_NATIVE=${SKIP_NATIVE:-false}
SKIP_WASM=${SKIP_WASM:-false}
SKIP_BROWSER=${SKIP_BROWSER:-false}
VERBOSE=${VERBOSE:-false}

# Function to print colored output
print_status() {
    echo -e "${BLUE}[INFO]${NC} $1"
}

print_success() {
    echo -e "${GREEN}[SUCCESS]${NC} $1"
}

print_warning() {
    echo -e "${YELLOW}[WARNING]${NC} $1"
}

print_error() {
    echo -e "${RED}[ERROR]${NC} $1"
}

# Function to check if command exists
command_exists() {
    command -v "$1" >/dev/null 2>&1
}

# Function to install wasm-bindgen if needed
install_wasm_bindgen() {
    if ! command_exists wasm-bindgen; then
        print_status "Installing wasm-bindgen-cli..."
        cargo install wasm-bindgen-cli
    fi
}

# Function to run native tests
run_native_tests() {
    if [ "$SKIP_NATIVE" = "true" ]; then
        print_warning "Skipping native tests"
        return 0
    fi

    print_status "Running native tests (file backend)..."
    
    if [ "$VERBOSE" = "true" ]; then
        cargo test --all-features --verbose
    else
        cargo test --all-features
    fi
    
    print_success "Native tests completed successfully"
}

# Function to build WASM target
build_wasm() {
    print_status "Building WASM target..."
    cargo build --target wasm32-unknown-unknown --all-features
    print_success "WASM target built successfully"
}

# Function to run WASM unit tests
run_wasm_unit_tests() {
    if [ "$SKIP_WASM" = "true" ]; then
        print_warning "Skipping WASM unit tests"
        return 0
    fi

    print_status "Running WASM unit tests..."
    
    # Install wasm-bindgen if needed
    install_wasm_bindgen
    
    # Find WASM test files
    WASM_TEST_FILES=$(find target/wasm32-unknown-unknown/debug/deps -name "*.wasm" -type f)
    
    if [ -z "$WASM_TEST_FILES" ]; then
        print_error "No WASM test files found"
        return 1
    fi
    
    # Run each WASM test file
    for wasm_file in $WASM_TEST_FILES; do
        print_status "Running tests in: $(basename "$wasm_file")"
        wasm-bindgen-test-runner "$wasm_file"
    done
    
    print_success "WASM unit tests completed successfully"
}

# Function to run browser-based tests
run_browser_tests() {
    if [ "$SKIP_BROWSER" = "true" ]; then
        print_warning "Skipping browser-based tests"
        return 0
    fi

    print_status "Running browser-based tests..."
    
    # Install wasm-bindgen if needed
    install_wasm_bindgen
    
    # Generate JavaScript bindings for browser tests
    WASM_FILE=$(find target/wasm32-unknown-unknown/debug/deps -name "tegdb-*.wasm" -type f | head -1)
    
    if [ -z "$WASM_FILE" ]; then
        print_error "No WASM file found for browser tests"
        return 1
    fi
    
    print_status "Generating JavaScript bindings..."
    mkdir -p target/wasm32-unknown-unknown/debug/deps
    wasm-bindgen "$WASM_FILE" \
        --out-dir target/wasm32-unknown-unknown/debug/deps \
        --target web
    
    if [ "$CI_MODE" = "true" ]; then
        # In CI, use headless browser testing
        print_status "Running headless browser tests..."
        
        if command_exists wasm-pack; then
            wasm-pack test --headless --firefox
        else
            print_warning "wasm-pack not found, skipping headless browser tests"
        fi
    else
        # In local development, provide instructions for manual testing
        print_status "Browser tests ready for manual execution"
        print_status "Open wasm_test_runner.html in your browser to run comprehensive tests"
        print_status "Or run: python3 -m http.server 8000 && open http://localhost:8000/wasm_test_runner.html"
    fi
    
    print_success "Browser test setup completed"
}

# Function to run performance tests
run_performance_tests() {
    if [ "$CI_MODE" = "true" ]; then
        print_warning "Skipping performance tests in CI mode"
        return 0
    fi

    if [ -f "run_performance_tests.sh" ]; then
        print_status "Running performance tests..."
        ./run_performance_tests.sh
        print_success "Performance tests completed"
    else
        print_warning "Performance test script not found"
    fi
}

# Function to show usage
show_usage() {
    echo "Usage: $0 [OPTIONS]"
    echo ""
    echo "Options:"
    echo "  --ci              Run in CI mode (headless browser tests)"
    echo "  --skip-native     Skip native tests"
    echo "  --skip-wasm       Skip WASM unit tests"
    echo "  --skip-browser    Skip browser-based tests"
    echo "  --verbose         Enable verbose output"
    echo "  --help            Show this help message"
    echo ""
    echo "Environment variables:"
    echo "  CI_MODE           Set to 'true' for CI mode"
    echo "  SKIP_NATIVE       Set to 'true' to skip native tests"
    echo "  SKIP_WASM         Set to 'true' to skip WASM tests"
    echo "  SKIP_BROWSER      Set to 'true' to skip browser tests"
    echo "  VERBOSE           Set to 'true' for verbose output"
}

# Parse command line arguments
while [[ $# -gt 0 ]]; do
    case $1 in
        --ci)
            CI_MODE=true
            shift
            ;;
        --skip-native)
            SKIP_NATIVE=true
            shift
            ;;
        --skip-wasm)
            SKIP_WASM=true
            shift
            ;;
        --skip-browser)
            SKIP_BROWSER=true
            shift
            ;;
        --verbose)
            VERBOSE=true
            shift
            ;;
        --help)
            show_usage
            exit 0
            ;;
        *)
            print_error "Unknown option: $1"
            show_usage
            exit 1
            ;;
    esac
done

# Main execution
echo "=== TegDB Comprehensive Test Suite ==="
echo "CI Mode: $CI_MODE"
echo "Skip Native: $SKIP_NATIVE"
echo "Skip WASM: $SKIP_WASM"
echo "Skip Browser: $SKIP_BROWSER"
echo "Verbose: $VERBOSE"
echo ""

# Check if we're in a CI environment
if [ "$CI" = "true" ] || [ "$GITHUB_ACTIONS" = "true" ] || [ "$GITLAB_CI" = "true" ]; then
    CI_MODE=true
    print_status "Detected CI environment, enabling CI mode"
fi

# Run tests
run_native_tests
build_wasm
run_wasm_unit_tests
run_browser_tests
run_performance_tests

echo ""
print_success "=== All tests completed successfully! ==="

if [ "$CI_MODE" != "true" ]; then
    echo ""
    echo "Next steps:"
    echo "1. Open wasm_test_runner.html in your browser for comprehensive WASM testing"
    echo "2. Run ./run_performance_tests.sh for detailed performance analysis"
    echo "3. Check the browser console for any test output"
fi 