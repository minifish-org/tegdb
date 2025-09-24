#!/bin/bash

# Native-focused test script for TegDB

set -e

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Configuration flags
CI_MODE=${CI_MODE:-false}
VERBOSE=${VERBOSE:-false}
SKIP_NATIVE=${SKIP_NATIVE:-false}

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

usage() {
    cat <<USAGE
Usage: ./run_all_tests.sh [options]

Options:
  --ci          Enable CI mode (adds -- --nocapture to cargo test)
  --verbose     Run cargo test with --verbose
  --skip-native Skip running native test suite
  --help        Show this help message
USAGE
}

while [[ $# -gt 0 ]]; do
    case "$1" in
        --ci)
            CI_MODE=true
            shift
            ;;
        --verbose)
            VERBOSE=true
            shift
            ;;
        --skip-native)
            SKIP_NATIVE=true
            shift
            ;;
        --help|-h)
            usage
            exit 0
            ;;
        *)
            print_error "Unknown option: $1"
            usage
            exit 1
            ;;
    esac
done

run_native_tests() {
    if [ "$SKIP_NATIVE" = "true" ]; then
        print_warning "Skipping native tests"
        return 0
    fi

    print_status "Running native tests (file backend only)..."

    local cargo_args=(test --all-features)
    if [ "$VERBOSE" = "true" ]; then
        cargo_args+=(--verbose)
    fi
    if [ "$CI_MODE" = "true" ]; then
        cargo_args+=(-- --nocapture)
    fi

    cargo "${cargo_args[@]}"

    print_success "Native tests completed successfully"
}

print_status "CI mode: $CI_MODE"
print_status "Verbose: $VERBOSE"
print_status "Skip native: $SKIP_NATIVE"

run_native_tests

print_success "All requested test suites completed"
