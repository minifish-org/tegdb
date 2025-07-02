#!/bin/bash

# TegDB High-Level API Performance Test Runner
# This script provides convenient ways to run performance tests for TegDB

set -e

DB_DIR="/Users/yusp/work/tegdb"
cd "$DB_DIR"

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

print_header() {
    echo -e "${BLUE}=================================${NC}"
    echo -e "${BLUE}  $1${NC}"
    echo -e "${BLUE}=================================${NC}"
}

print_success() {
    echo -e "${GREEN}✓ $1${NC}"
}

print_warning() {
    echo -e "${YELLOW}⚠ $1${NC}"
}

print_error() {
    echo -e "${RED}✗ $1${NC}"
}

show_usage() {
    echo "TegDB Performance Test Runner"
    echo ""
    echo "Usage: $0 [command]"
    echo ""
    echo "Commands:"
    echo "  all, comprehensive  - Run the complete performance suite"
    echo "  crud               - Run basic CRUD operations performance test"
    echo "  streaming          - Run streaming query performance test"
    echo "  transaction        - Run transaction performance test"
    echo "  schema             - Run schema operations performance test"
    echo "  large              - Run large dataset performance test"
    echo "  concurrent         - Run concurrent operations performance test"
    echo "  memory             - Run memory usage pattern test"
    echo "  quick              - Run a quick subset of tests"
    echo "  benchmark          - Compare with existing benchmarks"
    echo "  profile            - Run with profiling (requires additional setup)"
    echo ""
    echo "Options:"
    echo "  --release          - Run in release mode (default)"
    echo "  --debug            - Run in debug mode"
    echo "  --verbose          - Show verbose output"
    echo "  --help, -h         - Show this help message"
}

run_test() {
    local test_name="$1"
    local mode="${2:-release}"
    local verbose="${3:-false}"
    
    print_header "Running $test_name Performance Test"
    
    local cargo_args="test $test_name"
    if [[ "$mode" == "release" ]]; then
        cargo_args="$cargo_args --release"
    fi
    
    # Always use dev feature for detailed pipeline analysis
    cargo_args="$cargo_args --features dev"
    
    if [[ "$verbose" == "true" ]]; then
        cargo_args="$cargo_args -- --nocapture"
    else
        cargo_args="$cargo_args -- --nocapture --quiet"
    fi
    
    if cargo $cargo_args; then
        print_success "$test_name test completed successfully"
    else
        print_error "$test_name test failed"
        exit 1
    fi
    echo ""
}

run_benchmark_comparison() {
    print_header "Running Benchmark Comparison"
    
    echo "Running TegDB performance tests..."
    cargo test run_comprehensive_performance_suite --release --features dev -- --nocapture --quiet
    
    echo ""
    echo "Running existing benchmarks for comparison..."
    cargo bench --bench engine_basic_benchmark
    
    print_success "Benchmark comparison completed"
}

run_profiling() {
    print_header "Running Performance Tests with Profiling"
    
    if ! command -v perf &> /dev/null; then
        print_warning "perf tool not found. Install it for detailed profiling."
        print_warning "On macOS, try: brew install linux-perf-tools"
        print_warning "Falling back to time-based profiling..."
        time cargo test run_comprehensive_performance_suite --release -- --nocapture
    else
        print_success "Running with perf profiling..."
        perf record -g cargo test run_comprehensive_performance_suite --release -- --nocapture
        perf report
    fi
}

run_quick_tests() {
    print_header "Running Quick Performance Tests"
    
    echo "Running basic CRUD test..."
    cargo test test_basic_crud_performance --release --features dev -- --nocapture --quiet
    
    echo ""
    echo "Running streaming test..."
    cargo test test_streaming_query_performance --release --features dev -- --nocapture --quiet
    
    echo ""
    echo "Running transaction test..."
    cargo test test_transaction_performance --release --features dev -- --nocapture --quiet
    
    print_success "Quick tests completed"
}

# Parse command line arguments
COMMAND="${1:-help}"
MODE="release"
VERBOSE="false"

# Process additional flags
shift || true
while [[ $# -gt 0 ]]; do
    case $1 in
        --debug)
            MODE="debug"
            shift
            ;;
        --release)
            MODE="release"
            shift
            ;;
        --verbose)
            VERBOSE="true"
            shift
            ;;
        --help|-h)
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

# Main command processing
case $COMMAND in
    all|comprehensive)
        run_test "run_comprehensive_performance_suite" "$MODE" "$VERBOSE"
        ;;
    crud)
        run_test "test_basic_crud_performance" "$MODE" "$VERBOSE"
        ;;
    streaming)
        run_test "test_streaming_query_performance" "$MODE" "$VERBOSE"
        ;;
    transaction)
        run_test "test_transaction_performance" "$MODE" "$VERBOSE"
        ;;
    schema)
        run_test "test_schema_operations_performance" "$MODE" "$VERBOSE"
        ;;
    large)
        run_test "test_large_dataset_performance" "$MODE" "$VERBOSE"
        ;;
    concurrent)
        run_test "test_concurrent_schema_access_performance" "$MODE" "$VERBOSE"
        ;;
    memory)
        run_test "test_memory_usage_pattern" "$MODE" "$VERBOSE"
        ;;
    quick)
        run_quick_tests
        ;;
    benchmark)
        run_benchmark_comparison
        ;;
    profile)
        run_profiling
        ;;
    help|--help|-h)
        show_usage
        ;;
    *)
        print_error "Unknown command: $COMMAND"
        echo ""
        show_usage
        exit 1
        ;;
esac
