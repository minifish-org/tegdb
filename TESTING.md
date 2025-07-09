# TegDB Testing Guide

This document explains how to run tests for TegDB, including both native and WASM-based testing approaches.

## Quick Start

### Run All Tests (Recommended)
```bash
./run_all_tests.sh
```

This will run:
- Native tests (file backend)
- WASM unit tests
- Browser-based tests setup
- Performance tests (if available)

## Test Script Options

### Command Line Options
```bash
./run_all_tests.sh [OPTIONS]

Options:
  --ci              Run in CI mode (headless browser tests)
  --skip-native     Skip native tests
  --skip-wasm       Skip WASM unit tests
  --skip-browser    Skip browser-based tests
  --verbose         Enable verbose output
  --help            Show help message
```

### Environment Variables
```bash
CI_MODE=true        # Enable CI mode
SKIP_NATIVE=true    # Skip native tests
SKIP_WASM=true      # Skip WASM tests
SKIP_BROWSER=true   # Skip browser tests
VERBOSE=true        # Enable verbose output
```

## Testing Approaches

### 1. Native Tests (File Backend)
Tests that run on the native platform using file-based storage.

```bash
# Run only native tests
./run_all_tests.sh --skip-wasm --skip-browser

# Or run directly
cargo test --all-features
```

### 2. WASM Unit Tests
Tests that run in a WASM environment using browser storage backends.

```bash
# Run only WASM unit tests
./run_all_tests.sh --skip-native --skip-browser

# Or run directly
cargo test --target wasm32-unknown-unknown --all-features
```

### 3. Browser-Based Tests
Comprehensive tests that run in a real browser environment.

```bash
# Setup browser tests
./run_all_tests.sh --skip-native --skip-wasm

# Then open in browser
open wasm_test_runner.html
# Or start a local server
python3 -m http.server 8000
# Then open http://localhost:8000/wasm_test_runner.html
```

## CI/CD Integration

### GitHub Actions
The repository includes a GitHub Actions workflow (`.github/workflows/test.yml`) that automatically runs the comprehensive test suite on push and pull requests.

### Local CI Mode
```bash
# Run in CI mode locally
./run_all_tests.sh --ci --verbose
```

### Custom CI Setup
```bash
# Install dependencies
cargo install wasm-bindgen-cli
cargo install wasm-pack

# Run tests
./run_all_tests.sh --ci
```

## Individual Test Scripts

### Native Tests
- `cargo test --all-features` - Run all native tests
- `cargo test --all-features --verbose` - Run with verbose output

### WASM Tests
- `./run_wasm_tests.sh` - Run WASM tests with wasm-bindgen-test-runner
- `./run_wasm_tests_node.sh` - Alternative WASM test runner
- `./build_wasm.sh` - Build WASM and generate JavaScript bindings

### Performance Tests
- `./run_performance_tests.sh` - Run performance benchmarks
- `cargo bench` - Run Criterion benchmarks (native only)

### Browser Tests
- `wasm_test_runner.html` - Comprehensive browser test runner
- `test_wasm_simple.html` - Simple browser test page

## Test Coverage

### Native Tests
- Database operations (CRUD)
- Transaction handling
- Schema management
- Error handling
- ACID properties
- Performance benchmarks

### WASM Tests
- Browser storage backends (localStorage, IndexedDB)
- WASM-specific functionality
- Cross-platform compatibility
- Memory management

### Browser Tests
- Real browser environment testing
- User interface interactions
- Storage persistence
- Performance in browser context

## Troubleshooting

### Common Issues

1. **WASM build fails**
   ```bash
   # Ensure WASM target is installed
   rustup target add wasm32-unknown-unknown
   ```

2. **wasm-bindgen not found**
   ```bash
   # Install wasm-bindgen-cli
   cargo install wasm-bindgen-cli
   ```

3. **Browser tests fail**
   - Ensure you're serving files via HTTP (not file://)
   - Check browser console for errors
   - Verify JavaScript bindings are generated

4. **CI tests fail**
   - Check if Firefox is installed (for headless testing)
   - Verify wasm-pack is available
   - Check for memory limits in CI environment

### Debug Mode
```bash
# Run with maximum verbosity
./run_all_tests.sh --verbose

# Run specific test categories
./run_all_tests.sh --skip-browser --verbose
```

## Performance Testing

### Benchmarks
```bash
# Run Criterion benchmarks (native)
cargo bench

# Run performance tests
./run_performance_tests.sh
```

### Memory Profiling
```bash
# Build with profiling
cargo build --target wasm32-unknown-unknown --release --features dev

# Use browser dev tools for memory profiling
```

## Contributing

When adding new tests:

1. **Native tests**: Add to `tests/` directory
2. **WASM tests**: Use `#[wasm_bindgen_test]` attribute
3. **Browser tests**: Add to `wasm_test_runner.html`
4. **Performance tests**: Add to `benches/` directory

Ensure all tests pass in both native and WASM environments before submitting a pull request. 