# WASM Testing Guide for TegDB

This guide explains how to run TegDB tests on the WebAssembly (WASM) target.

## Overview

TegDB supports both native and WASM targets:
- **Native target**: Uses file-based storage backend
- **WASM target**: Uses browser-based storage backend (localStorage, IndexedDB)

## Prerequisites

1. **Rust and WASM target**:
   ```bash
   rustup target add wasm32-unknown-unknown
   ```

2. **wasm-bindgen-cli** (for advanced testing):
   ```bash
   cargo install wasm-bindgen-cli
   ```

## Quick Start

### 1. Build for WASM
```bash
cargo build --target wasm32-unknown-unknown --all-features
```

### 2. Run Basic WASM Tests
```bash
# This will compile but not execute (as expected)
cargo test --target wasm32-unknown-unknown --all-features --lib
```

## Running WASM Tests

### Method 1: Browser-based Testing (Recommended)

1. **Build the WASM target**:
   ```bash
   cargo build --target wasm32-unknown-unknown --all-features
   ```

2. **Open the test runner**:
   ```bash
   open wasm_test_runner.html
   ```
   Or serve it with a local server:
   ```bash
   python3 -m http.server 8000
   # Then open http://localhost:8000/wasm_test_runner.html
   ```

3. **Click "Run WASM Tests"** in the browser

### Method 2: Using wasm-bindgen-test-runner

1. **Build for WASM**:
   ```bash
   cargo build --target wasm32-unknown-unknown --all-features
   ```

2. **Run tests**:
   ```bash
   ./run_wasm_tests_node.sh
   ```

### Method 3: Manual WASM Test Execution

1. **Find the WASM files**:
   ```bash
   find target/wasm32-unknown-unknown/debug/deps -name "*.wasm"
   ```

2. **Run with wasm-bindgen-test-runner**:
   ```bash
   wasm-bindgen-test-runner target/wasm32-unknown-unknown/debug/deps/tegdb-*.wasm
   ```

## Test Structure

### WASM-Specific Tests
- Located in `tests/wasm_tests.rs`
- Use `#[wasm_bindgen_test]` attribute
- Test browser backend functionality

### Cross-Platform Tests
- Located in other test files
- Use `run_with_both_backends()` helper
- Automatically run on both native and WASM targets

### Library Tests
- Located in `src/lib.rs` (tests module)
- Use regular `#[test]` with `#[cfg(target_arch = "wasm32")]`
- Test basic WASM compilation and functionality

## Test Coverage

### WASM Tests Cover:
- ✅ Basic database operations (CREATE, INSERT, SELECT, UPDATE, DELETE)
- ✅ Data type handling (INTEGER, TEXT, REAL, NULL)
- ✅ Transaction support (BEGIN, COMMIT, ROLLBACK)
- ✅ Schema persistence across sessions
- ✅ Error handling and validation
- ✅ Browser storage backend (localStorage)

### Backend-Specific Features:
- **File Backend** (Native): File locking, concurrent access
- **Browser Backend** (WASM): localStorage persistence, browser APIs

## Troubleshooting

### Common Issues

1. **"cannot execute binary file" error**:
   - This is expected! Native `cargo test` cannot run `.wasm` files
   - Use the browser-based test runner or `wasm-bindgen-test-runner`

2. **"wasm-bindgen-test-runner not found"**:
   ```bash
   cargo install wasm-bindgen-cli
   ```

3. **Browser storage not available**:
   - Ensure you're running in a browser environment
   - Check that localStorage is enabled

4. **CORS issues**:
   - Serve files through a local web server
   - Don't open HTML files directly from file:// protocol

### Debug Tips

1. **Check WASM compilation**:
   ```bash
   cargo check --target wasm32-unknown-unknown --all-features
   ```

2. **View browser console**:
   - Open Developer Tools (F12)
   - Check Console tab for errors

3. **Test individual functions**:
   - Use the browser test runner
   - Check the output for specific test results

## Continuous Integration

For CI/CD, you can:

1. **Build and verify compilation**:
   ```bash
   cargo build --target wasm32-unknown-unknown --all-features
   ```

2. **Run in headless browser** (requires additional setup):
   ```bash
   # Install headless browser
   npm install -g puppeteer
   
   # Run tests
   node wasm_test_runner.js
   ```

## Performance Notes

- WASM tests run in browser environment
- Performance may vary based on browser implementation
- Browser storage has different performance characteristics than file storage
- Consider running performance tests on both targets for comparison

## Next Steps

1. **Add more WASM-specific tests** in `tests/wasm_tests.rs`
2. **Test browser-specific features** (IndexedDB, Web Workers)
3. **Add performance benchmarks** for WASM vs native
4. **Set up automated WASM testing** in CI/CD pipeline

---

For more information, see:
- [wasm-bindgen documentation](https://rustwasm.github.io/wasm-bindgen/)
- [WebAssembly testing guide](https://rustwasm.github.io/wasm-bindgen/wasm-bindgen-test/usage.html)
- [TegDB browser backend implementation](src/backends/browser_log_backend.rs) 