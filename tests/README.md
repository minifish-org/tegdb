# Test Helpers for Multi-Backend Testing

This directory contains test helpers that make it easy to run the same test logic with different storage backends (file and browser).

## Overview

TegDB supports multiple storage backends:
- **File backend** (`file://`) - For native platforms, stores data in files
- **Browser backend** (`browser://`, `localstorage://`, `indexeddb://`) - For WASM platforms, stores data in browser storage

The test helpers allow you to write a test once and run it with both backends automatically.

## Quick Start

### 1. Add the test helpers to your test file

```rust
mod test_helpers;
use test_helpers::run_with_both_backends;
```

### 2. Wrap your test logic

```rust
#[test]
fn my_test() -> Result<()> {
    run_with_both_backends("my_test_name", |db_path| {
        let mut db = Database::open(&format!("file://{}", db_path.display()))?;
        
        // Your test logic here...
        db.execute("CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)")?;
        db.execute("INSERT INTO users (id, name) VALUES (1, 'Alice')")?;
        
        let result = db.query("SELECT * FROM users").unwrap();
        assert_eq!(result.rows().len(), 1);
        
        Ok(())
    })
}
```

### 3. That's it!

Your test will automatically run with:
- File backend (when targeting native platforms)
- Browser backend (when targeting WASM)

## Converting Existing Tests

If you have an existing test that uses `NamedTempFile`, conversion is simple:

### Before (existing pattern):
```rust
#[test]
fn test_something() -> Result<()> {
    let temp_file = NamedTempFile::new().expect("Failed to create temp file");
    let db_path = temp_file.path();
    let mut db = Database::open(&format!("file://{}", db_path.display()))?;
    
    // test logic...
    Ok(())
}
```

### After (with test helpers):
```rust
#[test]
fn test_something() -> Result<()> {
    run_with_both_backends("test_something", |db_path| {
        let mut db = Database::open(&format!("file://{}", db_path.display()))?;
        
        // test logic...
        Ok(())
    })
}
```

## Available Helper Functions

### `run_with_both_backends(test_name, test_fn)`
Runs the test with both file and browser backends (when available).

### `run_with_file_backend(test_name, test_fn)`
Runs the test with file backend only.

### `run_with_browser_backend(test_name, test_fn)`
Runs the test with browser backend only (WASM only).

### `run_with_backend(test_name, backend, test_fn)`
Runs the test with a specific backend:
- `"file"` - File backend
- `"browser"` - Browser backend
- `"localstorage"` - LocalStorage backend
- `"indexeddb"` - IndexedDB backend

## Examples

See `backend_compatibility_test.rs` for complete working examples including:
- Basic CRUD operations
- Transactions
- Data types
- Schema persistence
- Converting existing tests

## Benefits

1. **No code duplication** - Write test logic once, run with multiple backends
2. **Automatic backend selection** - Tests run with appropriate backends for the target platform
3. **Easy maintenance** - Changes to test logic automatically apply to all backends
4. **Comprehensive testing** - Ensures your code works with all supported storage backends

## Platform Support

- **Native platforms**: Tests run with file backend
- **WASM platforms**: Tests run with both file and browser backends (when available)

## Troubleshooting

### Test only runs with file backend
This is normal when not targeting WASM. The browser backend tests are only run when `target_arch = "wasm32"`.

### Browser backend tests fail
Make sure you're targeting WASM (`--target wasm32-unknown-unknown`) and that the browser storage APIs are available.

### Import errors
Make sure you have `mod test_helpers;` at the top of your test file and `use test_helpers::run_with_both_backends;` in your imports. 