# Platform-Specific Test Refactoring Guide

This guide shows how to convert platform-specific tests that use `StorageEngine` directly to use the high-level `Database` API with `run_with_both_backends` for maximum WASM coverage.

## Overview

**Goal**: Convert tests from low-level `StorageEngine` operations to high-level SQL operations that work on both native (file backend) and WASM (browser backend).

## Refactoring Pattern

### Before (Platform-Specific)
```rust
#[test]
fn test_something() {
    let test_db_path = PathBuf::from("/tmp/test.db");
    
    // Clean up if the file exists
    if test_db_path.exists() {
        std::fs::remove_file(&test_db_path).unwrap();
    }
    
    let mut engine = StorageEngine::new(test_db_path.clone()).unwrap();
    
    // Low-level operations
    engine.set(b"key1", b"value1".to_vec()).unwrap();
    engine.set(b"key2", b"value2".to_vec()).unwrap();
    
    // Assertions
    assert_eq!(engine.get(b"key1").as_deref(), Some(b"value1" as &[u8]));
    
    // Clean up
    std::fs::remove_file(&test_db_path).unwrap();
}
```

### After (Backend-Agnostic)
```rust
mod test_helpers;

#[cfg(test)]
mod my_tests {
    use tegdb::{Database, Result, SqlValue};
    use test_helpers::run_with_both_backends;

    #[test]
    fn test_something() -> Result<()> {
        run_with_both_backends("test_something", |db_path| {
            let mut db = Database::open(db_path)?;
            
            // Create a test table
            db.execute("CREATE TABLE test_data (key TEXT PRIMARY KEY, value TEXT)")?;
            
            // High-level SQL operations
            db.execute("INSERT INTO test_data (key, value) VALUES ('key1', 'value1')")?;
            db.execute("INSERT INTO test_data (key, value) VALUES ('key2', 'value2')")?;
            
            // Assertions using SQL queries
            let result = db.query("SELECT value FROM test_data WHERE key = 'key1'")?;
            assert_eq!(result.rows().len(), 1);
            assert_eq!(result.rows()[0][0], SqlValue::Text("value1".to_string()));
            
            Ok(())
        })
    }
}
```

## Key Conversion Patterns

### 1. StorageEngine Operations → SQL Operations

| StorageEngine | SQL Equivalent |
|---------------|----------------|
| `engine.set(key, value)` | `db.execute("INSERT INTO table (key, value) VALUES (?, ?)")` |
| `engine.get(key)` | `db.query("SELECT value FROM table WHERE key = ?")` |
| `engine.delete(key)` | `db.execute("DELETE FROM table WHERE key = ?")` |
| `engine.begin_transaction()` | `db.begin_transaction()` |

### 2. Transaction Operations

| StorageEngine Transaction | SQL Transaction |
|---------------------------|-----------------|
| `tx.set(key, value)` | `tx.execute("INSERT INTO table (key, value) VALUES (?, ?)")` |
| `tx.get(key)` | `tx.query("SELECT value FROM table WHERE key = ?")` |
| `tx.delete(key)` | `tx.execute("DELETE FROM table WHERE key = ?")` |
| `tx.commit()` | `tx.commit()` |
| `tx.rollback()` | `tx.rollback()` |

### 3. Assertions

| StorageEngine Assertion | SQL Assertion |
|-------------------------|---------------|
| `assert_eq!(engine.get(key), Some(value))` | `let result = db.query("SELECT value FROM table WHERE key = ?")?; assert_eq!(result.rows()[0][0], SqlValue::Text(value));` |
| `assert_eq!(engine.get(key), None)` | `let result = db.query("SELECT value FROM table WHERE key = ?")?; assert_eq!(result.rows().len(), 0);` |

## Files to Refactor

### High Priority (Easy Conversions)
1. **`tests/commit_marker_tests.rs`** ✅ (Already done)
2. **`tests/read_only_transaction_test.rs`** - Simple transaction tests
3. **`tests/schema_persistence_test.rs`** - Schema and persistence tests

### Medium Priority (Moderate Complexity)
4. **`tests/transaction_tests.rs`** - Complex transaction scenarios
5. **`tests/engine_tests.rs`** - Core engine functionality tests

### Low Priority (Complex Conversions)
6. **`tests/high_level_api_performance_test.rs`** - Performance tests
7. **`tests/schema_performance_test.rs`** - Performance tests

## Step-by-Step Refactoring Process

### Step 1: Add Module Declaration
```rust
mod test_helpers;
```

### Step 2: Update Imports
```rust
use tegdb::{Database, Result, SqlValue};
use test_helpers::run_with_both_backends;
```

### Step 3: Remove Platform-Specific Code
- Remove `use std::path::PathBuf;`
- Remove `use std::fs;`
- Remove `use tegdb::storage_engine::StorageEngine;`
- Remove file cleanup code (`fs::remove_file`, `temp_db_path` function)

### Step 4: Wrap Test Functions
```rust
#[test]
fn test_name() -> Result<()> {
    run_with_both_backends("test_name", |db_path| {
        // Test logic here
        Ok(())
    })
}
```

### Step 5: Convert Operations
- Replace `StorageEngine::new(path)` with `Database::open(db_path)`
- Convert low-level operations to SQL
- Update assertions to use SQL queries

### Step 6: Test the Refactored Code
```bash
cargo test test_name --verbose
```

## Example: Complete Refactoring

### Original: `tests/commit_marker_tests.rs`
```rust
#[cfg(test)]
mod commit_marker_tests {
    use std::path::PathBuf;
    use tegdb::storage_engine::StorageEngine;

    #[test]
    fn test_commit_marker_and_crash_recovery() {
        let test_db_path = PathBuf::from("/tmp/test_commit_marker_recovery.db");
        
        if test_db_path.exists() {
            std::fs::remove_file(&test_db_path).unwrap();
        }
        
        {
            let mut engine = StorageEngine::new(test_db_path.clone()).unwrap();
            
            {
                let mut tx = engine.begin_transaction();
                tx.set(b"key1", b"value1".to_vec()).unwrap();
                tx.set(b"key2", b"value2".to_vec()).unwrap();
                tx.commit().unwrap();
            }
            
            {
                let mut tx = engine.begin_transaction();
                tx.set(b"key3", b"value3".to_vec()).unwrap();
            }
        }
        
        {
            let engine = StorageEngine::new(test_db_path.clone()).unwrap();
            assert_eq!(engine.get(b"key1").as_deref(), Some(b"value1" as &[u8]));
            assert_eq!(engine.get(b"key2").as_deref(), Some(b"value2" as &[u8]));
            assert_eq!(engine.get(b"key3"), None);
        }
        
        std::fs::remove_file(&test_db_path).unwrap();
    }
}
```

### Refactored: `tests/commit_marker_tests.rs`
```rust
mod test_helpers;

#[cfg(test)]
mod commit_marker_tests {
    use tegdb::{Database, Result, SqlValue};
    use test_helpers::run_with_both_backends;

    #[test]
    fn test_commit_marker_and_crash_recovery() -> Result<()> {
        run_with_both_backends("test_commit_marker_and_crash_recovery", |db_path| {
            let mut db = Database::open(db_path)?;
            
            db.execute("CREATE TABLE test_data (key TEXT PRIMARY KEY, value TEXT)")?;
            
            {
                let mut tx = db.begin_transaction()?;
                tx.execute("INSERT INTO test_data (key, value) VALUES ('key1', 'value1')")?;
                tx.execute("INSERT INTO test_data (key, value) VALUES ('key2', 'value2')")?;
                tx.commit()?;
            }
            
            {
                let mut tx = db.begin_transaction()?;
                tx.execute("INSERT INTO test_data (key, value) VALUES ('key3', 'value3')")?;
            }
            
            let mut db2 = Database::open(db_path)?;
            
            let result1 = db2.query("SELECT value FROM test_data WHERE key = 'key1'")?;
            assert_eq!(result1.rows().len(), 1);
            assert_eq!(result1.rows()[0][0], SqlValue::Text("value1".to_string()));
            
            let result2 = db2.query("SELECT value FROM test_data WHERE key = 'key2'")?;
            assert_eq!(result2.rows().len(), 1);
            assert_eq!(result2.rows()[0][0], SqlValue::Text("value2".to_string()));
            
            let result3 = db2.query("SELECT value FROM test_data WHERE key = 'key3'")?;
            assert_eq!(result3.rows().len(), 0);
            
            Ok(())
        })
    }
}
```

## Benefits of Refactoring

1. **Cross-Platform Compatibility**: Tests run on both native and WASM
2. **Higher-Level Testing**: Tests the actual SQL API that users will use
3. **Better Coverage**: Same test logic runs on multiple backends
4. **Maintainability**: Single test covers multiple scenarios
5. **WASM Support**: Automatic WASM test generation via `generate_wasm_tests.py`

## Next Steps

1. **Fix Import Issues**: Resolve the `test_helpers` import problem
2. **Refactor Priority Files**: Start with the easiest conversions
3. **Test Incrementally**: Test each refactored file individually
4. **Regenerate WASM Tests**: Run `python3 generate_wasm_tests.py`
5. **Run Full Test Suite**: Use `./run_all_tests.sh`

## Troubleshooting

### Import Issues
If you get import errors for `test_helpers`, ensure:
- `mod test_helpers;` is at the top level of the file
- The file structure matches other working test files

### Protocol Errors
If you get "Unsupported protocol" errors:
- Use `run_with_both_backends` instead of hardcoded paths
- The helper automatically uses the correct protocol for each platform

### SQL Conversion Issues
If SQL operations don't work as expected:
- Check that tables are created before use
- Verify SQL syntax matches the supported subset
- Use simple operations first, then add complexity

## Expected Results

After refactoring, you should have:
- **183 Native Tests** (file backend)
- **183 WASM Tests** (localStorage backend)
- **Same test coverage** on both platforms
- **Automatic WASM test generation** working
- **Cross-platform compatibility** for all backend-agnostic tests 