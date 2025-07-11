//! WASM-specific tests using wasm-bindgen-test
//!
//! This module contains tests that can run in WASM environment.
//! Use `wasm-bindgen-test-runner` to execute these tests.

#[allow(clippy::duplicate_mod)]
#[path = "../helpers/test_helpers.rs"]
mod test_helpers;

#[cfg(target_arch = "wasm32")]
use wasm_bindgen_test::*;

#[cfg(target_arch = "wasm32")]
use tegdb::{Database, Result, SqlValue};

#[cfg(target_arch = "wasm32")]
wasm_bindgen_test_configure!(run_in_browser);

/// Simple test to verify WASM compilation and basic functionality
#[cfg(target_arch = "wasm32")]
#[wasm_bindgen_test]
fn test_wasm_basic_functionality() -> Result<()> {
    // Test with browser backend
    let mut db = Database::open("browser://test_db")?;

    // Test CREATE TABLE
    let affected =
        db.execute("CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT NOT NULL, age INTEGER)")?;
    assert_eq!(affected, 0); // CREATE TABLE returns 0 affected rows

    // Test INSERT
    let affected = db.execute("INSERT INTO users (id, name, age) VALUES (1, 'Alice', 30)")?;
    assert_eq!(affected, 1);

    // Test SELECT
    let result = db.query("SELECT * FROM users").unwrap();
    assert!(result.columns().contains(&"id".to_string()));
    assert!(result.columns().contains(&"name".to_string()));
    assert!(result.columns().contains(&"age".to_string()));
    assert_eq!(result.columns().len(), 3);
    assert_eq!(result.rows().len(), 1);

    // Test UPDATE
    let affected = db.execute("UPDATE users SET age = 31 WHERE name = 'Alice'")?;
    assert_eq!(affected, 1);

    // Test DELETE
    let affected = db.execute("DELETE FROM users WHERE age < 30")?;
    assert_eq!(affected, 0); // No rows deleted since age is 31

    // Verify final state
    let result = db.query("SELECT name, age FROM users").unwrap();
    assert_eq!(result.rows().len(), 1); // Alice should still be there

    Ok(())
}

/// Test data types in WASM
#[cfg(target_arch = "wasm32")]
#[wasm_bindgen_test]
fn test_wasm_data_types() -> Result<()> {
    let mut db = Database::open("browser://test_types")?;

    // Test all supported data types
    db.execute(
        "CREATE TABLE test_types (
        id INTEGER PRIMARY KEY,
        text_col TEXT,
        int_col INTEGER,
        real_col REAL,
        null_col TEXT
    )",
    )?;

    // Insert data with different types
    db.execute(&format!("INSERT INTO test_types (id, text_col, int_col, real_col, null_col) VALUES (1, 'hello', 42, {}, NULL)", std::f64::consts::PI))?;
    db.execute("INSERT INTO test_types (id, text_col, int_col, real_col, null_col) VALUES (2, 'empty_test', -100, -2.5, NULL)")?;
    db.execute("INSERT INTO test_types (id, text_col, int_col, real_col, null_col) VALUES (3, 'world', 0, 0.0, 'not null')")?;

    let result = db.query("SELECT * FROM test_types").unwrap();
    assert_eq!(result.rows().len(), 3);

    // Test first row
    let row1 = &result.rows()[0];
    let id_pos = result.columns().iter().position(|c| c == "id").unwrap();
    let text_pos = result
        .columns()
        .iter()
        .position(|c| c == "text_col")
        .unwrap();
    let int_pos = result
        .columns()
        .iter()
        .position(|c| c == "int_col")
        .unwrap();
    let real_pos = result
        .columns()
        .iter()
        .position(|c| c == "real_col")
        .unwrap();
    let null_pos = result
        .columns()
        .iter()
        .position(|c| c == "null_col")
        .unwrap();

    assert_eq!(row1[id_pos], SqlValue::Integer(1));
    assert_eq!(row1[text_pos], SqlValue::Text("hello".to_string()));
    assert_eq!(row1[int_pos], SqlValue::Integer(42));
    assert_eq!(row1[real_pos], SqlValue::Real(std::f64::consts::PI));
    assert_eq!(row1[null_pos], SqlValue::Null);

    Ok(())
}

/// Test transactions in WASM
#[cfg(target_arch = "wasm32")]
#[wasm_bindgen_test]
fn test_wasm_transactions() -> Result<()> {
    let mut db = Database::open("browser://test_transactions")?;

    // Setup test table
    db.execute("CREATE TABLE accounts (id INTEGER PRIMARY KEY, balance INTEGER)")?;
    db.execute("INSERT INTO accounts (id, balance) VALUES (1, 1000)")?;
    db.execute("INSERT INTO accounts (id, balance) VALUES (2, 500)")?;

    // Test successful transaction
    {
        let mut tx = db.begin_transaction()?;

        // Transfer money from account 1 to account 2
        let affected1 = tx.execute("UPDATE accounts SET balance = 800 WHERE id = 1")?;
        assert_eq!(affected1, 1);

        let affected2 = tx.execute("UPDATE accounts SET balance = 700 WHERE id = 2")?;
        assert_eq!(affected2, 1);

        // Verify changes within transaction
        let result = tx.query("SELECT id, balance FROM accounts").unwrap();
        assert_eq!(result.rows().len(), 2);

        // Commit transaction
        tx.commit()?;
    }

    // Verify changes persisted after transaction commit
    let result = db.query("SELECT id, balance FROM accounts").unwrap();
    let row1 = &result.rows()[0];
    let row2 = &result.rows()[1];
    assert_eq!(row1[1], SqlValue::Integer(800));
    assert_eq!(row2[1], SqlValue::Integer(700));

    Ok(())
}

/// Test schema persistence in WASM
#[cfg(target_arch = "wasm32")]
#[wasm_bindgen_test]
fn test_wasm_schema_persistence() -> Result<()> {
    // Create database and table in first session
    {
        let mut db = Database::open("browser://persistent_test")?;
        db.execute("CREATE TABLE persistent_test (id INTEGER PRIMARY KEY, data TEXT)")?;
        db.execute("INSERT INTO persistent_test (id, data) VALUES (1, 'test data')")?;
    }

    // Reopen database and verify schema and data are preserved
    {
        let mut db = Database::open("browser://persistent_test")?;

        // Should be able to query existing data
        let result = db.query("SELECT * FROM persistent_test").unwrap();
        assert_eq!(result.rows().len(), 1);

        let id_pos = result
            .columns()
            .iter()
            .position(|c| c == "id")
            .expect("id column not found");
        let data_pos = result
            .columns()
            .iter()
            .position(|c| c == "data")
            .expect("data column not found");

        assert_eq!(result.rows()[0][id_pos], SqlValue::Integer(1));
        assert_eq!(
            result.rows()[0][data_pos],
            SqlValue::Text("test data".to_string())
        );

        // Should be able to insert new data
        db.execute("INSERT INTO persistent_test (id, data) VALUES (2, 'more data')")?;

        let result = db.query("SELECT * FROM persistent_test").unwrap();
        assert_eq!(result.rows().len(), 2);
    }

    Ok(())
}

/// Test error handling in WASM
#[cfg(target_arch = "wasm32")]
#[wasm_bindgen_test]
fn test_wasm_error_handling() -> Result<()> {
    let mut db = Database::open("browser://error_test")?;

    // Test SQL parse errors
    let result = db.execute("INVALID SQL STATEMENT");
    assert!(result.is_err());

    // Test constraint violations
    db.execute("CREATE TABLE test (id INTEGER PRIMARY KEY, name TEXT NOT NULL)")?;

    // Test using execute() for SELECT - this should fail
    db.execute("INSERT INTO test (id, name) VALUES (1, 'Alice')")?;
    let result = db.execute("SELECT * FROM test");
    assert!(result.is_err());

    // Test the proper way to do SELECT with the new streaming API
    let query_result = db.query("SELECT * FROM test");
    assert!(query_result.is_ok());

    // Test using query() for non-SELECT (should fail)
    let result = db.query("INSERT INTO test (id, name) VALUES (3, 'Bob')");
    assert!(result.is_err());

    Ok(())
}
