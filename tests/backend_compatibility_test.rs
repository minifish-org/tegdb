//! Test to demonstrate running tests with both file and browser backends
//!
//! This module shows how to use the test helpers to run the same test logic
//! with both the file backend (native) and browser backend (WASM).
//!
//! ## How to use:
//!
//! 1. Import the test helpers:
//!    ```rust
//!    mod test_helpers;
//!    use crate::test_helpers::run_with_both_backends;
//!    ```
//!
//! 2. Wrap your test logic in a closure:
//!    ```rust
//!    #[test]
//!    fn my_test() -> Result<()> {
//!         run_with_both_backends("my_test_name", |db_path| {
//!             let mut db = Database::open(db_path)?;
//!             // Your test logic here...
//!             Ok(())
//!         })
//!     }
//!    ```
//!
//! 3. The test will automatically run with both backends when available.
//!
//! ## Converting existing tests:
//!
//! To convert an existing test that uses `NamedTempFile`, simply:
//! 1. Replace the temp file creation with `run_with_both_backends`
//! 2. Move the test logic into the closure
//! 3. Use the provided `db_path` parameter
//!
//! Example conversion:
//! ```rust
//! // Before:
//! fn test_something() -> Result<()> {
//!     let temp_file = NamedTempFile::new().expect("Failed to create temp file");
//!     let db_path = temp_file.path();
//!     let mut db = Database::open(&format!("file://{}", db_path.display()))?;
//!     // test logic...
//!     Ok(())
//! }
//!
//! // After:
//! fn test_something() -> Result<()> {
//!     run_with_both_backends("test_something", |db_path| {
//!         let mut db = Database::open(&format!("file://{}", db_path.display()))?;
//!         // test logic...
//!         Ok(())
//!     })
//! }
//! ```

use tegdb::{Database, Result, SqlValue};
mod test_helpers;
use crate::test_helpers::run_with_both_backends;

#[test]
fn test_basic_operations_both_backends() -> Result<()> {
    run_with_both_backends("basic_operations", |db_path| {
        let mut db = Database::open(db_path)?;

        // Test CREATE TABLE
        let affected = db.execute(
            "CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT NOT NULL, age INTEGER)",
        )?;
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
        assert_eq!(affected, 0); // No rows deleted since Alice is now 31

        // Verify final state
        let result = db
            .query("SELECT name, age FROM users ORDER BY name")
            .unwrap();
        assert_eq!(result.rows().len(), 1); // Alice remaining
        assert_eq!(result.rows()[0][0], SqlValue::Text("Alice".to_string()));
        assert_eq!(result.rows()[0][1], SqlValue::Integer(31));

        Ok(())
    })
}

#[test]
fn test_transactions_both_backends() -> Result<()> {
    run_with_both_backends("transactions", |db_path| {
        let mut db = Database::open(db_path)?;

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
            let result = tx
                .query("SELECT id, balance FROM accounts ORDER BY id")
                .unwrap();
            assert_eq!(result.rows().len(), 2);

            // Check balances in transaction
            let row1 = &result.rows()[0];
            let row2 = &result.rows()[1];
            assert_eq!(row1[1], SqlValue::Integer(800));
            assert_eq!(row2[1], SqlValue::Integer(700));

            // Commit transaction
            tx.commit()?;
        }

        // Verify changes persisted after transaction commit
        let result = db
            .query("SELECT id, balance FROM accounts ORDER BY id")
            .unwrap();
        let row1 = &result.rows()[0];
        let row2 = &result.rows()[1];
        assert_eq!(row1[1], SqlValue::Integer(800));
        assert_eq!(row2[1], SqlValue::Integer(700));

        Ok(())
    })
}

#[test]
fn test_data_types_both_backends() -> Result<()> {
    run_with_both_backends("data_types", |db_path| {
        let mut db = Database::open(db_path)?;

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
        db.execute("INSERT INTO test_types (id, text_col, int_col, real_col, null_col) VALUES (2, 'test', -100, -2.5, NULL)")?;
        db.execute("INSERT INTO test_types (id, text_col, int_col, real_col, null_col) VALUES (3, 'world', 0, 0.0, 'not null')")?;

        let result = db.query("SELECT * FROM test_types ORDER BY id").unwrap();
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
    })
}

#[test]
fn test_schema_persistence_both_backends() -> Result<()> {
    run_with_both_backends("schema_persistence", |db_path| {
        // Create database and table in first session
        {
            let mut db = Database::open(db_path)?;
            db.execute("CREATE TABLE persistent_test (id INTEGER PRIMARY KEY, data TEXT)")?;
            db.execute("INSERT INTO persistent_test (id, data) VALUES (1, 'test data')")?;
        }

        // Reopen database and verify schema and data are preserved
        {
            let mut db = Database::open(db_path)?;

            // Should be able to query existing data
            let result = db.query("SELECT * FROM persistent_test").unwrap();
            assert_eq!(result.rows().len(), 1);

            // Find column positions
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
    })
}

/// Example of converting an existing test from database_tests.rs
/// This shows how easy it is to make existing tests run with both backends
#[test]
fn test_converted_from_existing_test() -> Result<()> {
    run_with_both_backends("converted_test", |db_path| {
        let mut db = Database::open(db_path)?;

        // This is the same logic as test_database_open_and_basic_operations from database_tests.rs
        // but now it runs with both backends automatically!

        // Test CREATE TABLE
        let affected = db.execute(
            "CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT NOT NULL, age INTEGER)",
        )?;
        assert_eq!(affected, 0); // CREATE TABLE returns 0 affected rows

        // Test INSERT
        let affected = db.execute("INSERT INTO users (id, name, age) VALUES (1, 'Alice', 30)")?;
        assert_eq!(affected, 1);

        // Test multiple INSERT
        let affected = db.execute("INSERT INTO users (id, name, age) VALUES (2, 'Bob', 25)")?;
        assert_eq!(affected, 1);
        let affected = db.execute("INSERT INTO users (id, name, age) VALUES (3, 'Carol', 35)")?;
        assert_eq!(affected, 1);

        // Test SELECT
        let result = db.query("SELECT * FROM users").unwrap();
        // Column order may vary, so just check that we have the right columns
        assert!(result.columns().contains(&"id".to_string()));
        assert!(result.columns().contains(&"name".to_string()));
        assert!(result.columns().contains(&"age".to_string()));
        assert_eq!(result.columns().len(), 3);
        assert_eq!(result.rows().len(), 3);

        // Test UPDATE
        let affected = db.execute("UPDATE users SET age = 31 WHERE name = 'Alice'")?;
        assert_eq!(affected, 1);

        // Test DELETE
        let affected = db.execute("DELETE FROM users WHERE age < 30")?;
        assert_eq!(affected, 1); // Should delete Bob (age 25)

        // Verify final state
        let result = db
            .query("SELECT name, age FROM users ORDER BY name")
            .unwrap();
        assert_eq!(result.rows().len(), 2); // Alice and Carol remaining

        Ok(())
    })
}
