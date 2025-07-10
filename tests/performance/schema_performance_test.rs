//! Performance test to demonstrate the efficiency improvement of schema loading

#[allow(clippy::duplicate_mod)]
#[path = "../helpers/test_helpers.rs"]
mod test_helpers;
use test_helpers::run_with_both_backends;

use std::time::Instant;
use tegdb::{Database, Result};

#[cfg(not(target_arch = "wasm32"))]
fn remove_file_if_file_backend(db_path: &str) {
    if db_path.starts_with("file://") {
        let path_str = db_path.strip_prefix("file://").unwrap();
        let _ = std::fs::remove_file(path_str);
    }
}

#[test]
fn test_schema_loading_performance() -> Result<()> {
    run_with_both_backends("test_schema_loading_performance", |db_path| {
        // Clean up any existing database
        #[cfg(not(target_arch = "wasm32"))]
        {
            remove_file_if_file_backend(db_path);
        }

        // Create database with multiple tables
        {
            let mut db = Database::open(db_path)?;

            // Create several tables to make schema loading noticeable
            for i in 0..5 {
                db.execute(&format!(
                    "CREATE TABLE table_{i} (id INTEGER PRIMARY KEY, name TEXT NOT NULL, value INTEGER)"
                ))?;

                // Add minimal data to each table
                db.execute(&format!(
                    "INSERT INTO table_{} (id, name, value) VALUES ({}, 'item_{}', {})",
                    i, 1, 1, 10
                ))?;
            }
        }

        // Measure performance of multiple database operations
        {
            let start = Instant::now();
            let mut db = Database::open(db_path)?;
            let schema_load_time = start.elapsed();

            println!("Schema loading time: {schema_load_time:?}");

            // Perform multiple operations that would have triggered schema loading
            // in the old implementation
            let start = Instant::now();
            for i in 0..5 {
                let result = db
                    .query(&format!("SELECT * FROM table_{i} LIMIT 1"))
                    .unwrap();
                assert!(result.rows().len() <= 1);
            }
            let query_time = start.elapsed();

            println!("Time for 5 queries: {query_time:?}");

            // With the new implementation, schema loading happens once at database open,
            // not for each executor/query
            assert!(schema_load_time.as_millis() < 1000); // Should be reasonable
            assert!(query_time.as_millis() < 500); // Should also be fast
        }

        // Clean up
        #[cfg(not(target_arch = "wasm32"))]
        {
            remove_file_if_file_backend(db_path);
        }

        Ok(())
    })
}

#[test]
fn test_schema_sharing_across_operations() -> Result<()> {
    run_with_both_backends("test_schema_sharing_across_operations", |db_path| {
        // Clean up any existing database
        #[cfg(not(target_arch = "wasm32"))]
        {
            remove_file_if_file_backend(db_path);
        }

        // Test that schemas are properly shared and updated
        {
            let mut db = Database::open(db_path)?;

            // Create a table
            db.execute("CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)")?;

            // Insert data
            db.execute("INSERT INTO users (id, name) VALUES (1, 'Alice')")?;

            // Query should work immediately (schemas are shared)
            let result = db.query("SELECT * FROM users").unwrap();
            assert_eq!(result.rows().len(), 1);

            // Create another table in the same database instance
            db.execute("CREATE TABLE products (id INTEGER PRIMARY KEY, name TEXT, price REAL)")?;

            // Both tables should work
            let users_result = db.query("SELECT * FROM users").unwrap();
            let products_result = db.query("SELECT * FROM products").unwrap();

            assert_eq!(users_result.rows().len(), 1);
            assert_eq!(products_result.rows().len(), 0);
        }

        // Verify persistence
        {
            let mut db = Database::open(db_path)?;

            // Both tables should still be available
            let users_result = db.query("SELECT * FROM users").unwrap();
            let products_result = db.query("SELECT * FROM products").unwrap();

            assert_eq!(users_result.rows().len(), 1);
            assert_eq!(products_result.rows().len(), 0);
        }

        // Clean up
        #[cfg(not(target_arch = "wasm32"))]
        {
            remove_file_if_file_backend(db_path);
        }

        Ok(())
    })
}
