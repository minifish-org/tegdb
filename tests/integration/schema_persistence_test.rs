//! Test to verify that table schemas are properly persisted and loaded

#[path = "../helpers/test_helpers.rs"]
mod test_helpers;
use test_helpers::run_with_both_backends;

#[cfg(not(target_arch = "wasm32"))]
use std::fs;
use tegdb::{Database, Result};

#[test]
fn test_schema_persistence_across_database_reopens() -> Result<()> {
    run_with_both_backends(
        "test_schema_persistence_across_database_reopens",
        |db_path| {
            // Clean up any existing database - only for file backend
            #[cfg(not(target_arch = "wasm32"))]
            {
                if db_path.starts_with("file://") {
                    let path_str = db_path.strip_prefix("file://").unwrap();
                    let _ = fs::remove_file(path_str);
                }
            }

            // First session: Create a table
            {
                let mut db = Database::open(db_path).unwrap();

                // Create a table with specific schema
                db.execute(
                    "CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT(32) NOT NULL, age INTEGER)",
                )?;

                // Insert some data to verify the table works
                db.execute("INSERT INTO users (id, name, age) VALUES (1, 'Alice', 30)")?;
                db.execute("INSERT INTO users (id, name, age) VALUES (2, 'Bob', 25)")?;

                // Verify data can be retrieved
                let result = db.query("SELECT * FROM users WHERE age > 25").unwrap();
                assert_eq!(result.rows().len(), 1);
                assert_eq!(result.rows()[0].len(), 3); // id, name, age
            }

            // Second session: Reopen database and verify schema is available
            {
                let mut db = Database::open(db_path).unwrap();

                // The schema should be loaded automatically, so we should be able to:
                // 1. Insert more data
                db.execute("INSERT INTO users (id, name, age) VALUES (3, 'Charlie', 35)")?;

                // 2. Query the data (including old and new data)
                let result = db.query("SELECT name, age FROM users").unwrap();
                assert_eq!(result.rows().len(), 3);

                // Verify we have the correct columns
                assert_eq!(result.columns(), &["name", "age"]);

                // The main point is that the query worked, which means the schema was loaded correctly
                // Let's just verify we have 3 rows of data with 2 columns each
                for row in result.rows() {
                    assert_eq!(row.len(), 2); // name, age
                }
            }

            // Third session: Test that we can still create more tables
            {
                let mut db = Database::open(db_path).unwrap();

                // Create another table
                db.execute(
                    "CREATE TABLE products (id INTEGER PRIMARY KEY, name TEXT(32), price REAL)",
                )?;
                db.execute("INSERT INTO products (id, name, price) VALUES (1, 'Widget', 9.99)")?;

                // Verify both tables work by doing simple queries
                let users_result = db.query("SELECT * FROM users").unwrap();
                let products_result = db.query("SELECT * FROM products").unwrap();

                // Should have 3 users and 1 product
                assert_eq!(users_result.rows().len(), 3);
                assert_eq!(products_result.rows().len(), 1);

                // Verify the columns exist (order might vary due to HashMap iteration)
                let users_columns = users_result.columns();
                assert!(users_columns.contains(&"id".to_string()));
                assert!(users_columns.contains(&"name".to_string()));
                assert!(users_columns.contains(&"age".to_string()));
                assert_eq!(users_columns.len(), 3);

                let products_columns = products_result.columns();
                assert!(products_columns.contains(&"id".to_string()));
                assert!(products_columns.contains(&"name".to_string()));
                assert!(products_columns.contains(&"price".to_string()));
                assert_eq!(products_columns.len(), 3);
            }

            // Clean up - only for file backend
            #[cfg(not(target_arch = "wasm32"))]
            {
                if db_path.starts_with("file://") {
                    let path_str = db_path.strip_prefix("file://").unwrap();
                    let _ = fs::remove_file(path_str);
                }
            }

            Ok(())
        },
    )
}

#[test]
fn test_schema_loading_on_executor_creation() -> Result<()> {
    run_with_both_backends("test_schema_loading_on_executor_creation", |db_path| {
        // Clean up any existing database - only for file backend
        #[cfg(not(target_arch = "wasm32"))]
        {
            if db_path.starts_with("file://") {
                let path_str = db_path.strip_prefix("file://").unwrap();
                let _ = fs::remove_file(path_str);
            }
        }

        // Create database and table
        {
            let mut db = Database::open(db_path).unwrap();
            db.execute("CREATE TABLE items (id INTEGER PRIMARY KEY, description TEXT(32))")?;
            db.execute("INSERT INTO items (id, description) VALUES (1, 'Test Item')")?;
        }

        // Reopen and immediately try to use the table (this should work if schemas are loaded)
        {
            let mut db = Database::open(db_path).unwrap();

            // This should work without any issues if the schema was properly loaded
            let result = db.query("SELECT * FROM items").unwrap();
            assert_eq!(result.rows().len(), 1);

            // Verify columns exist (order might vary due to HashMap iteration)
            let columns = result.columns();
            assert!(columns.contains(&"id".to_string()));
            assert!(columns.contains(&"description".to_string()));
            assert_eq!(columns.len(), 2);
        }

        // Clean up - only for file backend
        #[cfg(not(target_arch = "wasm32"))]
        {
            if db_path.starts_with("file://") {
                let path_str = db_path.strip_prefix("file://").unwrap();
                let _ = fs::remove_file(path_str);
            }
        }

        Ok(())
    })
}
