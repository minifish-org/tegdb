mod test_helpers {
    include!("../common/test_helpers.rs");
}
use test_helpers::run_with_both_backends;

use tegdb::{Database, Result};

#[test]
fn test_insert_validation() -> Result<()> {
    run_with_both_backends("test_insert_validation", |db_path| {
        let mut db = Database::open(db_path)?;

        // Create test table (TegDB only supports PRIMARY KEY constraints)
        db.execute(
            "CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT(32) NOT NULL, email TEXT(32))",
        )?;

        // Test valid insert
        let result = db.execute(
            "INSERT INTO users (id, name, email) VALUES (1, 'Alice', 'alice@example.com')",
        )?;
        println!("Valid insert result: {result:?}");

        // Test NOT NULL constraint violation
        let result = db.execute("INSERT INTO users (id, email) VALUES (2, 'bob@example.com')");
        assert!(result.is_err(), "Should fail for NOT NULL violation");
        println!("NOT NULL validation: {result:?}");

        // Test PRIMARY KEY constraint violation
        let result = db.execute(
            "INSERT INTO users (id, name, email) VALUES (1, 'Charlie', 'charlie@example.com')",
        );
        assert!(result.is_err(), "Should fail for PRIMARY KEY violation");
        println!("PRIMARY KEY validation: {result:?}");

        // Test unknown column
        let result = db.execute(
            "INSERT INTO users (id, name, unknown_column) VALUES (4, 'David', 'something')",
        );
        assert!(result.is_err(), "Should fail for unknown column");
        println!("Unknown column validation: {result:?}");

        Ok(())
    })
}

#[test]
fn test_update_validation() -> Result<()> {
    run_with_both_backends("test_update_validation", |db_path| {
        let mut db = Database::open(db_path)?;

        // Create test table (TegDB only supports PRIMARY KEY constraints)
        db.execute(
            "CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT(32) NOT NULL, email TEXT(32))",
        )?;

        // Insert test data
        db.execute("INSERT INTO users (id, name, email) VALUES (1, 'Alice', 'alice@example.com')")?;
        db.execute("INSERT INTO users (id, name, email) VALUES (2, 'Bob', 'bob@example.com')")?;

        // Test valid update
        let result = db.execute("UPDATE users SET name = 'Alice Updated' WHERE id = 1");
        assert!(result.is_ok(), "Valid update should succeed: {result:?}");
        println!("Valid update result: {result:?}");

        // Test NOT NULL constraint violation
        let result = db.execute("UPDATE users SET name = NULL WHERE id = 1");
        assert!(result.is_err(), "Should fail for NOT NULL violation");
        println!("NOT NULL update validation: {result:?}");

        // Test unknown column in assignment
        let result = db.execute("UPDATE users SET unknown_column = 'oops' WHERE id = 1");
        assert!(result.is_err(), "Should fail for unknown column in UPDATE");
        println!("Unknown column update validation: {result:?}");

        // Test PRIMARY KEY constraint violation (updating to existing PK)
        let result = db.execute("UPDATE users SET id = 2 WHERE id = 1");
        assert!(result.is_err(), "Should fail for PRIMARY KEY violation");
        println!("PRIMARY KEY update validation: {result:?}");

        Ok(())
    })
}

#[test]
fn test_select_memory_optimization() -> Result<()> {
    run_with_both_backends("test_select_memory_optimization", |db_path| {
        let mut db = Database::open(db_path)?;

        // Create test table (TegDB only supports PRIMARY KEY constraints)
        db.execute(
            "CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT(32) NOT NULL, email TEXT(32))",
        )?;

        // Insert test data
        for i in 1..=100 {
            db.execute(&format!(
                "INSERT INTO users (id, name, email) VALUES ({i}, 'User{i}', 'user{i}@example.com')"
            ))?;
        }

        // Test LIMIT optimization (should only process limited rows)
        // Now using the proper streaming API
        let result = db.query("SELECT * FROM users LIMIT 5")?;
        let count = result.rows().len();
        println!("Limited select result: {count} rows (streaming)");
        assert!(count <= 5);

        // Test WHERE optimization (should filter early)
        let result = db.query("SELECT * FROM users WHERE id = 42")?;
        let count = result.rows().len();
        println!("Filtered select result: {count} rows (streaming)");

        // Test combined LIMIT and WHERE
        let result = db.query("SELECT * FROM users WHERE id > 50 LIMIT 3")?;
        let count = result.rows().len();
        println!("Combined optimization result: {count} rows (streaming)");
        assert!(count <= 3);

        Ok(())
    })
}
