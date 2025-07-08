use tegdb::{Database, Result};
use tempfile::TempDir;

fn setup_test_db() -> Result<(Database, TempDir)> {
    let temp_dir = TempDir::new().unwrap();
    let path = temp_dir.path().join("test.db");

    let mut db = Database::open(&format!("file://{}", path.display()))?;

    // Create test table
    db.execute(
        "CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT NOT NULL, email TEXT UNIQUE)",
    )?;

    Ok((db, temp_dir))
}

#[test]
fn test_insert_validation() -> Result<()> {
    let (mut db, _temp_dir) = setup_test_db()?;

    // Test valid insert
    let result =
        db.execute("INSERT INTO users (id, name, email) VALUES (1, 'Alice', 'alice@example.com')")?;
    println!("Valid insert result: {result:?}");

    // Test NOT NULL constraint violation
    let result = db.execute("INSERT INTO users (id, email) VALUES (2, 'bob@example.com')");
    assert!(result.is_err(), "Should fail for NOT NULL violation");
    println!("NOT NULL validation: {result:?}");

    // Test UNIQUE constraint violation
    let result = db
        .execute("INSERT INTO users (id, name, email) VALUES (3, 'Charlie', 'alice@example.com')");
    assert!(result.is_err(), "Should fail for UNIQUE violation");
    println!("UNIQUE validation: {result:?}");

    // Test unknown column
    let result =
        db.execute("INSERT INTO users (id, name, unknown_column) VALUES (4, 'David', 'something')");
    assert!(result.is_err(), "Should fail for unknown column");
    println!("Unknown column validation: {result:?}");

    Ok(())
}

#[test]
fn test_update_validation() -> Result<()> {
    let (mut db, _temp_dir) = setup_test_db()?;

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

    // Test UNIQUE constraint violation
    let result = db.execute("UPDATE users SET email = 'bob@example.com' WHERE id = 1");
    assert!(result.is_err(), "Should fail for UNIQUE violation");
    println!("UNIQUE update validation: {result:?}");

    Ok(())
}

#[test]
fn test_select_memory_optimization() -> Result<()> {
    let (mut db, _temp_dir) = setup_test_db()?;

    // Insert test data
    for i in 1..=100 {
        db.execute(&format!(
            "INSERT INTO users (id, name, email) VALUES ({i}, 'User{i}', 'user{i}@example.com')"
        ))?;
    }

    // Test LIMIT optimization (should only process limited rows)
    // Now using the proper streaming API
    let result = db.query("SELECT * FROM users LIMIT 5")?;
    let count = result.len();
    println!("Limited select result: {count} rows (streaming)");
    assert!(count <= 5);

    // Test WHERE optimization (should filter early)
    let result = db.query("SELECT * FROM users WHERE id = 42")?;
    let count = result.len();
    println!("Filtered select result: {count} rows (streaming)");

    // Test combined LIMIT and WHERE
    let result = db.query("SELECT * FROM users WHERE id > 50 LIMIT 3")?;
    let count = result.len();
    println!("Combined optimization result: {count} rows (streaming)");
    assert!(count <= 3);

    Ok(())
}
