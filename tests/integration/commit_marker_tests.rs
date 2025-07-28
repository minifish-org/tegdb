#[path = "../helpers/test_helpers.rs"]
mod test_helpers;
use test_helpers::run_with_both_backends;

use tegdb::{Database, Result, SqlValue};

#[test]
fn test_commit_marker_and_crash_recovery() -> Result<()> {
    run_with_both_backends("test_commit_marker_and_crash_recovery", |db_path| {
        let mut db = Database::open(db_path)?;

        // Create a test table
        db.execute("CREATE TABLE test_data (key TEXT(32) PRIMARY KEY, value TEXT(32))")?;

        // Begin a transaction and commit it
        {
            let mut tx = db.begin_transaction()?;
            tx.execute("INSERT INTO test_data (key, value) VALUES ('key1', 'value1')")?;
            tx.execute("INSERT INTO test_data (key, value) VALUES ('key2', 'value2')")?;
            tx.commit()?;
        }

        // Begin another transaction but don't commit (simulate crash)
        {
            let mut tx = db.begin_transaction()?;
            tx.execute("INSERT INTO test_data (key, value) VALUES ('key3', 'value3')")?;
            // Don't commit - this should be rolled back on recovery
        }

        // Drop the first database instance to release the file lock
        drop(db);

        // Reopen the database to simulate crash recovery
        let mut db2 = Database::open(db_path)?;

        // Check that committed data is still there
        let result1 = db2.query("SELECT value FROM test_data WHERE key = 'key1'")?;
        assert_eq!(result1.rows().len(), 1);
        assert_eq!(result1.rows()[0][0], SqlValue::Text("value1".to_string()));

        let result2 = db2.query("SELECT value FROM test_data WHERE key = 'key2'")?;
        assert_eq!(result2.rows().len(), 1);
        assert_eq!(result2.rows()[0][0], SqlValue::Text("value2".to_string()));

        // Check that uncommitted data was rolled back
        let result3 = db2.query("SELECT value FROM test_data WHERE key = 'key3'")?;
        assert_eq!(result3.rows().len(), 0); // Should be empty

        Ok(())
    })
}

#[test]
fn test_multiple_transactions_with_commit_markers() -> Result<()> {
    run_with_both_backends(
        "test_multiple_transactions_with_commit_markers",
        |db_path| {
            let mut db = Database::open(db_path)?;

            // Create a test table
            db.execute("CREATE TABLE test_data (key TEXT(32) PRIMARY KEY, value TEXT(32))")?;

            // Transaction 1: committed
            {
                let mut tx = db.begin_transaction()?;
                tx.execute("INSERT INTO test_data (key, value) VALUES ('tx1_key', 'tx1_value')")?;
                tx.commit()?;
            }

            // Transaction 2: committed
            {
                let mut tx = db.begin_transaction()?;
                tx.execute("INSERT INTO test_data (key, value) VALUES ('tx2_key', 'tx2_value')")?;
                tx.commit()?;
            }

            // Verify that both transactions were committed by checking their data
            let result1 = db.query("SELECT value FROM test_data WHERE key = 'tx1_key'")?;
            assert_eq!(result1.rows().len(), 1);
            assert_eq!(
                result1.rows()[0][0],
                SqlValue::Text("tx1_value".to_string())
            );

            let result2 = db.query("SELECT value FROM test_data WHERE key = 'tx2_key'")?;
            assert_eq!(result2.rows().len(), 1);
            assert_eq!(
                result2.rows()[0][0],
                SqlValue::Text("tx2_value".to_string())
            );

            Ok(())
        },
    )
}

#[test]
fn test_create_and_drop_index() {
    use tegdb::Database;
    let db_path = "file:///tmp/test_create_and_drop_index.db";
    let _ = std::fs::remove_file("/tmp/test_create_and_drop_index.db");
    let mut db = Database::open(db_path).unwrap();

    db.execute("CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT(50))").unwrap();
    db.execute("CREATE INDEX idx_name ON users(name)").unwrap();
    // Duplicate index name should fail
    assert!(db.execute("CREATE INDEX idx_name ON users(name)").is_err());
    // Non-existent table should fail
    assert!(db.execute("CREATE INDEX idx2 ON no_table(name)").is_err());
    // Non-existent column should fail
    assert!(db.execute("CREATE INDEX idx3 ON users(no_col)").is_err());
    // Drop index
    db.execute("DROP INDEX idx_name").unwrap();
    // Drop non-existent index should fail
    assert!(db.execute("DROP INDEX idx_name").is_err());
    // Index should persist after reopen
    db.execute("CREATE INDEX idx_persist ON users(name)").unwrap();
    drop(db);
    let mut db = Database::open(db_path).unwrap();
    // Drop after reload
    db.execute("DROP INDEX idx_persist").unwrap();
    let _ = std::fs::remove_file("/tmp/test_create_and_drop_index.db");
}

#[test]
fn test_index_scan_usage() {
    use tegdb::Database;
    let db_path = "file:///tmp/test_index_scan_usage.db";
    let _ = std::fs::remove_file("/tmp/test_index_scan_usage.db");
    let mut db = Database::open(db_path).unwrap();

    // Create table and index
    db.execute("CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT(50), email TEXT(100))").unwrap();
    db.execute("CREATE INDEX idx_name ON users(name)").unwrap();
    
    // Insert test data
    db.execute("INSERT INTO users (id, name, email) VALUES (1, 'alice', 'alice@example.com')").unwrap();
    db.execute("INSERT INTO users (id, name, email) VALUES (2, 'bob', 'bob@example.com')").unwrap();
    db.execute("INSERT INTO users (id, name, email) VALUES (3, 'alice', 'alice2@example.com')").unwrap();
    
    // Query that should use the index
    let result = db.query("SELECT * FROM users WHERE name = 'alice'").unwrap();
    assert_eq!(result.rows().len(), 2); // Should find both alice entries
    
    // Query that should not use the index (no WHERE clause)
    let result = db.query("SELECT * FROM users").unwrap();
    assert_eq!(result.rows().len(), 3); // Should find all entries
    
    // Query on non-indexed column should not use index
    let result = db.query("SELECT * FROM users WHERE email = 'bob@example.com'").unwrap();
    assert_eq!(result.rows().len(), 1); // Should find bob
    
    let _ = std::fs::remove_file("/tmp/test_index_scan_usage.db");
}

#[test]
fn test_basic_table_operations() {
    use tegdb::Database;
    let db_path = "file:///tmp/test_basic_table_operations.db";
    let _ = std::fs::remove_file("/tmp/test_basic_table_operations.db");
    let mut db = Database::open(db_path).unwrap();

    // Create table
    db.execute("CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT(50), email TEXT(100))").unwrap();
    
    // Insert test data
    db.execute("INSERT INTO users (id, name, email) VALUES (1, 'alice', 'alice@example.com')").unwrap();
    db.execute("INSERT INTO users (id, name, email) VALUES (2, 'bob', 'bob@example.com')").unwrap();
    
    // Query without WHERE clause
    let result = db.query("SELECT * FROM users").unwrap();
    assert_eq!(result.rows().len(), 2);
    
    // Query with WHERE clause
    let result = db.query("SELECT * FROM users WHERE name = 'alice'").unwrap();
    assert_eq!(result.rows().len(), 1);
    
    let _ = std::fs::remove_file("/tmp/test_basic_table_operations.db");
}

#[test]
fn test_integer_only_table() {
    use tegdb::Database;
    let db_path = "file:///tmp/test_integer_only_table.db";
    let _ = std::fs::remove_file("/tmp/test_integer_only_table.db");
    let mut db = Database::open(db_path).unwrap();

    // Create table with only INTEGER columns
    db.execute("CREATE TABLE numbers (id INTEGER PRIMARY KEY, value INTEGER)").unwrap();
    
    // Insert test data
    db.execute("INSERT INTO numbers (id, value) VALUES (1, 10)").unwrap();
    db.execute("INSERT INTO numbers (id, value) VALUES (2, 20)").unwrap();
    
    // Query without WHERE clause
    let result = db.query("SELECT * FROM numbers").unwrap();
    assert_eq!(result.rows().len(), 2);
    
    // Query with WHERE clause
    let result = db.query("SELECT * FROM numbers WHERE value = 10").unwrap();
    assert_eq!(result.rows().len(), 1);
    
    let _ = std::fs::remove_file("/tmp/test_integer_only_table.db");
}

#[test]
fn test_order_by_functionality() {
    use tegdb::Database;
    let db_path = "file:///tmp/test_order_by_functionality.db";
    let _ = std::fs::remove_file("/tmp/test_order_by_functionality.db");
    let mut db = Database::open(db_path).unwrap();

    // Create table
    db.execute("CREATE TABLE numbers (id INTEGER PRIMARY KEY, value INTEGER, name TEXT(50))").unwrap();
    
    // Insert test data
    db.execute("INSERT INTO numbers (id, value, name) VALUES (1, 30, 'c')").unwrap();
    db.execute("INSERT INTO numbers (id, value, name) VALUES (2, 10, 'a')").unwrap();
    db.execute("INSERT INTO numbers (id, value, name) VALUES (3, 20, 'b')").unwrap();
    
    // Test ORDER BY ascending
    let result = db.query("SELECT * FROM numbers ORDER BY value ASC").unwrap();
    let rows = result.rows();
    assert_eq!(rows.len(), 3);
    // Should be ordered by value: 10, 20, 30
    assert_eq!(rows[0][1], tegdb::parser::SqlValue::Integer(10));
    assert_eq!(rows[1][1], tegdb::parser::SqlValue::Integer(20));
    assert_eq!(rows[2][1], tegdb::parser::SqlValue::Integer(30));
    
    // Test ORDER BY descending
    let result = db.query("SELECT * FROM numbers ORDER BY value DESC").unwrap();
    let rows = result.rows();
    assert_eq!(rows.len(), 3);
    // Should be ordered by value: 30, 20, 10
    assert_eq!(rows[0][1], tegdb::parser::SqlValue::Integer(30));
    assert_eq!(rows[1][1], tegdb::parser::SqlValue::Integer(20));
    assert_eq!(rows[2][1], tegdb::parser::SqlValue::Integer(10));
    
    // Test ORDER BY text column
    let result = db.query("SELECT * FROM numbers ORDER BY name ASC").unwrap();
    let rows = result.rows();
    assert_eq!(rows.len(), 3);
    // Should be ordered by name: 'a', 'b', 'c'
    assert_eq!(rows[0][2], tegdb::parser::SqlValue::Text("a".to_string()));
    assert_eq!(rows[1][2], tegdb::parser::SqlValue::Text("b".to_string()));
    assert_eq!(rows[2][2], tegdb::parser::SqlValue::Text("c".to_string()));
    
    let _ = std::fs::remove_file("/tmp/test_order_by_functionality.db");
}
