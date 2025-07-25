//! Integration test for simplified Database API

#[path = "../helpers/test_helpers.rs"]
mod test_helpers;
use test_helpers::run_with_both_backends;

use tegdb::{Database, SqlValue};

#[test]
fn test_simplified_api() {
    run_with_both_backends("simplified_api", |db_path| {
        // Test that we can create database without configuration
        let mut db = Database::open(db_path).expect("Failed to open database");

        // Test DDL
        db.execute("CREATE TABLE test_table (id INTEGER PRIMARY KEY, name TEXT(32), value REAL)")
            .expect("Failed to create table");

        // Test DML
        db.execute("INSERT INTO test_table (id, name, value) VALUES (1, 'test1', 1.5)")
            .expect("Failed to insert data");

        db.execute("INSERT INTO test_table (id, name, value) VALUES (2, 'test2', 2.5)")
            .expect("Failed to insert data");

        // Test query
        let result = db
            .query("SELECT name, value FROM test_table WHERE id = 1")
            .expect("Failed to query data");

        assert_eq!(result.len(), 1);
        assert_eq!(result.columns(), &["name", "value"]);

        let row = &result.rows()[0];
        assert_eq!(row[0], SqlValue::Text("test1".to_string()));
        assert_eq!(row[1], SqlValue::Real(1.5));

        // Test transaction
        let mut tx = db.begin_transaction().expect("Failed to begin transaction");

        tx.execute("UPDATE test_table SET value = 3.0 WHERE id = 1")
            .expect("Failed to update in transaction");

        let tx_result = tx
            .query("SELECT value FROM test_table WHERE id = 1")
            .expect("Failed to query in transaction");

        assert_eq!(tx_result.rows()[0][0], SqlValue::Real(3.0));

        tx.commit().expect("Failed to commit transaction");

        // Verify commit worked
        let final_result = db
            .query("SELECT value FROM test_table WHERE id = 1")
            .expect("Failed to query after commit");

        assert_eq!(final_result.rows()[0][0], SqlValue::Real(3.0));

        Ok(())
    })
    .expect("Test failed");
}

#[test]
fn test_database_without_config() {
    run_with_both_backends("database_without_config", |db_path| {
        // This should work without any configuration
        let mut db = Database::open(db_path).expect("Failed to open database");

        // Should be able to create and use table immediately
        db.execute("CREATE TABLE simple (id INTEGER PRIMARY KEY)")
            .expect("Failed to create table");

        db.execute("INSERT INTO simple (id) VALUES (42)")
            .expect("Failed to insert");

        let result = db.query("SELECT id FROM simple").expect("Failed to query");

        assert_eq!(result.len(), 1);
        assert_eq!(result.rows()[0][0], SqlValue::Integer(42));

        Ok(())
    })
    .expect("Test failed");
}
