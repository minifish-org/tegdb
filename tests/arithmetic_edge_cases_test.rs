//! Test edge cases and error handling for arithmetic expressions

use tegdb::Database;

#[test]
fn test_arithmetic_error_handling() {
    let db_path = "test_error_handling.db";
    let _ = std::fs::remove_file(db_path);
    
    let mut db = Database::open(db_path).expect("Failed to open database");
    
    // Create test table
    db.execute("CREATE TABLE test (id INTEGER PRIMARY KEY, value INTEGER)")
        .expect("Failed to create table");
    
    db.execute("INSERT INTO test (id, value) VALUES (1, 10)")
        .expect("Failed to insert data");
    
    // Test division by zero
    let result = db.execute("UPDATE test SET value = value / 0 WHERE id = 1");
    assert!(result.is_err(), "Should fail on division by zero");
    
    // Test reference to non-existent column
    let result = db.execute("UPDATE test SET value = nonexistent + 5 WHERE id = 1");
    assert!(result.is_err(), "Should fail on non-existent column");
    
    // Clean up
    let _ = std::fs::remove_file(db_path);
}

#[test]
fn test_operator_precedence() {
    let db_path = "test_precedence.db";
    let _ = std::fs::remove_file(db_path);
    
    let mut db = Database::open(db_path).expect("Failed to open database");
    
    // Create test table
    db.execute("CREATE TABLE test (id INTEGER PRIMARY KEY, value INTEGER)")
        .expect("Failed to create table");
    
    db.execute("INSERT INTO test (id, value) VALUES (1, 2)")
        .expect("Failed to insert data");
    
    // Test that multiplication has higher precedence than addition
    // 2 + 3 * 4 should be 2 + 12 = 14, not (2 + 3) * 4 = 20
    db.execute("UPDATE test SET value = value + 3 * 4 WHERE id = 1")
        .expect("Failed to update");
    
    let result = db.query("SELECT value FROM test WHERE id = 1")
        .expect("Failed to query").into_query_result().unwrap();
    
    if let Some(row) = result.rows().get(0) {
        if let Some(value) = row.get(0) {
            assert_eq!(*value, tegdb::SqlValue::Integer(14)); // 2 + (3 * 4) = 14
        }
    }
    
    // Reset value
    db.execute("UPDATE test SET value = 2 WHERE id = 1")
        .expect("Failed to reset");
    
    // Test that division has higher precedence than subtraction  
    // 10 - 6 / 2 should be 10 - 3 = 7, not (10 - 6) / 2 = 2
    db.execute("UPDATE test SET value = 10 - 6 / value WHERE id = 1")
        .expect("Failed to update");
    
    let result = db.query("SELECT value FROM test WHERE id = 1")
        .expect("Failed to query").into_query_result().unwrap();
    
    if let Some(row) = result.rows().get(0) {
        if let Some(value) = row.get(0) {
            assert_eq!(*value, tegdb::SqlValue::Integer(7)); // 10 - (6 / 2) = 7
        }
    }
    
    // Clean up
    let _ = std::fs::remove_file(db_path);
}

#[test]
fn test_text_concatenation() {
    let db_path = "test_concat.db";
    let _ = std::fs::remove_file(db_path);
    
    let mut db = Database::open(db_path).expect("Failed to open database");
    
    // Create test table
    db.execute("CREATE TABLE test (id INTEGER PRIMARY KEY, text1 TEXT, text2 TEXT)")
        .expect("Failed to create table");
    
    db.execute("INSERT INTO test (id, text1, text2) VALUES (1, 'Hello', 'World')")
        .expect("Failed to insert data");
    
    // Test text concatenation with +
    db.execute("UPDATE test SET text1 = text1 + ' ' + text2 WHERE id = 1")
        .expect("Failed to update");
    
    let result = db.query("SELECT text1 FROM test WHERE id = 1")
        .expect("Failed to query").into_query_result().unwrap();
    
    if let Some(row) = result.rows().get(0) {
        if let Some(value) = row.get(0) {
            assert_eq!(*value, tegdb::SqlValue::Text("Hello World".to_string()));
        }
    }
    
    // Clean up
    let _ = std::fs::remove_file(db_path);
}
