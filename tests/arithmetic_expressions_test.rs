//! Test arithmetic expressions in UPDATE statements

use tegdb::Database;

#[test]
fn test_arithmetic_expressions_in_update() {
    let db_path = "test_arithmetic.db";
    let _ = std::fs::remove_file(db_path);
    
    let mut db = Database::open(db_path).expect("Failed to open database");
    
    // Create test table
    db.execute("CREATE TABLE test_table (id INTEGER PRIMARY KEY, value INTEGER, score REAL)")
        .expect("Failed to create table");
    
    // Insert test data
    db.execute("INSERT INTO test_table (id, value, score) VALUES (1, 10, 5.5)")
        .expect("Failed to insert data");
    
    db.execute("INSERT INTO test_table (id, value, score) VALUES (2, 20, 7.2)")
        .expect("Failed to insert data");
    
    // Test simple arithmetic: value + 5
    db.execute("UPDATE test_table SET value = value + 5 WHERE id = 1")
        .expect("Failed to update with addition");
    
    let result = db.query("SELECT value FROM test_table WHERE id = 1")
        .expect("Failed to query").into_query_result().unwrap();
    
    if let Some(row) = result.rows().get(0) {
        if let Some(value) = row.get(0) {
            assert_eq!(*value, tegdb::SqlValue::Integer(15)); // 10 + 5 = 15
        }
    }
    
    // Test subtraction: value - 3
    db.execute("UPDATE test_table SET value = value - 3 WHERE id = 2")
        .expect("Failed to update with subtraction");
    
    let result = db.query("SELECT value FROM test_table WHERE id = 2")
        .expect("Failed to query").into_query_result().unwrap();
    
    if let Some(row) = result.rows().get(0) {
        if let Some(value) = row.get(0) {
            assert_eq!(*value, tegdb::SqlValue::Integer(17)); // 20 - 3 = 17
        }
    }
    
    // Test multiplication: score * 2
    db.execute("UPDATE test_table SET score = score * 2 WHERE id = 1")
        .expect("Failed to update with multiplication");
    
    let result = db.query("SELECT score FROM test_table WHERE id = 1")
        .expect("Failed to query").into_query_result().unwrap();
    
    if let Some(row) = result.rows().get(0) {
        if let Some(value) = row.get(0) {
            assert_eq!(*value, tegdb::SqlValue::Real(11.0)); // 5.5 * 2 = 11.0
        }
    }
    
    // Test division: score / 2
    db.execute("UPDATE test_table SET score = score / 2 WHERE id = 2")
        .expect("Failed to update with division");
    
    let result = db.query("SELECT score FROM test_table WHERE id = 2")
        .expect("Failed to query").into_query_result().unwrap();
    
    if let Some(row) = result.rows().get(0) {
        if let Some(value) = row.get(0) {
            assert_eq!(*value, tegdb::SqlValue::Real(3.6)); // 7.2 / 2 = 3.6
        }
    }
    
    // Test complex expression: value + 10 * 2
    db.execute("UPDATE test_table SET value = value + 10 * 2 WHERE id = 1")
        .expect("Failed to update with complex expression");
    
    let result = db.query("SELECT value FROM test_table WHERE id = 1")
        .expect("Failed to query").into_query_result().unwrap();
    
    if let Some(row) = result.rows().get(0) {
        if let Some(value) = row.get(0) {
            assert_eq!(*value, tegdb::SqlValue::Integer(35)); // 15 + (10 * 2) = 35
        }
    }
    
    // Clean up
    let _ = std::fs::remove_file(db_path);
}

#[test]
fn test_arithmetic_expression_parsing() {
    // Test that the parser can handle the expressions
    use tegdb::SqlValue;
    
    // This requires accessing the parser directly, which needs dev features
    // For now, we'll test through the database API which exercises the parser
    let db_path = "test_parsing.db";
    let _ = std::fs::remove_file(db_path);
    
    let mut db = Database::open(db_path).expect("Failed to open database");
    
    db.execute("CREATE TABLE test (id INTEGER PRIMARY KEY, a INTEGER, b INTEGER)")
        .expect("Failed to create table");
    
    db.execute("INSERT INTO test (id, a, b) VALUES (1, 100, 25)")
        .expect("Failed to insert");
    
    // Test that complex expressions are parsed and evaluated correctly
    let tests = vec![
        ("UPDATE test SET a = a + b WHERE id = 1", 125),      // 100 + 25
        ("UPDATE test SET a = a - b WHERE id = 1", 100),      // 125 - 25  
        ("UPDATE test SET a = a * 2 WHERE id = 1", 200),      // 100 * 2
        ("UPDATE test SET a = a / 4 WHERE id = 1", 50),       // 200 / 4
    ];
    
    for (sql, expected) in tests {
        db.execute(sql).expect(&format!("Failed to execute: {}", sql));
        
        let result = db.query("SELECT a FROM test WHERE id = 1")
            .expect("Failed to query").into_query_result().unwrap();
        
        if let Some(row) = result.rows().get(0) {
            if let Some(value) = row.get(0) {
                assert_eq!(*value, SqlValue::Integer(expected), "Failed for SQL: {}", sql);
            }
        }
    }
    
    // Clean up
    let _ = std::fs::remove_file(db_path);
}
