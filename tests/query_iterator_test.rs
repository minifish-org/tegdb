use tegdb::Database;
use tegdb::SqlValue;
use tempfile::NamedTempFile;

#[test]
fn test_query_iterator_basic_functionality() {
    let temp_file = NamedTempFile::new().expect("Failed to create temp file");
    let db_path = temp_file.path();
    let mut db = Database::open(&format!("file://{}", db_path.display())).unwrap();

    // Setup test data
    db.execute("CREATE TABLE test_table (id INTEGER PRIMARY KEY, name TEXT, value INTEGER)")
        .unwrap();
    db.execute("INSERT INTO test_table (id, name, value) VALUES (1, 'first', 100)")
        .unwrap();
    db.execute("INSERT INTO test_table (id, name, value) VALUES (2, 'second', 200)")
        .unwrap();
    db.execute("INSERT INTO test_table (id, name, value) VALUES (3, 'third', 300)")
        .unwrap();

    // Test iterator functionality
    let query_result = db.query("SELECT * FROM test_table ORDER BY id").unwrap();

    // Check columns
    assert_eq!(query_result.columns(), &["id", "name", "value"]);

    // Collect all rows
    let rows = query_result.rows();
    assert_eq!(rows.len(), 3);

    assert_eq!(
        rows[0],
        vec![
            SqlValue::Integer(1),
            SqlValue::Text("first".to_string()),
            SqlValue::Integer(100)
        ]
    );
    assert_eq!(
        rows[1],
        vec![
            SqlValue::Integer(2),
            SqlValue::Text("second".to_string()),
            SqlValue::Integer(200)
        ]
    );
    assert_eq!(
        rows[2],
        vec![
            SqlValue::Integer(3),
            SqlValue::Text("third".to_string()),
            SqlValue::Integer(300)
        ]
    );
}

#[test]
fn test_query_iterator_streaming() {
    let temp_file = NamedTempFile::new().expect("Failed to create temp file");
    let db_path = temp_file.path();
    let mut db = Database::open(&format!("file://{}", db_path.display())).unwrap();

    // Setup test data
    db.execute("CREATE TABLE streaming_test (id INTEGER PRIMARY KEY, data TEXT)")
        .unwrap();
    for i in 1..=5 {
        db.execute(&format!(
            "INSERT INTO streaming_test (id, data) VALUES ({i}, 'data_{i}')"
        ))
        .unwrap();
    }

    // Test streaming iteration
    let query = db
        .query("SELECT * FROM streaming_test ORDER BY id")
        .unwrap();

    let mut count = 0;
    let mut collected_rows = Vec::new();

    for row_result in query {
        let row = row_result.unwrap();
        collected_rows.push(row);
        count += 1;

        // Test early termination
        if count >= 3 {
            break;
        }
    }

    assert_eq!(count, 3);
    assert_eq!(collected_rows.len(), 3);

    // Verify first three rows
    assert_eq!(
        collected_rows[0],
        vec![SqlValue::Integer(1), SqlValue::Text("data_1".to_string())]
    );
    assert_eq!(
        collected_rows[1],
        vec![SqlValue::Integer(2), SqlValue::Text("data_2".to_string())]
    );
    assert_eq!(
        collected_rows[2],
        vec![SqlValue::Integer(3), SqlValue::Text("data_3".to_string())]
    );
}

#[test]
fn test_query_iterator_backward_compatibility() {
    let temp_file = NamedTempFile::new().expect("Failed to create temp file");
    let db_path = temp_file.path();
    let mut db = Database::open(&format!("file://{}", db_path.display())).unwrap();

    // Setup test data
    db.execute("CREATE TABLE compat_test (id INTEGER PRIMARY KEY, name TEXT)")
        .unwrap();
    db.execute("INSERT INTO compat_test (id, name) VALUES (1, 'Alice')")
        .unwrap();
    db.execute("INSERT INTO compat_test (id, name) VALUES (2, 'Bob')")
        .unwrap();

    // Test conversion to old QueryResult format
    let query_result = db.query("SELECT * FROM compat_test ORDER BY id").unwrap();

    // Verify compatibility with old API
    assert_eq!(query_result.columns(), &["id", "name"]);
    assert_eq!(query_result.len(), 2);
    assert!(!query_result.is_empty());

    let rows = query_result.rows();
    assert_eq!(
        rows[0],
        vec![SqlValue::Integer(1), SqlValue::Text("Alice".to_string())]
    );
    assert_eq!(
        rows[1],
        vec![SqlValue::Integer(2), SqlValue::Text("Bob".to_string())]
    );
}

#[test]
fn test_query_iterator_empty_result() {
    let temp_file = NamedTempFile::new().expect("Failed to create temp file");
    let db_path = temp_file.path();
    let mut db = Database::open(&format!("file://{}", db_path.display())).unwrap();

    // Setup test data
    db.execute("CREATE TABLE empty_test (id INTEGER PRIMARY KEY, name TEXT)")
        .unwrap();

    // Query with no results
    let query_result = db.query("SELECT * FROM empty_test").unwrap();

    // Check columns are still available
    assert_eq!(query_result.columns(), &["id", "name"]);

    // Collect should return empty vec
    let rows = query_result.rows();
    assert_eq!(rows.len(), 0);
}

#[test]
fn test_query_iterator_with_where_clause() {
    let temp_file = NamedTempFile::new().expect("Failed to create temp file");
    let db_path = temp_file.path();
    let mut db = Database::open(&format!("file://{}", db_path.display())).unwrap();

    // Setup test data
    db.execute("CREATE TABLE where_test (id INTEGER PRIMARY KEY, value INTEGER)")
        .unwrap();
    for i in 1..=10 {
        db.execute(&format!(
            "INSERT INTO where_test (id, value) VALUES ({}, {})",
            i,
            i * 10
        ))
        .unwrap();
    }

    // Query with WHERE clause
    let query_result = db
        .query("SELECT * FROM where_test WHERE value > 50")
        .unwrap();
    let rows = query_result.rows();
    assert_eq!(rows.len(), 5); // ids 6-10 have values > 50

    // Since we can't guarantee order without ORDER BY, just verify we have the right data
    // Check that all values are > 50
    for row in rows {
        if let SqlValue::Integer(value) = &row[1] {
            assert!(value > &50);
        }
    }
}

#[test]
fn test_transaction_query_iterator() {
    let temp_file = NamedTempFile::new().expect("Failed to create temp file");
    let db_path = temp_file.path();
    let mut db = Database::open(&format!("file://{}", db_path.display())).unwrap();

    // Setup test data
    db.execute("CREATE TABLE tx_test (id INTEGER PRIMARY KEY, value INTEGER)")
        .unwrap();
    db.execute("INSERT INTO tx_test (id, value) VALUES (1, 100)")
        .unwrap();
    db.execute("INSERT INTO tx_test (id, value) VALUES (2, 200)")
        .unwrap();

    // Test query iterator within transaction
    let mut tx = db.begin_transaction().unwrap();

    let query_result = tx.query("SELECT * FROM tx_test ORDER BY id").unwrap();
    let rows = query_result.rows();
    assert_eq!(rows.len(), 2);

    // Modify data in transaction
    tx.execute("INSERT INTO tx_test (id, value) VALUES (3, 300)")
        .unwrap();

    // Query again to see new data
    let query_result = tx.query("SELECT * FROM tx_test ORDER BY id").unwrap();
    let rows = query_result.rows();
    assert_eq!(rows.len(), 3);

    // Commit transaction
    tx.commit().unwrap();

    // Verify data is persisted
    let query_result = db.query("SELECT * FROM tx_test ORDER BY id").unwrap();
    let rows = query_result.rows();
    assert_eq!(rows.len(), 3);
}
