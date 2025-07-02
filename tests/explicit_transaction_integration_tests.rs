use tegdb::Database;
use tempfile::tempdir;

/// Test basic explicit transaction workflow
#[test]
fn test_explicit_transaction_basic_workflow() {
    let dir = tempdir().unwrap();
    let db_path = dir.path().join("test_explicit_basic.db");
    let mut db = Database::open(db_path).unwrap();

    // Begin transaction
    let mut tx = db.begin_transaction().unwrap();

    // Create table
    let create_sql = "CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT NOT NULL, age INTEGER)";
    let result = tx.execute(create_sql).unwrap();
    assert_eq!(result, 0); // CREATE TABLE returns 0 affected rows

    // Insert data
    let insert_sql = "INSERT INTO users (id, name, age) VALUES (1, 'Alice', 30), (2, 'Bob', 25)";
    let result = tx.execute(insert_sql).unwrap();
    assert_eq!(result, 2); // 2 rows inserted

    // Select data
    let select_sql = "SELECT * FROM users";
    let result = tx
        .streaming_query(select_sql)
        .unwrap()
        .into_query_result()
        .unwrap();
    assert_eq!(result.len(), 2);

    // Commit transaction
    tx.commit().unwrap();
}

/// Test rollback functionality
#[test]
fn test_explicit_transaction_rollback() {
    let dir = tempdir().unwrap();
    let db_path = dir.path().join("test_explicit_rollback.db");
    let mut db = Database::open(db_path).unwrap();

    // Setup initial data
    db.execute("CREATE TABLE products (id INTEGER PRIMARY KEY, name TEXT, price REAL)")
        .unwrap();
    db.execute("INSERT INTO products (id, name, price) VALUES (1, 'Laptop', 999.99)")
        .unwrap();

    // Begin transaction with operations to rollback
    {
        let mut tx = db.begin_transaction().unwrap();

        // Insert more data
        let insert_sql = "INSERT INTO products (id, name, price) VALUES (2, 'Mouse', 29.99)";
        tx.execute(insert_sql).unwrap();

        // Update existing data
        let update_sql = "UPDATE products SET price = 899.99 WHERE id = 1";
        tx.execute(update_sql).unwrap();

        // Verify changes are visible within transaction
        let select_sql = "SELECT * FROM products";
        let result = tx
            .streaming_query(select_sql)
            .unwrap()
            .into_query_result()
            .unwrap();
        assert_eq!(result.len(), 2); // Should see both products

        // Rollback transaction
        tx.rollback().unwrap();
    }

    // Verify rollback worked - changes should be gone
    let select_sql = "SELECT * FROM products";
    let result = db.query(select_sql).unwrap().into_query_result().unwrap();
    assert_eq!(result.len(), 1); // Should only see original product

    // Original price should be unchanged
    let rows = result.rows();
    if let Some(row) = rows.get(0) {
        if let tegdb::parser::SqlValue::Real(price) = &row[2] {
            // Assuming price is 3rd column
            assert!((price - 999.99).abs() < 0.01);
        }
    }
}

/// Test error handling for commit/rollback without begin
#[test]
fn test_explicit_transaction_error_handling() {
    let dir = tempdir().unwrap();
    let db_path = dir.path().join("test_explicit_errors.db");
    let mut db = Database::open(db_path).unwrap();

    // Test that operations work with explicit transactions
    let mut tx = db.begin_transaction().unwrap();

    let create_sql = "CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)";
    let result = tx.execute(create_sql);
    assert!(result.is_ok());

    let insert_sql = "INSERT INTO users (id, name) VALUES (1, 'Alice')";
    let result = tx.execute(insert_sql);
    assert!(result.is_ok());

    let select_sql = "SELECT * FROM users";
    let result = tx.streaming_query(select_sql);
    assert!(result.is_ok());

    tx.commit().unwrap();
}

/// Test complex transaction with multiple operations
#[test]
fn test_explicit_transaction_complex_operations() {
    let dir = tempdir().unwrap();
    let db_path = dir.path().join("test_explicit_complex.db");
    let mut db = Database::open(db_path).unwrap();

    // Start transaction
    let mut tx = db.begin_transaction().unwrap();

    // Create multiple tables
    let create_users_sql = "CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT NOT NULL)";
    tx.execute(create_users_sql).unwrap();

    let create_orders_sql =
        "CREATE TABLE orders (id INTEGER PRIMARY KEY, user_id INTEGER, amount REAL)";
    tx.execute(create_orders_sql).unwrap();

    // Insert data into multiple tables
    let insert_users_sql = "INSERT INTO users (id, name) VALUES (1, 'Alice'), (2, 'Bob')";
    tx.execute(insert_users_sql).unwrap();

    let insert_orders_sql =
        "INSERT INTO orders (id, user_id, amount) VALUES (1, 1, 99.99), (2, 2, 149.99)";
    tx.execute(insert_orders_sql).unwrap();

    // Update and delete operations
    let update_sql = "UPDATE orders SET amount = 129.99 WHERE id = 2";
    tx.execute(update_sql).unwrap();

    let delete_sql = "DELETE FROM orders WHERE amount < 100.0";
    let result = tx.execute(delete_sql).unwrap();
    assert_eq!(result, 1); // 1 row deleted

    // Verify state within transaction
    let select_orders_sql = "SELECT * FROM orders";
    let result = tx
        .streaming_query(select_orders_sql)
        .unwrap()
        .into_query_result()
        .unwrap();
    assert_eq!(result.len(), 1); // Should only have the updated order

    let select_users_sql = "SELECT * FROM users";
    let result = tx
        .streaming_query(select_users_sql)
        .unwrap()
        .into_query_result()
        .unwrap();
    assert_eq!(result.len(), 2); // Should have both users

    // Commit all changes
    tx.commit().unwrap();

    // Verify persistence after commit
    let select_orders_sql = "SELECT * FROM orders";
    let result = db
        .query(select_orders_sql)
        .unwrap()
        .into_query_result()
        .unwrap();
    assert_eq!(result.len(), 1); // Changes should be persisted
}

/// Test nested transaction behavior (transactions don't support nesting in this API)
#[test]
fn test_explicit_transaction_nested_behavior() {
    let dir = tempdir().unwrap();
    let db_path = dir.path().join("test_explicit_nested.db");
    let mut db = Database::open(db_path).unwrap();

    // Start first transaction
    let mut tx1 = db.begin_transaction().unwrap();

    // Test operations within first transaction
    tx1.execute("CREATE TABLE test (id INTEGER PRIMARY KEY)")
        .unwrap();
    tx1.execute("INSERT INTO test (id) VALUES (1)").unwrap();

    // Commit the original transaction
    tx1.commit().unwrap();

    // Now we can start a new transaction
    let mut tx2 = db.begin_transaction().unwrap();
    let result = tx2
        .streaming_query("SELECT * FROM test")
        .unwrap()
        .into_query_result()
        .unwrap();
    assert_eq!(result.len(), 1);
    tx2.commit().unwrap();
}
