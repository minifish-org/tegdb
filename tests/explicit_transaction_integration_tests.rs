use tegdb::{Engine, executor::{Executor, ResultSet}};
use tegdb::parser::{parse_sql, SqlValue, Statement};
use tempfile::tempdir;

/// Test basic explicit transaction workflow
#[test]
fn test_explicit_transaction_basic_workflow() {
    let dir = tempdir().unwrap();
    let db_path = dir.path().join("test_explicit_basic.db");
    let mut engine = Engine::new(db_path).unwrap();
    let transaction = engine.begin_transaction();
    let mut executor = Executor::new(transaction);

    // Begin transaction
    let result = executor.begin_transaction().unwrap();
    assert!(matches!(result, ResultSet::Begin { .. }));

    // Create table
    let create_sql = "CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT NOT NULL, age INTEGER)";
    let (_, statement) = parse_sql(create_sql).unwrap();
    let result = match statement {
        Statement::CreateTable(create) => executor.execute_create_table(create).unwrap(),
        _ => panic!("Expected CREATE TABLE statement"),
    };
    assert!(matches!(result, ResultSet::CreateTable { .. }));

    // Insert data
    let insert_sql = "INSERT INTO users (id, name, age) VALUES (1, 'Alice', 30), (2, 'Bob', 25)";
    let (_, statement) = parse_sql(insert_sql).unwrap();
    let result = match statement {
        Statement::Insert(insert) => executor.execute_insert(insert).unwrap(),
        _ => panic!("Expected INSERT statement"),
    };
    assert!(matches!(result, ResultSet::Insert { rows_affected: 2 }));

    // Select data
    let select_sql = "SELECT * FROM users";
    let (_, statement) = parse_sql(select_sql).unwrap();
    let result = match statement {
        Statement::Select(select) => executor.execute_select(select).unwrap(),
        _ => panic!("Expected SELECT statement"),
    };
    if let ResultSet::Select { rows, .. } = result {
        assert_eq!(rows.len(), 2);
    }

    // Commit transaction
    let result = executor.commit_transaction().unwrap();
    assert!(matches!(result, ResultSet::Commit { .. }));
}

/// Test rollback functionality
#[test]
fn test_explicit_transaction_rollback() {
    let dir = tempdir().unwrap();
    let db_path = dir.path().join("test_explicit_rollback.db");
    let mut engine = Engine::new(db_path).unwrap();
    
    // Setup initial data in first transaction
    {
        let transaction = engine.begin_transaction();
        let mut executor = Executor::new(transaction);

        executor.begin_transaction().unwrap();

        let create_sql = "CREATE TABLE products (id INTEGER PRIMARY KEY, name TEXT, price REAL)";
        let (_, statement) = parse_sql(create_sql).unwrap();
        match statement {
            Statement::CreateTable(create) => executor.execute_create_table(create).unwrap(),
            _ => panic!("Expected CREATE TABLE statement"),
        };

        let insert_sql = "INSERT INTO products (id, name, price) VALUES (1, 'Laptop', 999.99)";
        let (_, statement) = parse_sql(insert_sql).unwrap();
        match statement {
            Statement::Insert(insert) => executor.execute_insert(insert).unwrap(),
            _ => panic!("Expected INSERT statement"),
        };

        executor.commit_transaction().unwrap();
        
        // Actually commit
        executor.transaction_mut().commit().unwrap();
    }

    // Second transaction with operations to rollback
    {
        let transaction = engine.begin_transaction();
        let mut executor = Executor::new(transaction);

        executor.begin_transaction().unwrap();

        // Insert more data
        let insert_sql = "INSERT INTO products (id, name, price) VALUES (2, 'Mouse', 29.99)";
        let (_, statement) = parse_sql(insert_sql).unwrap();
        match statement {
            Statement::Insert(insert) => executor.execute_insert(insert).unwrap(),
            _ => panic!("Expected INSERT statement"),
        };

        // Update existing data
        let update_sql = "UPDATE products SET price = 899.99 WHERE id = 1";
        let (_, statement) = parse_sql(update_sql).unwrap();
        match statement {
            Statement::Update(update) => executor.execute_update(update).unwrap(),
            _ => panic!("Expected UPDATE statement"),
        };

        // Verify changes are visible within transaction
        let select_sql = "SELECT * FROM products";
        let (_, statement) = parse_sql(select_sql).unwrap();
        let result = match statement {
            Statement::Select(select) => executor.execute_select(select).unwrap(),
            _ => panic!("Expected SELECT statement"),
        };
        if let ResultSet::Select { rows, .. } = result {
            assert_eq!(rows.len(), 2); // Should see both products
        }

        // Rollback transaction
        let result = executor.rollback_transaction().unwrap();
        assert!(matches!(result, ResultSet::Rollback { .. }));
        
        // Actually rollback
        let _ = executor.transaction_mut().rollback();
    }

    // Third transaction to verify rollback worked
    {
        let transaction = engine.begin_transaction();
        let mut executor = Executor::new(transaction);

        executor.begin_transaction().unwrap();

        let select_sql = "SELECT * FROM products";
        let (_, statement) = parse_sql(select_sql).unwrap();
        let result = match statement {
            Statement::Select(select) => executor.execute_select(select).unwrap(),
            _ => panic!("Expected SELECT statement"),
        };
        if let ResultSet::Select { rows, .. } = result {
            assert_eq!(rows.len(), 1); // Should only see original product
            // Original price should be unchanged
            if let SqlValue::Real(price) = &rows[0][2] { // Assuming price is 3rd column
                assert!((price - 999.99).abs() < 0.01);
            }
        }

        executor.commit_transaction().unwrap();
        
        // Actually commit
        executor.transaction_mut().commit().unwrap();
    }
}

/// Test error handling for commit/rollback without begin
#[test]
fn test_explicit_transaction_error_handling() {
    let dir = tempdir().unwrap();
    let db_path = dir.path().join("test_explicit_errors.db");
    let mut engine = Engine::new(db_path).unwrap();
    let transaction = engine.begin_transaction();
    let mut executor = Executor::new(transaction);

    // Try COMMIT without BEGIN - should fail
    let result = executor.commit_transaction();
    assert!(result.is_err());

    // Try ROLLBACK without BEGIN - should fail
    let result = executor.rollback_transaction();
    assert!(result.is_err());
    
    // Now test that operations work after BEGIN
    executor.begin_transaction().unwrap();
    
    let create_sql = "CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)";
    let (_, statement) = parse_sql(create_sql).unwrap();
    let result = match statement {
        Statement::CreateTable(create) => executor.execute_create_table(create),
        _ => panic!("Expected CREATE TABLE statement"),
    };
    assert!(result.is_ok());

    let insert_sql = "INSERT INTO users (id, name) VALUES (1, 'Alice')";
    let (_, statement) = parse_sql(insert_sql).unwrap();
    let result = match statement {
        Statement::Insert(insert) => executor.execute_insert(insert),
        _ => panic!("Expected INSERT statement"),
    };
    assert!(result.is_ok());

    let select_sql = "SELECT * FROM users";
    let (_, statement) = parse_sql(select_sql).unwrap();
    let result = match statement {
        Statement::Select(select) => executor.execute_select(select),
        _ => panic!("Expected SELECT statement"),
    };
    assert!(result.is_ok());
    
    executor.commit_transaction().unwrap();
}

/// Test complex transaction with multiple operations
#[test]
fn test_explicit_transaction_complex_operations() {
    let dir = tempdir().unwrap();
    let db_path = dir.path().join("test_explicit_complex.db");
    let mut engine = Engine::new(db_path).unwrap();
    let transaction = engine.begin_transaction();
    let mut executor = Executor::new(transaction);

    // Start transaction
    executor.begin_transaction().unwrap();

    // Create multiple tables
    let create_users_sql = "CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT NOT NULL)";
    let (_, statement) = parse_sql(create_users_sql).unwrap();
    match statement {
        Statement::CreateTable(create) => executor.execute_create_table(create).unwrap(),
        _ => panic!("Expected CREATE TABLE statement"),
    };

    let create_orders_sql = "CREATE TABLE orders (id INTEGER PRIMARY KEY, user_id INTEGER, amount REAL)";
    let (_, statement) = parse_sql(create_orders_sql).unwrap();
    match statement {
        Statement::CreateTable(create) => executor.execute_create_table(create).unwrap(),
        _ => panic!("Expected CREATE TABLE statement"),
    };

    // Insert data into multiple tables
    let insert_users_sql = "INSERT INTO users (id, name) VALUES (1, 'Alice'), (2, 'Bob')";
    let (_, statement) = parse_sql(insert_users_sql).unwrap();
    match statement {
        Statement::Insert(insert) => executor.execute_insert(insert).unwrap(),
        _ => panic!("Expected INSERT statement"),
    };

    let insert_orders_sql = "INSERT INTO orders (id, user_id, amount) VALUES (1, 1, 99.99), (2, 2, 149.99)";
    let (_, statement) = parse_sql(insert_orders_sql).unwrap();
    match statement {
        Statement::Insert(insert) => executor.execute_insert(insert).unwrap(),
        _ => panic!("Expected INSERT statement"),
    };

    // Update and delete operations
    let update_sql = "UPDATE orders SET amount = 129.99 WHERE id = 2";
    let (_, statement) = parse_sql(update_sql).unwrap();
    match statement {
        Statement::Update(update) => executor.execute_update(update).unwrap(),
        _ => panic!("Expected UPDATE statement"),
    };

    let delete_sql = "DELETE FROM orders WHERE amount < 100.0";
    let (_, statement) = parse_sql(delete_sql).unwrap();
    let result = match statement {
        Statement::Delete(delete) => executor.execute_delete(delete).unwrap(),
        _ => panic!("Expected DELETE statement"),
    };
    assert!(matches!(result, ResultSet::Delete { rows_affected: 1 }));

    // Verify state within transaction
    let select_orders_sql = "SELECT * FROM orders";
    let (_, statement) = parse_sql(select_orders_sql).unwrap();
    let result = match statement {
        Statement::Select(select) => executor.execute_select(select).unwrap(),
        _ => panic!("Expected SELECT statement"),
    };
    if let ResultSet::Select { rows, .. } = result {
        assert_eq!(rows.len(), 1); // Should only have the updated order
    }

    let select_users_sql = "SELECT * FROM users";
    let (_, statement) = parse_sql(select_users_sql).unwrap();
    let result = match statement {
        Statement::Select(select) => executor.execute_select(select).unwrap(),
        _ => panic!("Expected SELECT statement"),
    };
    if let ResultSet::Select { rows, .. } = result {
        assert_eq!(rows.len(), 2); // Should have both users
    }

    // Commit all changes
    executor.commit_transaction().unwrap();

    // Verify persistence after commit
    executor.begin_transaction().unwrap();

    let select_orders_sql = "SELECT * FROM orders";
    let (_, statement) = parse_sql(select_orders_sql).unwrap();
    let result = match statement {
        Statement::Select(select) => executor.execute_select(select).unwrap(),
        _ => panic!("Expected SELECT statement"),
    };
    if let ResultSet::Select { rows, .. } = result {
        assert_eq!(rows.len(), 1); // Changes should be persisted
    }

    executor.commit_transaction().unwrap();
}

/// Test nested transaction behavior (should not allow)
#[test]
fn test_explicit_transaction_nested_behavior() {
    let dir = tempdir().unwrap();
    let db_path = dir.path().join("test_explicit_nested.db");
    let mut engine = Engine::new(db_path).unwrap();
    let transaction = engine.begin_transaction();
    let mut executor = Executor::new(transaction);

    // Start first transaction
    executor.begin_transaction().unwrap();

    // Try to start nested transaction - should fail
    let result = executor.begin_transaction();
    assert!(result.is_err()); // Should not allow nested transactions

    // Commit the original transaction
    executor.commit_transaction().unwrap();

    // Now BEGIN should work again
    let result = executor.begin_transaction();
    assert!(result.is_ok());

    executor.commit_transaction().unwrap();
}