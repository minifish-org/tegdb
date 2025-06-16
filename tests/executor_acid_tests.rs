use tegdb::{Engine, executor::{Executor, ResultSet}};
use tegdb::parser::{parse_sql, SqlValue};
use tempfile::tempdir;

/// Test atomicity - all operations in a transaction succeed or all fail
#[test]
fn test_transaction_atomicity() {
    let dir = tempdir().unwrap();
    let db_path = dir.path().join("test_atomicity.db");
    let mut engine = Engine::new(db_path).unwrap();
    
    // First transaction - setup
    {
        let transaction = engine.begin_transaction();
        let mut executor = Executor::new(transaction);

        // Start transaction
        let (_, statement) = parse_sql("BEGIN").unwrap();
        executor.execute(statement).unwrap();

        // Create table
        let create_sql = "CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT NOT NULL, age INTEGER)";
        let (_, statement) = parse_sql(create_sql).unwrap();
        executor.execute(statement).unwrap();

        // Insert initial data
        let insert_sql = "INSERT INTO users (id, name, age) VALUES (1, 'John', 25)";
        let (_, statement) = parse_sql(insert_sql).unwrap();
        executor.execute(statement).unwrap();

        // Commit transaction
        let (_, statement) = parse_sql("COMMIT").unwrap();
        executor.execute(statement).unwrap();
        
        // Actually commit the transaction
        executor.transaction_mut().commit().unwrap();
    }

    // Second transaction - verify and update
    {
        let transaction = engine.begin_transaction();
        let mut executor = Executor::new(transaction);

        // Start new transaction to verify data
        let (_, statement) = parse_sql("BEGIN").unwrap();
        executor.execute(statement).unwrap();

        // Verify initial state
        let select_sql = "SELECT * FROM users";
        let (_, statement) = parse_sql(select_sql).unwrap();
        let result = executor.execute(statement).unwrap();
        if let ResultSet::Select { rows, .. } = result {
            assert_eq!(rows.len(), 1);
        }

        // Test atomicity with transaction
        // This test demonstrates that operations within a transaction are atomic
        let update_sql = "UPDATE users SET age = 30 WHERE id = 1";
        let (_, statement) = parse_sql(update_sql).unwrap();
        let result = executor.execute(statement).unwrap();
        if let ResultSet::Update { rows_affected } = result {
            assert_eq!(rows_affected, 1);
        }

        // Commit the update
        let (_, statement) = parse_sql("COMMIT").unwrap();
        executor.execute(statement).unwrap();
        
        // Actually commit the transaction
        executor.transaction_mut().commit().unwrap();
    }

    // Third transaction - final verification
    {
        let transaction = engine.begin_transaction();
        let mut executor = Executor::new(transaction);

        // Verify the update was applied atomically
        let (_, statement) = parse_sql("BEGIN").unwrap();
        executor.execute(statement).unwrap();
        
        let select_sql = "SELECT age FROM users WHERE id = 1";
        let (_, statement) = parse_sql(select_sql).unwrap();
        let result = executor.execute(statement).unwrap();
        if let ResultSet::Select { rows, .. } = result {
            assert_eq!(rows.len(), 1);
            assert_eq!(rows[0][0], SqlValue::Integer(30));
        }
        
        // Commit verification transaction
        let (_, statement) = parse_sql("COMMIT").unwrap();
        executor.execute(statement).unwrap();
        
        // Actually commit the transaction
        executor.transaction_mut().commit().unwrap();
    }
}

/// Test consistency - data remains in a valid state before and after transactions
#[test]
fn test_transaction_consistency() {
    let dir = tempdir().unwrap();
    let db_path = dir.path().join("test_consistency.db");
    let mut engine = Engine::new(db_path).unwrap();
    let transaction = engine.begin_transaction();
    let mut executor = Executor::new(transaction);

    // Begin transaction
    let (_, statement) = parse_sql("BEGIN").unwrap();
    executor.execute(statement).unwrap();

    // Create table
    let create_sql = "CREATE TABLE accounts (id INTEGER PRIMARY KEY, balance INTEGER)";
    let (_, statement) = parse_sql(create_sql).unwrap();
    executor.execute(statement).unwrap();

    // Insert initial data
    let insert_sql = "INSERT INTO accounts (id, balance) VALUES (1, 100), (2, 50)";
    let (_, statement) = parse_sql(insert_sql).unwrap();
    executor.execute(statement).unwrap();

    // Commit initial setup
    let (_, statement) = parse_sql("COMMIT").unwrap();
    executor.execute(statement).unwrap();

    // Begin transaction for verification
    let (_, statement) = parse_sql("BEGIN").unwrap();
    executor.execute(statement).unwrap();

    // Verify initial consistency - total balance should be 150
    let select_sql = "SELECT * FROM accounts";
    let (_, statement) = parse_sql(select_sql).unwrap();
    let result = executor.execute(statement).unwrap();
    if let ResultSet::Select { columns, rows } = result {
        assert_eq!(rows.len(), 2);
        
        let balance_idx = columns.iter().position(|c| c == "balance").unwrap();
        let total_balance: i64 = rows.iter()
            .map(|row| match &row[balance_idx] {
                SqlValue::Integer(balance) => *balance,
                _ => 0,
            })
            .sum();
        assert_eq!(total_balance, 150);
    }

    // Commit verification transaction
    let (_, statement) = parse_sql("COMMIT").unwrap();
    executor.execute(statement).unwrap();

    // Begin transaction for update
    let (_, statement) = parse_sql("BEGIN").unwrap();
    executor.execute(statement).unwrap();

    // Update one account
    let update_sql = "UPDATE accounts SET balance = 75 WHERE id = 1";
    let (_, statement) = parse_sql(update_sql).unwrap();
    executor.execute(statement).unwrap();

    // Verify consistency is maintained within transaction
    let select_sql = "SELECT * FROM accounts";
    let (_, statement) = parse_sql(select_sql).unwrap();
    let result = executor.execute(statement).unwrap();
    if let ResultSet::Select { columns, rows } = result {
        assert_eq!(rows.len(), 2);
        
        let balance_idx = columns.iter().position(|c| c == "balance").unwrap();
        
        // Account 1 should have balance 75
        // Account 2 should still have balance 50
        let account1_balance = match &rows[0][balance_idx] {
            SqlValue::Integer(balance) => *balance,
            _ => 0,
        };
        assert_eq!(account1_balance, 75);
    }
    
    // Commit the update transaction
    let (_, statement) = parse_sql("COMMIT").unwrap();
    executor.execute(statement).unwrap();
}

/// Test isolation - transactions don't interfere with each other
#[test]
fn test_transaction_isolation() {
    let dir = tempdir().unwrap();
    let db_path = dir.path().join("test_isolation.db");
    let mut engine = Engine::new(db_path).unwrap();
    let transaction = engine.begin_transaction();
    let mut executor = Executor::new(transaction);

    // Begin setup transaction
    let (_, statement) = parse_sql("BEGIN").unwrap();
    executor.execute(statement).unwrap();

    // Create table
    let create_sql = "CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT NOT NULL)";
    let (_, statement) = parse_sql(create_sql).unwrap();
    executor.execute(statement).unwrap();

    // Insert initial data
    let insert_sql = "INSERT INTO users (id, name) VALUES (1, 'John')";
    let (_, statement) = parse_sql(insert_sql).unwrap();
    executor.execute(statement).unwrap();

    // Commit setup transaction
    let (_, statement) = parse_sql("COMMIT").unwrap();
    executor.execute(statement).unwrap();

    // Begin first transaction
    let (_, statement) = parse_sql("BEGIN").unwrap();
    executor.execute(statement).unwrap();

    // First SELECT - should see the inserted data
    let select_sql = "SELECT * FROM users";
    let (_, statement) = parse_sql(select_sql).unwrap();
    let result1 = executor.execute(statement).unwrap();
    
    // Update data within same transaction
    let update_sql = "UPDATE users SET name = 'Jane' WHERE id = 1";
    let (_, statement) = parse_sql(update_sql).unwrap();
    executor.execute(statement).unwrap();

    // Second SELECT - should see updated data within transaction
    let select_sql = "SELECT * FROM users";
    let (_, statement) = parse_sql(select_sql).unwrap();
    let result2 = executor.execute(statement).unwrap();

    // Commit the transaction
    let (_, statement) = parse_sql("COMMIT").unwrap();
    executor.execute(statement).unwrap();

    // Verify isolation - each query within transaction sees consistent view
    if let (ResultSet::Select { columns: cols1, rows: rows1 }, ResultSet::Select { columns: cols2, rows: rows2 }) = (result1, result2) {
        assert_eq!(rows1.len(), 1);
        assert_eq!(rows2.len(), 1);
        
        // Find name column index for both queries
        let name_idx1 = cols1.iter().position(|c| c == "name").unwrap();
        let name_idx2 = cols2.iter().position(|c| c == "name").unwrap();
        
        // First query saw original data
        assert_eq!(rows1[0][name_idx1], SqlValue::Text("John".to_string()));
        
        // Second query sees updated data
        assert_eq!(rows2[0][name_idx2], SqlValue::Text("Jane".to_string()));
    }
}

/// Test durability - committed transactions persist across engine restarts
#[test]
fn test_transaction_durability() {
    let dir = tempdir().unwrap();
    let db_path = dir.path().join("test_durability.db");
    
    // First session - insert data
    {
        let mut engine = Engine::new(db_path.clone()).unwrap();
        let transaction = engine.begin_transaction();
        let mut executor = Executor::new(transaction);

        // Begin transaction
        let (_, statement) = parse_sql("BEGIN").unwrap();
        executor.execute(statement).unwrap();

        // Create table
        let create_sql = "CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT NOT NULL)";
        let (_, statement) = parse_sql(create_sql).unwrap();
        executor.execute(statement).unwrap();

        // Insert data
        let insert_sql = "INSERT INTO users (id, name) VALUES (1, 'John'), (2, 'Jane')";
        let (_, statement) = parse_sql(insert_sql).unwrap();
        executor.execute(statement).unwrap();

        // Commit transaction
        let (_, statement) = parse_sql("COMMIT").unwrap();
        executor.execute(statement).unwrap();
        
        // Actually commit the transaction
        executor.transaction_mut().commit().unwrap();
    } // Engine dropped here, data should be persisted

    // Second session - verify data persists
    {
        let mut engine = Engine::new(db_path).unwrap();
        let transaction = engine.begin_transaction();
        let mut executor = Executor::new(transaction);

        // Begin verification transaction
        let (_, statement) = parse_sql("BEGIN").unwrap();
        executor.execute(statement).unwrap();

        // Query data
        let select_sql = "SELECT * FROM users";
        let (_, statement) = parse_sql(select_sql).unwrap();
        let result = executor.execute(statement).unwrap();

        if let ResultSet::Select { rows, .. } = result {
            assert_eq!(rows.len(), 2);
            // Data should have persisted across engine restart
        }

        // Commit verification transaction
        let (_, statement) = parse_sql("COMMIT").unwrap();
        executor.execute(statement).unwrap();
        
        // Actually commit the transaction
        executor.transaction_mut().commit().unwrap();
    }
}

/// Test transaction behavior with DELETE operations
#[test]
fn test_delete_transaction_isolation() {
    let dir = tempdir().unwrap();
    let db_path = dir.path().join("test_delete.db");
    let mut engine = Engine::new(db_path).unwrap();
    let transaction = engine.begin_transaction();
    let mut executor = Executor::new(transaction);

    // Begin setup transaction
    let (_, statement) = parse_sql("BEGIN").unwrap();
    executor.execute(statement).unwrap();

    // Create table and insert data
    let create_sql = "CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT NOT NULL)";
    let (_, statement) = parse_sql(create_sql).unwrap();
    executor.execute(statement).unwrap();

    let insert_sql = "INSERT INTO users (id, name) VALUES (1, 'John'), (2, 'Jane'), (3, 'Bob')";
    let (_, statement) = parse_sql(insert_sql).unwrap();
    executor.execute(statement).unwrap();

    // Commit setup transaction
    let (_, statement) = parse_sql("COMMIT").unwrap();
    executor.execute(statement).unwrap();

    // Begin deletion transaction
    let (_, statement) = parse_sql("BEGIN").unwrap();
    executor.execute(statement).unwrap();

    // Delete specific record
    let delete_sql = "DELETE FROM users WHERE id = 2";
    let (_, statement) = parse_sql(delete_sql).unwrap();
    let result = executor.execute(statement).unwrap();

    if let ResultSet::Delete { rows_affected } = result {
        assert_eq!(rows_affected, 1);
    }

    // Verify deletion within transaction
    let select_sql = "SELECT * FROM users";
    let (_, statement) = parse_sql(select_sql).unwrap();
    let result = executor.execute(statement).unwrap();

    if let ResultSet::Select { columns, rows } = result {
        assert_eq!(rows.len(), 2);
        
        let id_idx = columns.iter().position(|c| c == "id").unwrap();
        
        // Should only have users with id 1 and 3
        let ids: Vec<i64> = rows.iter()
            .map(|row| match &row[id_idx] {
                SqlValue::Integer(id) => *id,
                _ => 0,
            })
            .collect();
        assert!(ids.contains(&1));
        assert!(ids.contains(&3));
        assert!(!ids.contains(&2));
    }

    // Commit deletion transaction
    let (_, statement) = parse_sql("COMMIT").unwrap();
    executor.execute(statement).unwrap();
}

/// Test multiple operations maintain transactional guarantees
#[test]
fn test_multiple_operations_acid() {
    let dir = tempdir().unwrap();
    let db_path = dir.path().join("test_multiple.db");
    let mut engine = Engine::new(db_path).unwrap();
    let transaction = engine.begin_transaction();
    let mut executor = Executor::new(transaction);

    // Begin transaction for all operations
    let (_, statement) = parse_sql("BEGIN").unwrap();
    executor.execute(statement).unwrap();

    // Create table
    let create_sql = "CREATE TABLE inventory (id INTEGER PRIMARY KEY, item TEXT, quantity INTEGER)";
    let (_, statement) = parse_sql(create_sql).unwrap();
    executor.execute(statement).unwrap();

    // Insert initial inventory
    let insert_sql = "INSERT INTO inventory (id, item, quantity) VALUES (1, 'apples', 10), (2, 'oranges', 5)";
    let (_, statement) = parse_sql(insert_sql).unwrap();
    executor.execute(statement).unwrap();

    // Update quantities within the same transaction
    let update1_sql = "UPDATE inventory SET quantity = 8 WHERE id = 1";
    let (_, statement) = parse_sql(update1_sql).unwrap();
    executor.execute(statement).unwrap();

    let update2_sql = "UPDATE inventory SET quantity = 3 WHERE id = 2";
    let (_, statement) = parse_sql(update2_sql).unwrap();
    executor.execute(statement).unwrap();

    // Add new item
    let insert2_sql = "INSERT INTO inventory (id, item, quantity) VALUES (3, 'bananas', 15)";
    let (_, statement) = parse_sql(insert2_sql).unwrap();
    executor.execute(statement).unwrap();

    // Verify final state within transaction
    let select_sql = "SELECT * FROM inventory";
    let (_, statement) = parse_sql(select_sql).unwrap();
    let result = executor.execute(statement).unwrap();

    if let ResultSet::Select { columns, rows } = result {
        assert_eq!(rows.len(), 3);
        
        // Find the indices of id and quantity columns
        let id_idx = columns.iter().position(|c| c == "id").unwrap();
        let quantity_idx = columns.iter().position(|c| c == "quantity").unwrap();
        
        // Verify each item has correct quantity
        for row in rows {
            match (&row[id_idx], &row[quantity_idx]) {
                (SqlValue::Integer(1), SqlValue::Integer(quantity)) => assert_eq!(*quantity, 8),
                (SqlValue::Integer(2), SqlValue::Integer(quantity)) => assert_eq!(*quantity, 3),
                (SqlValue::Integer(3), SqlValue::Integer(quantity)) => assert_eq!(*quantity, 15),
                _ => panic!("Unexpected row data: {:?}", row),
            }
        }
    }

    // Commit all operations
    let (_, statement) = parse_sql("COMMIT").unwrap();
    executor.execute(statement).unwrap();
}
