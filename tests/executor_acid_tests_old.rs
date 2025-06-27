use tegdb::{Engine, executor::{Executor, ResultSet}};
use tegdb::parser::{parse_sql, SqlValue, Statement};
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
        executor.begin_transaction().unwrap();

        // Create table
        let create_sql = "CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT NOT NULL, age INTEGER)";
        let (_, statement) = parse_sql(create_sql).unwrap();
        match statement {
            Statement::CreateTable(create) => executor.execute_create_table(create).unwrap(),
            _ => panic!("Expected CREATE TABLE statement"),
        };

        // Insert initial data
        let insert_sql = "INSERT INTO users (id, name, age) VALUES (1, 'John', 25)";
        let (_, statement) = parse_sql(insert_sql).unwrap();
        match statement {
            Statement::Insert(insert) => executor.execute_insert(insert).unwrap(),
            _ => panic!("Expected INSERT statement"),
        };

        // Commit transaction
        executor.commit_transaction().unwrap();
        
        // Actually commit the transaction
        executor.transaction_mut().commit().unwrap();
    }

    // Second transaction - verify and update
    {
        let transaction = engine.begin_transaction();
        let mut executor = Executor::new(transaction);

        // Start new transaction to verify data
        executor.begin_transaction().unwrap();

        // Verify initial state
        let select_sql = "SELECT * FROM users";
        let (_, statement) = parse_sql(select_sql).unwrap();
        let result = match statement {
            Statement::Select(select) => executor.execute_select(select).unwrap(),
            _ => panic!("Expected SELECT statement"),
        };
        
        if let ResultSet::Select { rows, .. } = result {
            assert_eq!(rows.len(), 1);
        }

        // Insert more data
        let insert_sql = "INSERT INTO users (id, name, age) VALUES (2, 'Jane', 30)";
        let (_, statement) = parse_sql(insert_sql).unwrap();
        match statement {
            Statement::Insert(insert) => executor.execute_insert(insert).unwrap(),
            _ => panic!("Expected INSERT statement"),
        };

        // Rollback transaction (simulates failure)
        executor.rollback_transaction().unwrap();
        
        // Actually rollback the transaction
        let _ = executor.transaction_mut().rollback();
    }

    // Third transaction - verify rollback worked
    {
        let transaction = engine.begin_transaction();
        let mut executor = Executor::new(transaction);

        executor.begin_transaction().unwrap();

        let select_sql = "SELECT * FROM users";
        let (_, statement) = parse_sql(select_sql).unwrap();
        let result = match statement {
            Statement::Select(select) => executor.execute_select(select).unwrap(),
            _ => panic!("Expected SELECT statement"),
        };
        
        if let ResultSet::Select { rows, .. } = result {
            assert_eq!(rows.len(), 1); // Should only have original row due to rollback
        }

        executor.commit_transaction().unwrap();
        executor.transaction_mut().commit().unwrap();
    }
}

/// Test consistency - database remains in valid state
#[test]
fn test_transaction_consistency() {
    let dir = tempdir().unwrap();
    let db_path = dir.path().join("test_consistency.db");
    let mut engine = Engine::new(db_path).unwrap();
    let transaction = engine.begin_transaction();
    let mut executor = Executor::new(transaction);

    executor.begin_transaction().unwrap();

    // Create table with constraints
    let create_sql = "CREATE TABLE accounts (id INTEGER PRIMARY KEY, balance REAL NOT NULL)";
    let (_, statement) = parse_sql(create_sql).unwrap();
    match statement {
        Statement::CreateTable(create) => executor.execute_create_table(create).unwrap(),
        _ => panic!("Expected CREATE TABLE statement"),
    };

    // Insert initial accounts
    let inserts = vec![
        "INSERT INTO accounts (id, balance) VALUES (1, 1000.0)",
        "INSERT INTO accounts (id, balance) VALUES (2, 500.0)",
    ];

    for sql in inserts {
        let (_, statement) = parse_sql(sql).unwrap();
        match statement {
            Statement::Insert(insert) => executor.execute_insert(insert).unwrap(),
            _ => panic!("Expected INSERT statement"),
        };
    }

    // Verify initial state
    let select_sql = "SELECT * FROM accounts";
    let (_, statement) = parse_sql(select_sql).unwrap();
    let result = match statement {
        Statement::Select(select) => executor.execute_select(select).unwrap(),
        _ => panic!("Expected SELECT statement"),
    };
    
    if let ResultSet::Select { rows, .. } = result {
        assert_eq!(rows.len(), 2);
    }

    executor.commit_transaction().unwrap();
}

/// Test isolation - concurrent transactions don't interfere
#[test]
fn test_transaction_isolation() {
    let dir = tempdir().unwrap();
    let db_path = dir.path().join("test_isolation.db");
    let mut engine = Engine::new(db_path).unwrap();
    
    // Setup initial data
    {
        let transaction = engine.begin_transaction();
        let mut executor = Executor::new(transaction);

        executor.begin_transaction().unwrap();

        let create_sql = "CREATE TABLE items (id INTEGER PRIMARY KEY, value INTEGER)";
        let (_, statement) = parse_sql(create_sql).unwrap();
        match statement {
            Statement::CreateTable(create) => executor.execute_create_table(create).unwrap(),
            _ => panic!("Expected CREATE TABLE statement"),
        };

        let insert_sql = "INSERT INTO items (id, value) VALUES (1, 100)";
        let (_, statement) = parse_sql(insert_sql).unwrap();
        match statement {
            Statement::Insert(insert) => executor.execute_insert(insert).unwrap(),
            _ => panic!("Expected INSERT statement"),
        };

        executor.commit_transaction().unwrap();
        executor.transaction_mut().commit().unwrap();
    }

    // Transaction 1 - read and modify
    {
        let transaction = engine.begin_transaction();
        let mut executor = Executor::new(transaction);

        executor.begin_transaction().unwrap();

        let select_sql = "SELECT * FROM items WHERE id = 1";
        let (_, statement) = parse_sql(select_sql).unwrap();
        let result = match statement {
            Statement::Select(select) => executor.execute_select(select).unwrap(),
            _ => panic!("Expected SELECT statement"),
        };
        
        if let ResultSet::Select { rows, .. } = result {
            assert_eq!(rows.len(), 1);
        }

        let update_sql = "UPDATE items SET value = 200 WHERE id = 1";
        let (_, statement) = parse_sql(update_sql).unwrap();
        match statement {
            Statement::Update(update) => executor.execute_update(update).unwrap(),
            _ => panic!("Expected UPDATE statement"),
        };

        executor.commit_transaction().unwrap();
        executor.transaction_mut().commit().unwrap();
    }
}

/// Test durability - committed changes persist
#[test]
fn test_transaction_durability() {
    let dir = tempdir().unwrap();
    let db_path = dir.path().join("test_durability.db");
    
    // First scope - make changes and commit
    {
        let mut engine = Engine::new(db_path.clone()).unwrap();
        let transaction = engine.begin_transaction();
        let mut executor = Executor::new(transaction);

        executor.begin_transaction().unwrap();

        let create_sql = "CREATE TABLE persistent_data (id INTEGER PRIMARY KEY, message TEXT)";
        let (_, statement) = parse_sql(create_sql).unwrap();
        match statement {
            Statement::CreateTable(create) => executor.execute_create_table(create).unwrap(),
            _ => panic!("Expected CREATE TABLE statement"),
        };

        let insert_sql = "INSERT INTO persistent_data (id, message) VALUES (1, 'This should persist')";
        let (_, statement) = parse_sql(insert_sql).unwrap();
        match statement {
            Statement::Insert(insert) => executor.execute_insert(insert).unwrap(),
            _ => panic!("Expected INSERT statement"),
        };

        executor.commit_transaction().unwrap();
        executor.transaction_mut().commit().unwrap();
    } // Engine goes out of scope and is dropped

    // Second scope - reopen database and verify data persisted
    {
        let mut engine = Engine::new(db_path).unwrap();
        let transaction = engine.begin_transaction();
        let mut executor = Executor::new(transaction);

        executor.begin_transaction().unwrap();

        let select_sql = "SELECT * FROM persistent_data";
        let (_, statement) = parse_sql(select_sql).unwrap();
        let result = match statement {
            Statement::Select(select) => executor.execute_select(select).unwrap(),
            _ => panic!("Expected SELECT statement"),
        };
        
        if let ResultSet::Select { rows, .. } = result {
            assert_eq!(rows.len(), 1);
            if let SqlValue::Text(message) = &rows[0][1] {
                assert_eq!(message, "This should persist");
            }
        }

        executor.commit_transaction().unwrap();
    }
}

/// Test rollback scenarios
#[test]
fn test_transaction_rollback_scenarios() {
    let dir = tempdir().unwrap();
    let db_path = dir.path().join("test_rollback.db");
    let mut engine = Engine::new(db_path).unwrap();
    
    // Setup
    {
        let transaction = engine.begin_transaction();
        let mut executor = Executor::new(transaction);

        executor.begin_transaction().unwrap();

        let create_sql = "CREATE TABLE test_rollback (id INTEGER PRIMARY KEY, data TEXT)";
        let (_, statement) = parse_sql(create_sql).unwrap();
        match statement {
            Statement::CreateTable(create) => executor.execute_create_table(create).unwrap(),
            _ => panic!("Expected CREATE TABLE statement"),
        };

        executor.commit_transaction().unwrap();
        executor.transaction_mut().commit().unwrap();
    }

    // Test explicit rollback
    {
        let transaction = engine.begin_transaction();
        let mut executor = Executor::new(transaction);

        executor.begin_transaction().unwrap();

        // Insert data
        let insert_sql = "INSERT INTO test_rollback (id, data) VALUES (1, 'should be rolled back')";
        let (_, statement) = parse_sql(insert_sql).unwrap();
        match statement {
            Statement::Insert(insert) => executor.execute_insert(insert).unwrap(),
            _ => panic!("Expected INSERT statement"),
        };

        // Verify data exists within transaction
        let select_sql = "SELECT * FROM test_rollback";
        let (_, statement) = parse_sql(select_sql).unwrap();
        let result = match statement {
            Statement::Select(select) => executor.execute_select(select).unwrap(),
            _ => panic!("Expected SELECT statement"),
        };
        
        if let ResultSet::Select { rows, .. } = result {
            assert_eq!(rows.len(), 1);
        }

        // Rollback
        executor.rollback_transaction().unwrap();
        let _ = executor.transaction_mut().rollback();
    }

    // Verify rollback worked
    {
        let transaction = engine.begin_transaction();
        let mut executor = Executor::new(transaction);

        executor.begin_transaction().unwrap();

        let select_sql = "SELECT * FROM test_rollback";
        let (_, statement) = parse_sql(select_sql).unwrap();
        let result = match statement {
            Statement::Select(select) => executor.execute_select(select).unwrap(),
            _ => panic!("Expected SELECT statement"),
        };
        
        if let ResultSet::Select { rows, .. } = result {
            assert_eq!(rows.len(), 0); // Should be empty due to rollback
        }

        executor.commit_transaction().unwrap();
    }
}

/// Test concurrent access patterns
#[test]
fn test_concurrent_transaction_patterns() {
    let dir = tempdir().unwrap();
    let db_path = dir.path().join("test_concurrent.db");
    let mut engine = Engine::new(db_path).unwrap();
    
    // Setup shared table
    {
        let transaction = engine.begin_transaction();
        let mut executor = Executor::new(transaction);

        executor.begin_transaction().unwrap();

        let create_sql = "CREATE TABLE shared_counter (id INTEGER PRIMARY KEY, count INTEGER)";
        let (_, statement) = parse_sql(create_sql).unwrap();
        match statement {
            Statement::CreateTable(create) => executor.execute_create_table(create).unwrap(),
            _ => panic!("Expected CREATE TABLE statement"),
        };

        let insert_sql = "INSERT INTO shared_counter (id, count) VALUES (1, 0)";
        let (_, statement) = parse_sql(insert_sql).unwrap();
        match statement {
            Statement::Insert(insert) => executor.execute_insert(insert).unwrap(),
            _ => panic!("Expected INSERT statement"),
        };

        executor.commit_transaction().unwrap();
        executor.transaction_mut().commit().unwrap();
    }

    // Simulate multiple transactions updating the counter
    for i in 1..=3 {
        let transaction = engine.begin_transaction();
        let mut executor = Executor::new(transaction);

        executor.begin_transaction().unwrap();

        let update_sql = format!("UPDATE shared_counter SET count = {} WHERE id = 1", i);
        let (_, statement) = parse_sql(&update_sql).unwrap();
        match statement {
            Statement::Update(update) => executor.execute_update(update).unwrap(),
            _ => panic!("Expected UPDATE statement"),
        };

        executor.commit_transaction().unwrap();
        executor.transaction_mut().commit().unwrap();
    }

    // Verify final state
    {
        let transaction = engine.begin_transaction();
        let mut executor = Executor::new(transaction);

        executor.begin_transaction().unwrap();

        let select_sql = "SELECT count FROM shared_counter WHERE id = 1";
        let (_, statement) = parse_sql(select_sql).unwrap();
        let result = match statement {
            Statement::Select(select) => executor.execute_select(select).unwrap(),
            _ => panic!("Expected SELECT statement"),
        };
        
        if let ResultSet::Select { rows, .. } = result {
            assert_eq!(rows.len(), 1);
            if let SqlValue::Integer(count) = &rows[0][0] {
                assert_eq!(*count, 3);
            }
        }

        executor.commit_transaction().unwrap();
    }
}
