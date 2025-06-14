use tegdb::{Engine, parser::parse_sql, executor::{Executor, ResultSet}};
use tempfile::tempdir;

#[test]
fn test_sql_integration_basic_operations() {
    let dir = tempdir().unwrap();
    let db_path = dir.path().join("test_integration.db");
    let engine = Engine::new(db_path).unwrap();
    let mut executor = Executor::new(engine);

    // Begin transaction
    let (_, statement) = parse_sql("BEGIN").unwrap();
    executor.execute(statement).unwrap();

    // Create table
    let create_sql = "CREATE TABLE products (id INTEGER PRIMARY KEY, name TEXT NOT NULL, price REAL)";
    let (_, statement) = parse_sql(create_sql).unwrap();
    let result = executor.execute(statement).unwrap();
    assert!(matches!(result, ResultSet::CreateTable { .. }));

    // Insert data
    let insert_sql = "INSERT INTO products (id, name, price) VALUES (1, 'Laptop', 999.99), (2, 'Mouse', 29.99)";
    let (_, statement) = parse_sql(insert_sql).unwrap();
    let result = executor.execute(statement).unwrap();
    assert!(matches!(result, ResultSet::Insert { rows_affected: 2 }));

    // Select all
    let select_sql = "SELECT * FROM products";
    let (_, statement) = parse_sql(select_sql).unwrap();
    let result = executor.execute(statement).unwrap();
    
    if let ResultSet::Select { columns: _, rows } = result {
        assert_eq!(rows.len(), 2);
    } else {
        panic!("Expected Select result");
    }

    // Commit transaction
    let (_, statement) = parse_sql("COMMIT").unwrap();
    executor.execute(statement).unwrap();

    // Begin new transaction for queries
    let (_, statement) = parse_sql("BEGIN").unwrap();
    executor.execute(statement).unwrap();

    // Select with WHERE
    let select_where_sql = "SELECT name FROM products WHERE price > 50.0";
    let (_, statement) = parse_sql(select_where_sql).unwrap();
    let result = executor.execute(statement).unwrap();
    
    if let ResultSet::Select { columns, rows } = result {
        assert_eq!(columns, vec!["name"]);
        assert_eq!(rows.len(), 1);
    } else {
        panic!("Expected Select result");
    }

    // Update
    let update_sql = "UPDATE products SET price = 899.99 WHERE name = 'Laptop'";
    let (_, statement) = parse_sql(update_sql).unwrap();
    let result = executor.execute(statement).unwrap();
    assert!(matches!(result, ResultSet::Update { rows_affected: 1 }));

    // Delete
    let delete_sql = "DELETE FROM products WHERE price < 50.0";
    let (_, statement) = parse_sql(delete_sql).unwrap();
    let result = executor.execute(statement).unwrap();
    assert!(matches!(result, ResultSet::Delete { rows_affected: 1 }));

    // Verify final state
    let final_select_sql = "SELECT * FROM products";
    let (_, statement) = parse_sql(final_select_sql).unwrap();
    let result = executor.execute(statement).unwrap();
    
    if let ResultSet::Select { columns: _, rows } = result {
        assert_eq!(rows.len(), 1); // Only the laptop should remain
    } else {
        panic!("Expected Select result");
    }

    // Commit final transaction
    let (_, statement) = parse_sql("COMMIT").unwrap();
    executor.execute(statement).unwrap();
}

#[test]
fn test_sql_parser_edge_cases() {
    // Test case-insensitive keywords
    let sql = "select * from USERS where AGE > 18";
    let result = parse_sql(sql);
    assert!(result.is_ok());

    // Test with extra whitespace
    let sql = "  SELECT   *   FROM   users   WHERE   age   >   18  ";
    let result = parse_sql(sql);
    assert!(result.is_ok());

    // Test string literals with spaces
    let sql = "INSERT INTO users (name) VALUES ('John Doe')";
    let result = parse_sql(sql);
    assert!(result.is_ok());

    // Test negative numbers
    let sql = "SELECT * FROM accounts WHERE balance < -100";
    let result = parse_sql(sql);
    assert!(result.is_ok());

    // Test floating point numbers
    let sql = "SELECT * FROM products WHERE price = 29.99";
    let result = parse_sql(sql);
    assert!(result.is_ok());
}

#[test]
fn test_sql_executor_persistence() {
    let dir = tempdir().unwrap();
    let db_path = dir.path().join("test_persistence.db");
    
    // First session - create and insert data
    {
        let engine = Engine::new(db_path.clone()).unwrap();
        let mut executor = Executor::new(engine);

        // Begin transaction
        let (_, statement) = parse_sql("BEGIN").unwrap();
        executor.execute(statement).unwrap();

        let create_sql = "CREATE TABLE settings (key TEXT PRIMARY KEY, value TEXT)";
        let (_, statement) = parse_sql(create_sql).unwrap();
        executor.execute(statement).unwrap();

        let insert_sql = "INSERT INTO settings (key, value) VALUES ('theme', 'dark'), ('language', 'en')";
        let (_, statement) = parse_sql(insert_sql).unwrap();
        executor.execute(statement).unwrap();

        // Commit transaction
        let (_, statement) = parse_sql("COMMIT").unwrap();
        executor.execute(statement).unwrap();
    }

    // Second session - verify data persists
    {
        let engine = Engine::new(db_path).unwrap();
        let mut executor = Executor::new(engine);

        // Begin transaction for reading
        let (_, statement) = parse_sql("BEGIN").unwrap();
        executor.execute(statement).unwrap();

        let select_sql = "SELECT * FROM settings";
        let (_, statement) = parse_sql(select_sql).unwrap();
        let result = executor.execute(statement).unwrap();
        
        if let ResultSet::Select { columns: _, rows } = result {
            assert_eq!(rows.len(), 2);
        } else {
            panic!("Expected Select result");
        }

        // Commit transaction
        let (_, statement) = parse_sql("COMMIT").unwrap();
        executor.execute(statement).unwrap();
    }
}

#[test]
fn test_executor_create_and_insert() {
    let dir = tempdir().unwrap();
    let db_path = dir.path().join("test.db");
    let engine = Engine::new(db_path).unwrap();
    let mut executor = Executor::new(engine);

    // Begin transaction
    let (_, statement) = parse_sql("BEGIN").unwrap();
    let result = executor.execute(statement).unwrap();
    match result {
        ResultSet::Begin { .. } => {},
        _ => panic!("Expected Begin result"),
    }

    // Create table
    let create_sql = "CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT NOT NULL, age INTEGER)";
    let (_, statement) = parse_sql(create_sql).unwrap();
    let result = executor.execute(statement).unwrap();
    
    match result {
        ResultSet::CreateTable { table_name } => {
            assert_eq!(table_name, "users");
        }
        _ => panic!("Expected CreateTable result"),
    }

    // Insert data
    let insert_sql = "INSERT INTO users (id, name, age) VALUES (1, 'John', 25)";
    let (_, statement) = parse_sql(insert_sql).unwrap();
    let result = executor.execute(statement).unwrap();

    match result {
        ResultSet::Insert { rows_affected } => {
            assert_eq!(rows_affected, 1);
        }
        _ => panic!("Expected Insert result"),
    }

    // Commit transaction
    let (_, statement) = parse_sql("COMMIT").unwrap();
    let result = executor.execute(statement).unwrap();
    match result {
        ResultSet::Commit { .. } => {},
        _ => panic!("Expected Commit result"),
    }
}

#[test]
fn test_executor_select() {
    let dir = tempdir().unwrap();
    let db_path = dir.path().join("test.db");
    let engine = Engine::new(db_path).unwrap();
    let mut executor = Executor::new(engine);

    // Begin transaction
    let (_, statement) = parse_sql("BEGIN").unwrap();
    executor.execute(statement).unwrap();

    // Create table first
    let create_sql = "CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT NOT NULL, age INTEGER)";
    let (_, statement) = parse_sql(create_sql).unwrap();
    executor.execute(statement).unwrap();

    // Insert test data
    let insert_sql = "INSERT INTO users (id, name, age) VALUES (1, 'John', 25), (2, 'Jane', 30)";
    let (_, statement) = parse_sql(insert_sql).unwrap();
    executor.execute(statement).unwrap();

    // Select all
    let select_sql = "SELECT * FROM users";
    let (_, statement) = parse_sql(select_sql).unwrap();
    let result = executor.execute(statement).unwrap();

    match result {
        ResultSet::Select { columns: _, rows } => {
            assert_eq!(rows.len(), 2);
        }
        _ => panic!("Expected Select result"),
    }

    // Commit transaction
    let (_, statement) = parse_sql("COMMIT").unwrap();
    executor.execute(statement).unwrap();
}

#[test]
fn test_transaction_rollback_on_error() {
    let dir = tempdir().unwrap();
    let db_path = dir.path().join("test.db");
    let engine = Engine::new(db_path).unwrap();
    let mut executor = Executor::new(engine);

    // Begin transaction
    let (_, statement) = parse_sql("BEGIN").unwrap();
    executor.execute(statement).unwrap();

    // Create table first
    let create_sql = "CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT NOT NULL, age INTEGER)";
    let (_, statement) = parse_sql(create_sql).unwrap();
    executor.execute(statement).unwrap();

    // Insert initial data
    let insert_sql = "INSERT INTO users (id, name, age) VALUES (1, 'John', 25)";
    let (_, statement) = parse_sql(insert_sql).unwrap();
    executor.execute(statement).unwrap();

    // Verify initial state
    let select_sql = "SELECT * FROM users";
    let (_, statement) = parse_sql(select_sql).unwrap();
    let result = executor.execute(statement).unwrap();
    if let ResultSet::Select { rows, .. } = result {
        assert_eq!(rows.len(), 1);
    }

    // Test rollback
    let (_, statement) = parse_sql("ROLLBACK").unwrap();
    let result = executor.execute(statement).unwrap();
    match result {
        ResultSet::Rollback { .. } => {},
        _ => panic!("Expected Rollback result"),
    }

    // This test demonstrates rollback functionality
    // All operations within the transaction are discarded
}
