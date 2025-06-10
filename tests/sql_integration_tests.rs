use tegdb::{Engine, sql::parse_sql, sql_executor::{SqlExecutor, SqlResult}};
use tempfile::tempdir;

#[test]
fn test_sql_integration_basic_operations() {
    let dir = tempdir().unwrap();
    let db_path = dir.path().join("test_integration.db");
    let engine = Engine::new(db_path).unwrap();
    let mut executor = SqlExecutor::new(engine);

    // Create table
    let create_sql = "CREATE TABLE products (id INTEGER PRIMARY KEY, name TEXT NOT NULL, price REAL)";
    let (_, statement) = parse_sql(create_sql).unwrap();
    let result = executor.execute(statement).unwrap();
    assert!(matches!(result, SqlResult::CreateTable { .. }));

    // Insert data
    let insert_sql = "INSERT INTO products (id, name, price) VALUES (1, 'Laptop', 999.99), (2, 'Mouse', 29.99)";
    let (_, statement) = parse_sql(insert_sql).unwrap();
    let result = executor.execute(statement).unwrap();
    assert!(matches!(result, SqlResult::Insert { rows_affected: 2 }));

    // Select all
    let select_sql = "SELECT * FROM products";
    let (_, statement) = parse_sql(select_sql).unwrap();
    let result = executor.execute(statement).unwrap();
    
    if let SqlResult::Select { columns: _, rows } = result {
        assert_eq!(rows.len(), 2);
    } else {
        panic!("Expected Select result");
    }

    // Select with WHERE
    let select_where_sql = "SELECT name FROM products WHERE price > 50.0";
    let (_, statement) = parse_sql(select_where_sql).unwrap();
    let result = executor.execute(statement).unwrap();
    
    if let SqlResult::Select { columns, rows } = result {
        assert_eq!(columns, vec!["name"]);
        assert_eq!(rows.len(), 1);
    } else {
        panic!("Expected Select result");
    }

    // Update
    let update_sql = "UPDATE products SET price = 899.99 WHERE name = 'Laptop'";
    let (_, statement) = parse_sql(update_sql).unwrap();
    let result = executor.execute(statement).unwrap();
    assert!(matches!(result, SqlResult::Update { rows_affected: 1 }));

    // Delete
    let delete_sql = "DELETE FROM products WHERE price < 50.0";
    let (_, statement) = parse_sql(delete_sql).unwrap();
    let result = executor.execute(statement).unwrap();
    assert!(matches!(result, SqlResult::Delete { rows_affected: 1 }));

    // Verify final state
    let final_select_sql = "SELECT * FROM products";
    let (_, statement) = parse_sql(final_select_sql).unwrap();
    let result = executor.execute(statement).unwrap();
    
    if let SqlResult::Select { columns: _, rows } = result {
        assert_eq!(rows.len(), 1); // Only the laptop should remain
    } else {
        panic!("Expected Select result");
    }
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
        let mut executor = SqlExecutor::new(engine);

        let create_sql = "CREATE TABLE settings (key TEXT PRIMARY KEY, value TEXT)";
        let (_, statement) = parse_sql(create_sql).unwrap();
        executor.execute(statement).unwrap();

        let insert_sql = "INSERT INTO settings (key, value) VALUES ('theme', 'dark'), ('language', 'en')";
        let (_, statement) = parse_sql(insert_sql).unwrap();
        executor.execute(statement).unwrap();
    }

    // Second session - verify data persists
    {
        let engine = Engine::new(db_path).unwrap();
        let mut executor = SqlExecutor::new(engine);

        let select_sql = "SELECT * FROM settings";
        let (_, statement) = parse_sql(select_sql).unwrap();
        let result = executor.execute(statement).unwrap();
        
        if let SqlResult::Select { columns: _, rows } = result {
            assert_eq!(rows.len(), 2);
        } else {
            panic!("Expected Select result");
        }
    }
}
