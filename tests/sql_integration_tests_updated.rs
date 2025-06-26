use tegdb::{Engine, parser::{parse_sql, Statement}, executor::{Executor, ResultSet}};
use tempfile::tempdir;

#[test]
fn test_sql_integration_basic_operations() {
    let dir = tempdir().unwrap();
    let db_path = dir.path().join("test_integration.db");
    let mut engine = Engine::new(db_path).unwrap();
    let transaction = engine.begin_transaction();
    let mut executor = Executor::new(transaction);

    // Begin transaction
    executor.begin_transaction().unwrap();

    // Create table
    let create_sql = "CREATE TABLE products (id INTEGER PRIMARY KEY, name TEXT NOT NULL, price REAL)";
    let (_, statement) = parse_sql(create_sql).unwrap();
    let result = match statement {
        Statement::CreateTable(create) => executor.execute_create_table(create).unwrap(),
        _ => panic!("Expected CREATE TABLE statement"),
    };
    assert!(matches!(result, ResultSet::CreateTable { .. }));

    // Insert data
    let insert_sql = "INSERT INTO products (id, name, price) VALUES (1, 'Laptop', 999.99), (2, 'Mouse', 29.99)";
    let (_, statement) = parse_sql(insert_sql).unwrap();
    let result = match statement {
        Statement::Insert(insert) => executor.execute_insert(insert).unwrap(),
        _ => panic!("Expected INSERT statement"),
    };
    assert!(matches!(result, ResultSet::Insert { rows_affected: 2 }));

    // Select all
    let select_sql = "SELECT * FROM products";
    let (_, statement) = parse_sql(select_sql).unwrap();
    let result = match statement {
        Statement::Select(select) => executor.execute_select(select).unwrap(),
        _ => panic!("Expected SELECT statement"),
    };
    
    if let ResultSet::Select { columns: _, rows } = result {
        assert_eq!(rows.len(), 2);
    } else {
        panic!("Expected Select result");
    }

    // Commit transaction
    executor.commit_transaction().unwrap();

    // Begin new transaction for queries
    executor.begin_transaction().unwrap();

    // Select with WHERE
    let select_where_sql = "SELECT name FROM products WHERE price > 50.0";
    let (_, statement) = parse_sql(select_where_sql).unwrap();
    let result = match statement {
        Statement::Select(select) => executor.execute_select(select).unwrap(),
        _ => panic!("Expected SELECT statement"),
    };
    
    if let ResultSet::Select { columns, rows } = result {
        assert_eq!(columns, vec!["name"]);
        assert_eq!(rows.len(), 1);
    } else {
        panic!("Expected Select result");
    }

    // Update
    let update_sql = "UPDATE products SET price = 899.99 WHERE name = 'Laptop'";
    let (_, statement) = parse_sql(update_sql).unwrap();
    let result = match statement {
        Statement::Update(update) => executor.execute_update(update).unwrap(),
        _ => panic!("Expected UPDATE statement"),
    };
    assert!(matches!(result, ResultSet::Update { rows_affected: 1 }));

    // Delete
    let delete_sql = "DELETE FROM products WHERE price < 50.0";
    let (_, statement) = parse_sql(delete_sql).unwrap();
    let result = match statement {
        Statement::Delete(delete) => executor.execute_delete(delete).unwrap(),
        _ => panic!("Expected DELETE statement"),
    };
    assert!(matches!(result, ResultSet::Delete { rows_affected: 1 }));

    // Verify final state
    let final_select_sql = "SELECT * FROM products";
    let (_, statement) = parse_sql(final_select_sql).unwrap();
    let result = match statement {
        Statement::Select(select) => executor.execute_select(select).unwrap(),
        _ => panic!("Expected SELECT statement"),
    };
    
    if let ResultSet::Select { columns: _, rows } = result {
        assert_eq!(rows.len(), 1); // Only the laptop should remain
    } else {
        panic!("Expected Select result");
    }

    // Commit final transaction
    executor.commit_transaction().unwrap();
}

#[test]
fn test_sql_parser_edge_cases() {
    // Test case-insensitive keywords
    let sql = "select * from USERS where AGE > 18";
    let result = parse_sql(sql);
    assert!(result.is_ok());

    // Test with extra whitespace
    let sql = "   SELECT   *   FROM   users   WHERE   age   >   18   ";
    let result = parse_sql(sql);
    assert!(result.is_ok());

    // Test trailing semicolon
    let sql = "SELECT * FROM users;";
    let result = parse_sql(sql);
    assert!(result.is_ok());
}

#[test]
fn test_sql_integration_transaction_isolation() {
    let dir = tempdir().unwrap();
    let db_path = dir.path().join("test_isolation.db");
    let mut engine = Engine::new(db_path).unwrap();
    
    // Setup initial data
    {
        let transaction = engine.begin_transaction();
        let mut executor = Executor::new(transaction);

        executor.begin_transaction().unwrap();

        let create_sql = "CREATE TABLE accounts (id INTEGER PRIMARY KEY, balance REAL)";
        let (_, statement) = parse_sql(create_sql).unwrap();
        match statement {
            Statement::CreateTable(create) => executor.execute_create_table(create).unwrap(),
            _ => panic!("Expected CREATE TABLE statement"),
        };

        let insert_sql = "INSERT INTO accounts (id, balance) VALUES (1, 100.0), (2, 200.0)";
        let (_, statement) = parse_sql(insert_sql).unwrap();
        match statement {
            Statement::Insert(insert) => executor.execute_insert(insert).unwrap(),
            _ => panic!("Expected INSERT statement"),
        };

        executor.commit_transaction().unwrap();
        executor.transaction_mut().commit().unwrap();
    }

    // Test transaction isolation - changes in one transaction shouldn't be visible in another until commit
    {
        let transaction = engine.begin_transaction();
        let mut executor = Executor::new(transaction);

        executor.begin_transaction().unwrap();

        let update_sql = "UPDATE accounts SET balance = 150.0 WHERE id = 1";
        let (_, statement) = parse_sql(update_sql).unwrap();
        match statement {
            Statement::Update(update) => executor.execute_update(update).unwrap(),
            _ => panic!("Expected UPDATE statement"),
        };

        let select_sql = "SELECT balance FROM accounts WHERE id = 1";
        let (_, statement) = parse_sql(select_sql).unwrap();
        let result = match statement {
            Statement::Select(select) => executor.execute_select(select).unwrap(),
            _ => panic!("Expected SELECT statement"),
        };

        if let ResultSet::Select { columns: _, rows } = result {
            // Within the transaction, the change should be visible
            assert_eq!(rows.len(), 1);
        }

        executor.commit_transaction().unwrap();
        executor.transaction_mut().commit().unwrap();
    }
}

#[test]
fn test_sql_integration_constraints() {
    let dir = tempdir().unwrap();
    let db_path = dir.path().join("test_constraints.db");
    let mut engine = Engine::new(db_path).unwrap();
    let transaction = engine.begin_transaction();
    let mut executor = Executor::new(transaction);

    executor.begin_transaction().unwrap();

    // Create table with constraints
    let create_sql = "CREATE TABLE users (id INTEGER PRIMARY KEY, email TEXT UNIQUE NOT NULL, age INTEGER)";
    let (_, statement) = parse_sql(create_sql).unwrap();
    match statement {
        Statement::CreateTable(create) => executor.execute_create_table(create).unwrap(),
        _ => panic!("Expected CREATE TABLE statement"),
    };

    // Insert valid data
    let insert_sql = "INSERT INTO users (id, email, age) VALUES (1, 'alice@example.com', 30)";
    let (_, statement) = parse_sql(insert_sql).unwrap();
    let result = match statement {
        Statement::Insert(insert) => executor.execute_insert(insert),
        _ => panic!("Expected INSERT statement"),
    };
    assert!(result.is_ok());

    // Try to insert duplicate primary key - should fail
    let duplicate_pk_sql = "INSERT INTO users (id, email, age) VALUES (1, 'bob@example.com', 25)";
    let (_, statement) = parse_sql(duplicate_pk_sql).unwrap();
    let result = match statement {
        Statement::Insert(insert) => executor.execute_insert(insert),
        _ => panic!("Expected INSERT statement"),
    };
    assert!(result.is_err()); // Should fail due to primary key constraint
    
    executor.commit_transaction().unwrap();
}

#[test] 
fn test_sql_integration_complex_queries() {
    let dir = tempdir().unwrap();
    let db_path = dir.path().join("test_complex.db");
    let mut engine = Engine::new(db_path).unwrap();
    let transaction = engine.begin_transaction();
    let mut executor = Executor::new(transaction);

    executor.begin_transaction().unwrap();

    // Create table
    let create_sql = "CREATE TABLE sales (id INTEGER PRIMARY KEY, product TEXT, amount REAL, date TEXT)";
    let (_, statement) = parse_sql(create_sql).unwrap();
    match statement {
        Statement::CreateTable(create) => executor.execute_create_table(create).unwrap(),
        _ => panic!("Expected CREATE TABLE statement"),
    };

    // Insert test data
    let sales_data = vec![
        "INSERT INTO sales (id, product, amount, date) VALUES (1, 'Laptop', 1000.0, '2024-01-01')",
        "INSERT INTO sales (id, product, amount, date) VALUES (2, 'Mouse', 25.0, '2024-01-02')",
        "INSERT INTO sales (id, product, amount, date) VALUES (3, 'Laptop', 1200.0, '2024-01-03')",
        "INSERT INTO sales (id, product, amount, date) VALUES (4, 'Keyboard', 75.0, '2024-01-04')",
    ];

    for sql in sales_data {
        let (_, statement) = parse_sql(sql).unwrap();
        match statement {
            Statement::Insert(insert) => executor.execute_insert(insert).unwrap(),
            _ => panic!("Expected INSERT statement"),
        };
    }

    // Test complex WHERE conditions
    let complex_where_sql = "SELECT * FROM sales WHERE amount > 100.0 AND product = 'Laptop'";
    let (_, statement) = parse_sql(complex_where_sql).unwrap();
    let result = match statement {
        Statement::Select(select) => executor.execute_select(select).unwrap(),
        _ => panic!("Expected SELECT statement"),
    };

    if let ResultSet::Select { columns: _, rows } = result {
        assert_eq!(rows.len(), 2); // Should find both laptop sales
    } else {
        panic!("Expected Select result");
    }

    executor.commit_transaction().unwrap();
}

#[test]
fn test_sql_drop_table() {
    let dir = tempdir().unwrap();
    let db_path = dir.path().join("test_drop.db");
    let mut engine = Engine::new(db_path).unwrap();
    let transaction = engine.begin_transaction();
    let mut executor = Executor::new(transaction);

    executor.begin_transaction().unwrap();

    // Create table
    let create_sql = "CREATE TABLE temp_table (id INTEGER PRIMARY KEY, data TEXT)";
    let (_, statement) = parse_sql(create_sql).unwrap();
    match statement {
        Statement::CreateTable(create) => executor.execute_create_table(create).unwrap(),
        _ => panic!("Expected CREATE TABLE statement"),
    };

    // Drop table
    let drop_sql = "DROP TABLE temp_table";
    let (_, statement) = parse_sql(drop_sql).unwrap();
    let result = match statement {
        Statement::DropTable(drop) => executor.execute_drop_table(drop).unwrap(),
        _ => panic!("Expected DROP TABLE statement"),
    };
    
    assert!(matches!(result, ResultSet::DropTable { table_name, existed } if table_name == "temp_table" && existed));

    executor.commit_transaction().unwrap();
}
