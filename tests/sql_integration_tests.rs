use tegdb::Database;

mod test_helpers;
use crate::test_helpers::run_with_both_backends;

#[test]
fn test_sql_integration_basic_operations() {
    let _ = run_with_both_backends("test_sql_integration_basic_operations", |db_path| {
        let mut db = Database::open(db_path).unwrap();

        // Create table
        let create_sql =
            "CREATE TABLE products (id INTEGER PRIMARY KEY, name TEXT NOT NULL, price REAL)";
        let result = db.execute(create_sql).unwrap();
        assert_eq!(result, 0); // CREATE TABLE returns 0 affected rows

        // Insert data
        let insert_sql =
            "INSERT INTO products (id, name, price) VALUES (1, 'Laptop', 999.99), (2, 'Mouse', 29.99)";
        let result = db.execute(insert_sql).unwrap();
        assert_eq!(result, 2); // 2 rows inserted

        // Select all
        let select_sql = "SELECT * FROM products";
        let result = db.query(select_sql).unwrap();
        assert_eq!(result.len(), 2);

        // Select with WHERE
        let select_where_sql = "SELECT name FROM products WHERE price > 50.0";
        let result = db.query(select_where_sql).unwrap();
        assert_eq!(result.columns(), ["name"]);
        assert_eq!(result.len(), 1);

        // Update
        let update_sql = "UPDATE products SET price = 899.99 WHERE name = 'Laptop'";
        let result = db.execute(update_sql).unwrap();
        assert_eq!(result, 1); // 1 row updated

        // Delete
        let delete_sql = "DELETE FROM products WHERE price < 50.0";
        let result = db.execute(delete_sql).unwrap();
        assert_eq!(result, 1); // 1 row deleted

        // Verify final state
        let final_select_sql = "SELECT * FROM products";
        let result = db.query(final_select_sql).unwrap();
        assert_eq!(result.len(), 1); // Only the laptop should remain
        Ok(())
    });
}

#[test]
fn test_sql_parser_edge_cases() {
    let _ = run_with_both_backends("test_sql_parser_edge_cases", |db_path| {
        let mut db = Database::open(db_path).unwrap();

        // Test case-insensitive keywords
        db.execute("CREATE TABLE users (id INTEGER PRIMARY KEY, age INTEGER)")
            .unwrap();
        let sql = "select * from users where age > 18";
        let result = db.query(sql);
        assert!(result.is_ok());

        // Test with extra whitespace
        let sql = "   SELECT   *   FROM   users   WHERE   age   >   18   ";
        let result = db.query(sql);
        assert!(result.is_ok());

        // Test trailing semicolon
        let sql = "SELECT * FROM users;";
        let result = db.query(sql);
        assert!(result.is_ok());
        Ok(())
    });
}

#[test]
fn test_sql_integration_transaction_isolation() {
    let _ = run_with_both_backends("test_sql_integration_transaction_isolation", |db_path| {
        let mut db = Database::open(db_path).unwrap();

        // Setup initial data
        db.execute("CREATE TABLE accounts (id INTEGER PRIMARY KEY, balance REAL)")
            .unwrap();
        db.execute("INSERT INTO accounts (id, balance) VALUES (1, 100.0), (2, 200.0)")
            .unwrap();

        // Test transaction isolation using explicit transactions
        {
            let mut tx = db.begin_transaction().unwrap();
            tx.execute("UPDATE accounts SET balance = 150.0 WHERE id = 1")
                .unwrap();

            let result = tx
                .query("SELECT balance FROM accounts WHERE id = 1")
                .unwrap();
            // Within the transaction, the change should be visible
            assert_eq!(result.len(), 1);

            tx.commit().unwrap();
        }
        Ok(())
    });
}

#[test]
fn test_sql_integration_constraints() {
    let _ = run_with_both_backends("test_sql_integration_constraints", |db_path| {
        let mut db = Database::open(db_path).unwrap();

        // Create table with constraints
        let create_sql =
            "CREATE TABLE users (id INTEGER PRIMARY KEY, email TEXT UNIQUE NOT NULL, age INTEGER)";
        db.execute(create_sql).unwrap();

        // Insert valid data
        let insert_sql = "INSERT INTO users (id, email, age) VALUES (1, 'alice@example.com', 30)";
        let result = db.execute(insert_sql);
        assert!(result.is_ok());

        // Try to insert duplicate primary key - should fail
        let duplicate_pk_sql =
            "INSERT INTO users (id, email, age) VALUES (1, 'bob@example.com', 25)";
        let result = db.execute(duplicate_pk_sql);
        assert!(result.is_err()); // Should fail due to primary key constraint
        Ok(())
    });
}

#[test]
fn test_sql_integration_complex_queries() {
    let _ = run_with_both_backends("test_sql_integration_complex_queries", |db_path| {
        let mut db = Database::open(db_path).unwrap();

        // Create table
        let create_sql =
            "CREATE TABLE sales (id INTEGER PRIMARY KEY, product TEXT, amount REAL, date TEXT)";
        db.execute(create_sql).unwrap();

        // Insert test data
        let sales_data = vec![
            "INSERT INTO sales (id, product, amount, date) VALUES (1, 'Laptop', 1000.0, '2024-01-01')",
            "INSERT INTO sales (id, product, amount, date) VALUES (2, 'Mouse', 25.0, '2024-01-02')",
            "INSERT INTO sales (id, product, amount, date) VALUES (3, 'Laptop', 1200.0, '2024-01-03')",
            "INSERT INTO sales (id, product, amount, date) VALUES (4, 'Keyboard', 75.0, '2024-01-04')",
        ];

        for sql in sales_data {
            db.execute(sql).unwrap();
        }

        // Test complex WHERE conditions
        let complex_where_sql = "SELECT * FROM sales WHERE amount > 100.0 AND product = 'Laptop'";
        let result = db.query(complex_where_sql).unwrap();
        assert_eq!(result.len(), 2); // Should find both laptop sales
        Ok(())
    });
}

#[test]
fn test_sql_drop_table() {
    let _ = run_with_both_backends("test_sql_drop_table", |db_path| {
        let mut db = Database::open(db_path).unwrap();

        // Create table
        let create_sql = "CREATE TABLE temp_table (id INTEGER PRIMARY KEY, data TEXT)";
        db.execute(create_sql).unwrap();

        // Drop table
        let drop_sql = "DROP TABLE temp_table";
        let result = db.execute(drop_sql).unwrap();
        assert_eq!(result, 0); // DROP TABLE returns 0 affected rows
        Ok(())
    });
}
