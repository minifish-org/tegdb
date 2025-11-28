mod test_helpers {
    include!("../common/test_helpers.rs");
}
use test_helpers::run_with_both_backends;

use tegdb::{Database, Result, SqlValue};

#[test]
fn test_multiple_where_conditions_and() -> Result<()> {
    run_with_both_backends("test_multiple_where_conditions_and", |db_path| {
        let mut db = Database::open(db_path)?;

        db.execute("CREATE TABLE products (id INTEGER PRIMARY KEY, name TEXT(32), price REAL, category TEXT(32))")?;
        db.execute("INSERT INTO products (id, name, price, category) VALUES (1, 'Laptop', 999.99, 'Electronics'), (2, 'Mouse', 29.99, 'Electronics'), (3, 'Desk', 199.99, 'Furniture')")?;

        // Multiple AND conditions
        let result =
            db.query("SELECT name FROM products WHERE category = 'Electronics' AND price > 50.0")?;
        assert_eq!(result.len(), 1);
        assert_eq!(result.rows()[0][0], SqlValue::Text("Laptop".to_string()));

        // Three AND conditions
        let result = db.query(
            "SELECT name FROM products WHERE category = 'Electronics' AND price > 50.0 AND id < 10",
        )?;
        assert_eq!(result.len(), 1);

        Ok(())
    })
}

#[test]
fn test_multiple_where_conditions_or() -> Result<()> {
    run_with_both_backends("test_multiple_where_conditions_or", |db_path| {
        let mut db = Database::open(db_path)?;

        db.execute("CREATE TABLE items (id INTEGER PRIMARY KEY, status TEXT(32), value INTEGER)")?;
        db.execute("INSERT INTO items (id, status, value) VALUES (1, 'active', 100), (2, 'inactive', 200), (3, 'pending', 150)")?;

        // OR conditions
        let result =
            db.query("SELECT id FROM items WHERE status = 'active' OR status = 'pending'")?;
        assert_eq!(result.len(), 2);

        // Multiple OR conditions
        let result =
            db.query("SELECT id FROM items WHERE value = 100 OR value = 200 OR value = 300")?;
        assert_eq!(result.len(), 2);

        Ok(())
    })
}

#[test]
fn test_nested_where_conditions() -> Result<()> {
    run_with_both_backends("test_nested_where_conditions", |db_path| {
        let mut db = Database::open(db_path)?;

        db.execute("CREATE TABLE employees (id INTEGER PRIMARY KEY, department TEXT(32), salary REAL, age INTEGER)")?;
        db.execute("INSERT INTO employees (id, department, salary, age) VALUES (1, 'Engineering', 100000, 30), (2, 'Sales', 80000, 25), (3, 'Engineering', 120000, 35)")?;

        // Nested parentheses with AND and OR
        let result = db.query("SELECT id FROM employees WHERE (department = 'Engineering' AND salary > 90000) OR (department = 'Sales' AND age < 30)")?;
        assert_eq!(result.len(), 3); // All three match

        // Complex nested condition
        let result = db.query("SELECT id FROM employees WHERE ((department = 'Engineering') AND (salary > 110000 OR age > 32))")?;
        assert_eq!(result.len(), 1);
        assert_eq!(result.rows()[0][0], SqlValue::Integer(3));

        Ok(())
    })
}

#[test]
fn test_order_by_multiple_columns() -> Result<()> {
    run_with_both_backends("test_order_by_multiple_columns", |db_path| {
        let mut db = Database::open(db_path)?;

        db.execute("CREATE TABLE students (id INTEGER PRIMARY KEY, name TEXT(32), grade INTEGER, score REAL)")?;
        db.execute("INSERT INTO students (id, name, grade, score) VALUES (1, 'Alice', 10, 95.5), (2, 'Bob', 10, 88.0), (3, 'Charlie', 9, 92.0), (4, 'David', 10, 88.0)")?;

        // Order by multiple columns - grade first, then score
        let result =
            db.query("SELECT name, grade, score FROM students ORDER BY grade, score DESC")?;
        let rows = result.rows();
        assert_eq!(rows.len(), 4);

        // Verify ordering: grade 9 first, then grade 10 sorted by score descending
        assert_eq!(rows[0][1], SqlValue::Integer(9)); // Charlie
        assert_eq!(rows[1][1], SqlValue::Integer(10)); // Alice (higher score)
        assert_eq!(rows[2][1], SqlValue::Integer(10)); // Bob or David
        assert_eq!(rows[3][1], SqlValue::Integer(10)); // Bob or David

        Ok(())
    })
}

#[test]
fn test_order_by_ascending_descending() -> Result<()> {
    run_with_both_backends("test_order_by_ascending_descending", |db_path| {
        let mut db = Database::open(db_path)?;

        db.execute("CREATE TABLE numbers (id INTEGER PRIMARY KEY, value INTEGER)")?;
        db.execute("INSERT INTO numbers (id, value) VALUES (1, 30), (2, 10), (3, 20)")?;

        // Ascending order
        let result = db.query("SELECT value FROM numbers ORDER BY value ASC")?;
        let rows = result.rows();
        assert_eq!(rows[0][0], SqlValue::Integer(10));
        assert_eq!(rows[1][0], SqlValue::Integer(20));
        assert_eq!(rows[2][0], SqlValue::Integer(30));

        // Descending order
        let result = db.query("SELECT value FROM numbers ORDER BY value DESC")?;
        let rows = result.rows();
        assert_eq!(rows[0][0], SqlValue::Integer(30));
        assert_eq!(rows[1][0], SqlValue::Integer(20));
        assert_eq!(rows[2][0], SqlValue::Integer(10));

        Ok(())
    })
}

#[test]
fn test_order_by_different_data_types() -> Result<()> {
    run_with_both_backends("test_order_by_different_data_types", |db_path| {
        let mut db = Database::open(db_path)?;

        db.execute("CREATE TABLE mixed (id INTEGER PRIMARY KEY, int_val INTEGER, real_val REAL, text_val TEXT(32))")?;
        db.execute("INSERT INTO mixed (id, int_val, real_val, text_val) VALUES (1, 30, 3.5, 'zebra'), (2, 10, 1.2, 'apple'), (3, 20, 2.8, 'banana')")?;

        // Order by INTEGER
        let result = db.query("SELECT int_val FROM mixed ORDER BY int_val")?;
        let rows = result.rows();
        assert_eq!(rows[0][0], SqlValue::Integer(10));
        assert_eq!(rows[1][0], SqlValue::Integer(20));
        assert_eq!(rows[2][0], SqlValue::Integer(30));

        // Order by REAL
        let result = db.query("SELECT real_val FROM mixed ORDER BY real_val")?;
        let rows = result.rows();
        if let (SqlValue::Real(v1), SqlValue::Real(v2), SqlValue::Real(v3)) =
            (&rows[0][0], &rows[1][0], &rows[2][0])
        {
            assert!(v1 < v2 && v2 < v3);
        }

        // Order by TEXT
        let result = db.query("SELECT text_val FROM mixed ORDER BY text_val")?;
        let rows = result.rows();
        assert_eq!(rows[0][0], SqlValue::Text("apple".to_string()));
        assert_eq!(rows[1][0], SqlValue::Text("banana".to_string()));
        assert_eq!(rows[2][0], SqlValue::Text("zebra".to_string()));

        Ok(())
    })
}

#[test]
fn test_where_with_arithmetic_expressions() -> Result<()> {
    run_with_both_backends("test_where_with_arithmetic_expressions", |db_path| {
        let mut db = Database::open(db_path)?;

        db.execute("CREATE TABLE calculations (id INTEGER PRIMARY KEY, a INTEGER, b INTEGER)")?;
        db.execute(
            "INSERT INTO calculations (id, a, b) VALUES (1, 10, 5), (2, 20, 10), (3, 15, 3)",
        )?;

        // WHERE with addition
        let result = db.query("SELECT id FROM calculations WHERE a + b > 20")?;
        assert_eq!(result.len(), 2);

        // WHERE with subtraction
        let result = db.query("SELECT id FROM calculations WHERE a - b < 10")?;
        assert_eq!(result.len(), 1);

        // WHERE with multiplication
        let result = db.query("SELECT id FROM calculations WHERE a * b = 200")?;
        assert_eq!(result.len(), 1);
        assert_eq!(result.rows()[0][0], SqlValue::Integer(2));

        // WHERE with division
        let result = db.query("SELECT id FROM calculations WHERE a / b = 5")?;
        assert_eq!(result.len(), 1);

        Ok(())
    })
}

#[test]
fn test_where_with_function_calls() -> Result<()> {
    run_with_both_backends("test_where_with_function_calls", |db_path| {
        let mut db = Database::open(db_path)?;

        // Register extension for function testing
        db.execute("CREATE EXTENSION tegdb_string")?;

        db.execute("CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT(32), email TEXT(50))")?;
        db.execute("INSERT INTO users (id, name, email) VALUES (1, 'alice', 'ALICE@EXAMPLE.COM'), (2, 'Bob', 'bob@example.com')")?;

        // WHERE with UPPER function
        let result = db.query("SELECT id FROM users WHERE UPPER(name) = 'ALICE'")?;
        assert_eq!(result.len(), 1);
        assert_eq!(result.rows()[0][0], SqlValue::Integer(1));

        // WHERE with LENGTH function
        let result = db.query("SELECT id FROM users WHERE LENGTH(name) > 3")?;
        assert_eq!(result.len(), 1);
        assert_eq!(result.rows()[0][0], SqlValue::Integer(1));

        Ok(())
    })
}

#[test]
fn test_select_with_computed_columns() -> Result<()> {
    run_with_both_backends("test_select_with_computed_columns", |db_path| {
        let mut db = Database::open(db_path)?;

        db.execute("CREATE TABLE sales (id INTEGER PRIMARY KEY, quantity INTEGER, price REAL)")?;
        db.execute("INSERT INTO sales (id, quantity, price) VALUES (1, 5, 10.0), (2, 3, 15.0)")?;

        // Computed column with multiplication
        let result = db.query("SELECT id, quantity * price AS total FROM sales")?;
        assert_eq!(result.len(), 2);
        assert_eq!(result.rows()[0][1], SqlValue::Real(50.0));
        assert_eq!(result.rows()[1][1], SqlValue::Real(45.0));

        // Computed column with addition
        let result = db.query("SELECT id, quantity + 10 AS adjusted_quantity FROM sales")?;
        assert_eq!(result.len(), 2);
        assert_eq!(result.rows()[0][1], SqlValue::Integer(15));
        assert_eq!(result.rows()[1][1], SqlValue::Integer(13));

        Ok(())
    })
}

#[test]
fn test_complex_query_combinations() -> Result<()> {
    run_with_both_backends("test_complex_query_combinations", |db_path| {
        let mut db = Database::open(db_path)?;

        db.execute("CREATE TABLE orders (id INTEGER PRIMARY KEY, customer_id INTEGER, amount REAL, status TEXT(32))")?;
        db.execute("INSERT INTO orders (id, customer_id, amount, status) VALUES (1, 1, 100.0, 'completed'), (2, 1, 200.0, 'pending'), (3, 2, 150.0, 'completed'), (4, 2, 50.0, 'cancelled')")?;

        // Complex query: WHERE with AND/OR, ORDER BY, LIMIT
        let result = db.query("SELECT id, amount FROM orders WHERE (status = 'completed' OR status = 'pending') AND amount > 100.0 ORDER BY amount DESC LIMIT 2")?;
        assert_eq!(result.len(), 2);

        // Verify ordering
        let rows = result.rows();
        if let (SqlValue::Real(a1), SqlValue::Real(a2)) = (&rows[0][1], &rows[1][1]) {
            assert!(a1 >= a2);
        }

        Ok(())
    })
}

#[test]
fn test_multiple_table_operations_in_sequence() -> Result<()> {
    run_with_both_backends("test_multiple_table_operations_in_sequence", |db_path| {
        let mut db = Database::open(db_path)?;

        // Create multiple tables
        db.execute("CREATE TABLE table1 (id INTEGER PRIMARY KEY, value INTEGER)")?;
        db.execute("CREATE TABLE table2 (id INTEGER PRIMARY KEY, value INTEGER)")?;
        db.execute("CREATE TABLE table3 (id INTEGER PRIMARY KEY, value INTEGER)")?;

        // Insert into each table
        for i in 1..=5 {
            db.execute(&format!(
                "INSERT INTO table1 (id, value) VALUES ({}, {})",
                i,
                i * 10
            ))?;
            db.execute(&format!(
                "INSERT INTO table2 (id, value) VALUES ({}, {})",
                i,
                i * 20
            ))?;
            db.execute(&format!(
                "INSERT INTO table3 (id, value) VALUES ({}, {})",
                i,
                i * 30
            ))?;
        }

        // Query each table
        let result1 = db.query("SELECT COUNT(*) FROM table1")?;
        assert_eq!(result1.rows()[0][0], SqlValue::Integer(5));

        let result2 = db.query("SELECT SUM(value) FROM table2")?;
        assert_eq!(result2.rows()[0][0], SqlValue::Real(300.0)); // Sum of 20, 40, 60, 80, 100

        let result3 = db.query("SELECT AVG(value) FROM table3")?;
        assert_eq!(result3.rows()[0][0], SqlValue::Real(90.0)); // Average of 30, 60, 90, 120, 150

        // Update operations
        db.execute("UPDATE table1 SET value = 999 WHERE id = 1")?;
        let result = db.query("SELECT value FROM table1 WHERE id = 1")?;
        assert_eq!(result.rows()[0][0], SqlValue::Integer(999));

        // Delete operations
        db.execute("DELETE FROM table2 WHERE id > 3")?;
        let result = db.query("SELECT COUNT(*) FROM table2")?;
        assert_eq!(result.rows()[0][0], SqlValue::Integer(3));

        Ok(())
    })
}
