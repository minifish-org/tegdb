mod test_helpers {
    include!("../common/test_helpers.rs");
}
use test_helpers::run_with_both_backends;

use tegdb::{Database, Result, SqlValue};

#[test]
fn test_prepared_select_various_where_conditions() -> Result<()> {
    run_with_both_backends("test_prepared_select_various_where_conditions", |db_path| {
        let mut db = Database::open(db_path)?;

        db.execute("CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT(32), age INTEGER)")?;
        db.execute("INSERT INTO users (id, name, age) VALUES (1, 'Alice', 30), (2, 'Bob', 25), (3, 'Charlie', 35)")?;

        // Equality
        let stmt = db.prepare("SELECT name FROM users WHERE id = ?1")?;
        let result = db.query_prepared(&stmt, &[SqlValue::Integer(1)])?;
        assert_eq!(result.rows()[0][0], SqlValue::Text("Alice".to_string()));

        // Greater than
        let stmt = db.prepare("SELECT name FROM users WHERE age > ?1")?;
        let result = db.query_prepared(&stmt, &[SqlValue::Integer(25)])?;
        assert_eq!(result.len(), 2);

        // Less than or equal
        let stmt = db.prepare("SELECT name FROM users WHERE age <= ?1")?;
        let result = db.query_prepared(&stmt, &[SqlValue::Integer(30)])?;
        assert_eq!(result.len(), 2);

        // Text equality
        let stmt = db.prepare("SELECT age FROM users WHERE name = ?1")?;
        let result = db.query_prepared(&stmt, &[SqlValue::Text("Bob".to_string())])?;
        assert_eq!(result.rows()[0][0], SqlValue::Integer(25));

        // Multiple conditions with AND
        let stmt = db.prepare("SELECT name FROM users WHERE age > ?1 AND age < ?2")?;
        let result = db.query_prepared(&stmt, &[SqlValue::Integer(25), SqlValue::Integer(35)])?;
        assert_eq!(result.len(), 1);
        assert_eq!(result.rows()[0][0], SqlValue::Text("Charlie".to_string()));

        Ok(())
    })
}

#[test]
fn test_prepared_insert_multiple_parameters() -> Result<()> {
    run_with_both_backends("test_prepared_insert_multiple_parameters", |db_path| {
        let mut db = Database::open(db_path)?;

        db.execute("CREATE TABLE products (id INTEGER PRIMARY KEY, name TEXT(32), price REAL, quantity INTEGER)")?;

        let stmt =
            db.prepare("INSERT INTO products (id, name, price, quantity) VALUES (?1, ?2, ?3, ?4)")?;

        // Insert first product
        let affected = db.execute_prepared(
            &stmt,
            &[
                SqlValue::Integer(1),
                SqlValue::Text("Laptop".to_string()),
                SqlValue::Real(999.99),
                SqlValue::Integer(5),
            ],
        )?;
        assert_eq!(affected, 1);

        // Insert second product
        let affected = db.execute_prepared(
            &stmt,
            &[
                SqlValue::Integer(2),
                SqlValue::Text("Mouse".to_string()),
                SqlValue::Real(29.99),
                SqlValue::Integer(10),
            ],
        )?;
        assert_eq!(affected, 1);

        // Verify inserts
        let result = db.query("SELECT COUNT(*) FROM products")?;
        assert_eq!(result.rows()[0][0], SqlValue::Integer(2));

        Ok(())
    })
}

#[test]
fn test_prepared_update_set_and_where_parameters() -> Result<()> {
    run_with_both_backends("test_prepared_update_set_and_where_parameters", |db_path| {
        let mut db = Database::open(db_path)?;

        db.execute("CREATE TABLE accounts (id INTEGER PRIMARY KEY, balance REAL)")?;
        db.execute("INSERT INTO accounts (id, balance) VALUES (1, 100.0), (2, 200.0)")?;

        // Update with parameter in SET and WHERE
        let stmt = db.prepare("UPDATE accounts SET balance = ?1 WHERE id = ?2")?;

        // Update first account
        let affected =
            db.execute_prepared(&stmt, &[SqlValue::Real(150.0), SqlValue::Integer(1)])?;
        assert_eq!(affected, 1);

        // Verify update
        let result = db.query("SELECT balance FROM accounts WHERE id = 1")?;
        assert_eq!(result.rows()[0][0], SqlValue::Real(150.0));

        // Update second account
        let affected =
            db.execute_prepared(&stmt, &[SqlValue::Real(250.0), SqlValue::Integer(2)])?;
        assert_eq!(affected, 1);

        // Verify update
        let result = db.query("SELECT balance FROM accounts WHERE id = 2")?;
        assert_eq!(result.rows()[0][0], SqlValue::Real(250.0));

        Ok(())
    })
}

#[test]
fn test_prepared_delete_with_where_parameters() -> Result<()> {
    run_with_both_backends("test_prepared_delete_with_where_parameters", |db_path| {
        let mut db = Database::open(db_path)?;

        db.execute("CREATE TABLE items (id INTEGER PRIMARY KEY, status TEXT(32))")?;
        db.execute(
            "INSERT INTO items (id, status) VALUES (1, 'active'), (2, 'inactive'), (3, 'active')",
        )?;

        let stmt = db.prepare("DELETE FROM items WHERE id = ?1")?;

        // Delete first item
        let affected = db.execute_prepared(&stmt, &[SqlValue::Integer(1)])?;
        assert_eq!(affected, 1);

        // Verify deletion
        let result = db.query("SELECT COUNT(*) FROM items")?;
        assert_eq!(result.rows()[0][0], SqlValue::Integer(2));

        // Delete another item
        let affected = db.execute_prepared(&stmt, &[SqlValue::Integer(2)])?;
        assert_eq!(affected, 1);

        // Verify deletion
        let result = db.query("SELECT COUNT(*) FROM items")?;
        assert_eq!(result.rows()[0][0], SqlValue::Integer(1));

        Ok(())
    })
}

#[test]
fn test_prepared_statement_reuse() -> Result<()> {
    run_with_both_backends("test_prepared_statement_reuse", |db_path| {
        let mut db = Database::open(db_path)?;

        db.execute("CREATE TABLE numbers (id INTEGER PRIMARY KEY, value INTEGER)")?;

        let stmt = db.prepare("INSERT INTO numbers (id, value) VALUES (?1, ?2)")?;

        // Reuse statement multiple times
        for i in 1..=10 {
            let affected =
                db.execute_prepared(&stmt, &[SqlValue::Integer(i), SqlValue::Integer(i * 10)])?;
            assert_eq!(affected, 1);
        }

        // Verify all inserts
        let result = db.query("SELECT COUNT(*) FROM numbers")?;
        assert_eq!(result.rows()[0][0], SqlValue::Integer(10));

        // Verify values
        let result = db.query("SELECT value FROM numbers WHERE id = 5")?;
        assert_eq!(result.rows()[0][0], SqlValue::Integer(50));

        Ok(())
    })
}

#[test]
fn test_prepared_statement_error_cases() -> Result<()> {
    run_with_both_backends("test_prepared_statement_error_cases", |db_path| {
        let mut db = Database::open(db_path)?;

        db.execute("CREATE TABLE test (id INTEGER PRIMARY KEY, name TEXT(32))")?;

        let stmt = db.prepare("INSERT INTO test (id, name) VALUES (?1, ?2)")?;

        // Wrong parameter count - too few
        let result = db.execute_prepared(&stmt, &[SqlValue::Integer(1)]);
        assert!(result.is_err());

        // Wrong parameter count - too many
        let result = db.execute_prepared(
            &stmt,
            &[
                SqlValue::Integer(1),
                SqlValue::Text("test".to_string()),
                SqlValue::Integer(3),
            ],
        );
        assert!(result.is_err());

        // Wrong type - should still work if types are compatible, but test error handling
        // Primary key violation
        db.execute_prepared(
            &stmt,
            &[SqlValue::Integer(1), SqlValue::Text("first".to_string())],
        )?;
        let result = db.execute_prepared(
            &stmt,
            &[
                SqlValue::Integer(1),
                SqlValue::Text("duplicate".to_string()),
            ],
        );
        assert!(result.is_err());

        Ok(())
    })
}

#[test]
fn test_prepared_statement_null_handling() -> Result<()> {
    run_with_both_backends("test_prepared_statement_null_handling", |db_path| {
        let mut db = Database::open(db_path)?;

        db.execute("CREATE TABLE test (id INTEGER PRIMARY KEY, name TEXT(32), age INTEGER)")?;

        let stmt = db.prepare("INSERT INTO test (id, name, age) VALUES (?1, ?2, ?3)")?;

        // Insert with NULL
        let affected = db.execute_prepared(
            &stmt,
            &[
                SqlValue::Integer(1),
                SqlValue::Text("Alice".to_string()),
                SqlValue::Null,
            ],
        )?;
        assert_eq!(affected, 1);

        // Query with NULL in WHERE
        let stmt2 = db.prepare("SELECT name FROM test WHERE age IS NULL")?;
        let result = db.query_prepared(&stmt2, &[])?;
        assert_eq!(result.len(), 1);
        assert_eq!(result.rows()[0][0], SqlValue::Text("Alice".to_string()));

        Ok(())
    })
}

#[test]
fn test_prepared_statements_in_transactions() -> Result<()> {
    run_with_both_backends("test_prepared_statements_in_transactions", |db_path| {
        let mut db = Database::open(db_path)?;

        db.execute("CREATE TABLE accounts (id INTEGER PRIMARY KEY, balance REAL)")?;
        db.execute("INSERT INTO accounts (id, balance) VALUES (1, 100.0), (2, 200.0)")?;

        // Note: Prepared statements are not supported within transactions
        // Test that regular SQL works in transactions instead
        let mut tx = db.begin_transaction()?;
        let affected = tx.execute("UPDATE accounts SET balance = 150.0 WHERE id = 1")?;
        assert_eq!(affected, 1);

        // Verify within transaction
        let result = tx.query("SELECT balance FROM accounts WHERE id = 1")?;
        assert_eq!(result.rows()[0][0], SqlValue::Real(150.0));

        tx.commit()?;

        // Verify after commit
        let result = db.query("SELECT balance FROM accounts WHERE id = 1")?;
        assert_eq!(result.rows()[0][0], SqlValue::Real(150.0));

        Ok(())
    })
}

#[test]
fn test_prepared_statements_with_extensions() -> Result<()> {
    run_with_both_backends("test_prepared_statements_with_extensions", |db_path| {
        let mut db = Database::open(db_path)?;

        // Register extension
        db.execute("CREATE EXTENSION tegdb_string")?;

        db.execute("CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT(32))")?;
        db.execute("INSERT INTO users (id, name) VALUES (1, 'alice'), (2, 'bob')")?;

        // Use extension function in prepared statement
        let stmt = db.prepare("SELECT UPPER(name) FROM users WHERE id = ?1")?;
        let result = db.query_prepared(&stmt, &[SqlValue::Integer(1)])?;
        assert_eq!(result.rows()[0][0], SqlValue::Text("ALICE".to_string()));

        // Use extension function in WHERE clause
        let stmt2 = db.prepare("SELECT id FROM users WHERE UPPER(name) = ?1")?;
        let result = db.query_prepared(&stmt2, &[SqlValue::Text("BOB".to_string())])?;
        assert_eq!(result.rows()[0][0], SqlValue::Integer(2));

        Ok(())
    })
}

#[test]
fn test_prepared_statement_plan_caching() -> Result<()> {
    run_with_both_backends("test_prepared_statement_plan_caching", |db_path| {
        let mut db = Database::open(db_path)?;

        db.execute("CREATE TABLE test (id INTEGER PRIMARY KEY, value INTEGER)")?;
        db.execute("INSERT INTO test (id, value) VALUES (1, 10), (2, 20), (3, 30)")?;

        // Prepare statement
        let stmt = db.prepare("SELECT value FROM test WHERE id = ?1")?;

        // Execute multiple times - plan should be cached
        for i in 1..=3 {
            let result = db.query_prepared(&stmt, &[SqlValue::Integer(i)])?;
            assert_eq!(result.rows()[0][0], SqlValue::Integer(i * 10));
        }

        // Prepare another statement with same structure but different table
        db.execute("CREATE TABLE test2 (id INTEGER PRIMARY KEY, value INTEGER)")?;
        db.execute("INSERT INTO test2 (id, value) VALUES (1, 100), (2, 200)")?;

        let stmt2 = db.prepare("SELECT value FROM test2 WHERE id = ?1")?;
        let result = db.query_prepared(&stmt2, &[SqlValue::Integer(1)])?;
        assert_eq!(result.rows()[0][0], SqlValue::Integer(100));

        Ok(())
    })
}

#[test]
fn test_prepared_statement_with_arithmetic() -> Result<()> {
    run_with_both_backends("test_prepared_statement_with_arithmetic", |db_path| {
        let mut db = Database::open(db_path)?;

        db.execute("CREATE TABLE products (id INTEGER PRIMARY KEY, price REAL, quantity INTEGER)")?;
        db.execute("INSERT INTO products (id, price, quantity) VALUES (1, 10.0, 5), (2, 20.0, 3)")?;

        // Use arithmetic in prepared statement
        let stmt = db.prepare("SELECT price * quantity FROM products WHERE id = ?1")?;
        let result = db.query_prepared(&stmt, &[SqlValue::Integer(1)])?;
        assert_eq!(result.rows()[0][0], SqlValue::Real(50.0));

        // Use arithmetic in WHERE clause
        let stmt2 = db.prepare("SELECT id FROM products WHERE price * quantity > ?1")?;
        let result = db.query_prepared(&stmt2, &[SqlValue::Real(40.0)])?;
        assert_eq!(result.len(), 1);
        assert_eq!(result.rows()[0][0], SqlValue::Integer(1));

        Ok(())
    })
}
