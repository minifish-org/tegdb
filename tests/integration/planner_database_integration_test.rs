mod test_helpers {
    include!("../common/test_helpers.rs");
}
use test_helpers::run_with_both_backends;

use tegdb::{Database, Result};

#[test]
fn test_planner_integration_in_database() -> Result<()> {
    run_with_both_backends("test_planner_integration_in_database", |db_path| {
        let mut db = Database::open(db_path)?;

        // Create a test table
        db.execute("CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT(32), age INTEGER)")?;

        // Insert some test data
        db.execute("INSERT INTO users (id, name, age) VALUES (1, 'Alice', 30)")?;
        db.execute("INSERT INTO users (id, name, age) VALUES (2, 'Bob', 25)")?;
        db.execute("INSERT INTO users (id, name, age) VALUES (3, 'Charlie', 35)")?;

        // Test that queries work (these go through the planner)
        let result = db.query("SELECT * FROM users WHERE id = 1").unwrap();
        assert_eq!(result.columns().len(), 3);
        assert_eq!(result.rows().len(), 1);
        assert_eq!(result.rows()[0].len(), 3);

        // Test table scan query
        let result = db.query("SELECT name FROM users WHERE age > 30").unwrap();
        println!(
            "Query result: columns={:?}, rows={:?}",
            result.columns(),
            result.rows()
        );
        assert_eq!(result.columns(), &vec!["name"]);
        // Should find Alice (30) and Charlie (35), but age > 30 means only Charlie (35)
        assert_eq!(result.rows().len(), 1); // Only Charlie with age 35

        // Test limited query
        let result = db.query("SELECT * FROM users LIMIT 2").unwrap();
        assert_eq!(result.rows().len(), 2);

        println!("✓ Planner integration test passed - queries executed successfully through planner pipeline");
        Ok(())
    })
}

#[test]
fn test_crud_operations_with_planner() -> Result<()> {
    run_with_both_backends("test_crud_operations_with_planner", |db_path| {
        let mut db = Database::open(db_path)?;

        // CREATE
        db.execute("CREATE TABLE products (id INTEGER PRIMARY KEY, name TEXT(32), price REAL)")?;

        // INSERT
        let affected =
            db.execute("INSERT INTO products (id, name, price) VALUES (1, 'Widget', 9.99)")?;
        assert_eq!(affected, 1);

        let affected =
            db.execute("INSERT INTO products (id, name, price) VALUES (2, 'Gadget', 19.99)")?;
        assert_eq!(affected, 1);

        // SELECT
        let result = db.query("SELECT * FROM products WHERE id = 1").unwrap();
        assert_eq!(result.rows().len(), 1);

        // UPDATE
        let affected = db.execute("UPDATE products SET price = 12.99 WHERE id = 1")?;
        assert_eq!(affected, 1);

        // Verify update
        let result = db.query("SELECT price FROM products WHERE id = 1").unwrap();
        assert_eq!(result.rows()[0][0], tegdb::SqlValue::Real(12.99));

        // DELETE
        let affected = db.execute("DELETE FROM products WHERE id = 2")?;
        assert_eq!(affected, 1);

        // Verify delete
        let result = db.query("SELECT * FROM products").unwrap();
        assert_eq!(result.rows().len(), 1);

        println!("✓ CRUD operations through planner pipeline work correctly");
        Ok(())
    })
}
