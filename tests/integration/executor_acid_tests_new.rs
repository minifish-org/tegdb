#[path = "../helpers/test_helpers.rs"] mod test_helpers;
use test_helpers::run_with_both_backends;

use tegdb::{Database, Result, SqlValue};


/// Test atomicity - all operations in a transaction succeed or all fail
#[test]
fn test_transaction_atomicity() -> Result<()> {
    run_with_both_backends("test_transaction_atomicity", |db_path| {
        let mut db = Database::open(db_path)?;

        // Setup initial data
        db.execute("CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT NOT NULL, age INTEGER)")?;
        db.execute("INSERT INTO users (id, name, age) VALUES (1, 'John', 25)")?;

        // Verify initial state
        let result = db.query("SELECT * FROM users")?;
        assert_eq!(result.rows().len(), 1);

        // Test successful transaction (all operations should succeed)
        {
            let mut tx = db.begin_transaction()?;
            tx.execute("INSERT INTO users (id, name, age) VALUES (2, 'Jane', 30)")?;
            tx.execute("UPDATE users SET age = 26 WHERE id = 1")?;
            tx.commit()?;
        }

        // Verify both operations succeeded
        let result = db.query("SELECT * FROM users ORDER BY id")?;
        assert_eq!(result.rows().len(), 2);

        // Test failed transaction (rollback should undo all operations)
        {
            let mut tx = db.begin_transaction()?;
            tx.execute("INSERT INTO users (id, name, age) VALUES (3, 'Bob', 35)")?;

            // This should fail due to duplicate ID
            let invalid_result =
                tx.execute("INSERT INTO users (id, name, age) VALUES (1, 'Duplicate', 40)");
            assert!(invalid_result.is_err());

            // Transaction should be rolled back automatically on drop without commit
        }

        // Verify that Bob was not inserted (transaction was rolled back)
        let result = db.query("SELECT * FROM users ORDER BY id")?;
        assert_eq!(result.rows().len(), 2); // Still only John and Jane

        println!("✓ Transaction atomicity test passed");
        Ok(())
    })
}

/// Test consistency - database remains in a valid state
#[test]
fn test_transaction_consistency() -> Result<()> {
    run_with_both_backends("test_transaction_consistency", |db_path| {
        let mut db = Database::open(db_path)?;

        // Setup test schema
        db.execute("CREATE TABLE accounts (id INTEGER PRIMARY KEY, balance INTEGER NOT NULL)")?;
        db.execute("INSERT INTO accounts (id, balance) VALUES (1, 1000)")?;
        db.execute("INSERT INTO accounts (id, balance) VALUES (2, 500)")?;

        // Test money transfer that maintains balance invariant
        {
            let mut tx = db.begin_transaction()?;

            // Transfer $200 from account 1 to account 2
            tx.execute("UPDATE accounts SET balance = balance - 200 WHERE id = 1")?;
            tx.execute("UPDATE accounts SET balance = balance + 200 WHERE id = 2")?;

            tx.commit()?;
        }

        // Verify balances are consistent
        let result = db.query("SELECT balance FROM accounts ORDER BY id")?;
        assert_eq!(result.rows().len(), 2);

        let balance1 = match &result.rows()[0][0] {
            SqlValue::Integer(b) => b,
            _ => panic!("Expected integer balance"),
        };
        let balance2 = match &result.rows()[1][0] {
            SqlValue::Integer(b) => b,
            _ => panic!("Expected integer balance"),
        };

        assert_eq!(*balance1, 800); // 1000 - 200
        assert_eq!(*balance2, 700); // 500 + 200
        assert_eq!(balance1 + balance2, 1500); // Total should be preserved

        println!("✓ Transaction consistency test passed");
        Ok(())
    })
}

/// Test isolation - transactions don't interfere with each other
#[test]
fn test_transaction_isolation() -> Result<()> {
    run_with_both_backends("test_transaction_isolation", |db_path| {
        let mut db = Database::open(db_path)?;

        // Setup test data
        db.execute("CREATE TABLE items (id INTEGER PRIMARY KEY, name TEXT, quantity INTEGER)")?;
        db.execute("INSERT INTO items (id, name, quantity) VALUES (1, 'Widget', 10)")?;

        // Test that uncommitted changes aren't visible to other transactions
        // Note: Since we have a single Database instance, we'll simulate this
        // by showing that rollback works properly

        // Start transaction and make changes
        {
            let mut tx = db.begin_transaction()?;
            tx.execute("UPDATE items SET quantity = 5 WHERE id = 1")?;

            // Don't commit - let transaction drop and rollback
        }

        // Verify changes were rolled back
        let result = db.query("SELECT quantity FROM items WHERE id = 1")?;
        let quantity = match &result.rows()[0][0] {
            SqlValue::Integer(q) => q,
            _ => panic!("Expected integer quantity"),
        };
        assert_eq!(*quantity, 10); // Should still be original value

        println!("✓ Transaction isolation test passed");
        Ok(())
    })
}

/// Test durability - committed changes survive system restart
#[test]
fn test_transaction_durability() -> Result<()> {
    run_with_both_backends("test_transaction_durability", |db_path| {
        // Phase 1: Create data and commit
        {
            let mut db = Database::open(db_path)?;
            let mut tx = db.begin_transaction()?;
            tx.execute("CREATE TABLE persistent_data (id INTEGER PRIMARY KEY, value TEXT)")?;

            tx.execute("INSERT INTO persistent_data (id, value) VALUES (1, 'test')")?;
            tx.execute("INSERT INTO persistent_data (id, value) VALUES (2, 'data')")?;
            tx.commit()?;
        } // Database closed

        // Add a small delay to ensure file handles are released
        std::thread::sleep(std::time::Duration::from_millis(10));

        // Phase 2: Reopen and verify data survived
        {
            let mut db = Database::open(db_path)?;
            let result = db.query("SELECT * FROM persistent_data ORDER BY id")?;
            assert_eq!(result.rows().len(), 2);

            let value1 = match &result.rows()[0][1] {
                SqlValue::Text(v) => v.clone(),
                _ => panic!("Expected text value"),
            };
            let value2 = match &result.rows()[1][1] {
                SqlValue::Text(v) => v.clone(),
                _ => panic!("Expected text value"),
            };

            assert_eq!(value1, "test");
            assert_eq!(value2, "data");
        }

        println!("✓ Transaction durability test passed");
        Ok(())
    })
}

/// Test rollback scenarios
#[test]
fn test_transaction_rollback_scenarios() -> Result<()> {
    run_with_both_backends("test_transaction_rollback_scenarios", |db_path| {
        let mut db = Database::open(db_path)?;

        // Setup test data
        db.execute("CREATE TABLE test_rollback (id INTEGER PRIMARY KEY, name TEXT)")?;
        db.execute("INSERT INTO test_rollback (id, name) VALUES (1, 'original')")?;

        // Test explicit rollback
        {
            let mut tx = db.begin_transaction()?;
            tx.execute("INSERT INTO test_rollback (id, name) VALUES (2, 'should_rollback')")?;
            tx.execute("UPDATE test_rollback SET name = 'modified' WHERE id = 1")?;

            // Explicit rollback
            tx.rollback()?;
        }

        // Verify rollback worked
        let result = db.query("SELECT * FROM test_rollback ORDER BY id")?;
        assert_eq!(result.rows().len(), 1);

        let name = match &result.rows()[0][1] {
            SqlValue::Text(n) => n.clone(),
            _ => panic!("Expected text name"),
        };
        assert_eq!(name, "original");

        // Test implicit rollback (transaction dropped without commit)
        {
            let mut tx = db.begin_transaction()?;
            tx.execute("INSERT INTO test_rollback (id, name) VALUES (3, 'should_also_rollback')")?;
            // Don't commit - transaction will be rolled back on drop
        }

        // Verify implicit rollback worked
        let result = db.query("SELECT * FROM test_rollback ORDER BY id")?;
        assert_eq!(result.rows().len(), 1); // Still only original row

        println!("✓ Transaction rollback test passed");
        Ok(())
    })
}

/// Test concurrent transaction patterns
#[test]
fn test_concurrent_transaction_patterns() -> Result<()> {
    run_with_both_backends("test_concurrent_transaction_patterns", |db_path| {
        let mut db = Database::open(db_path)?;

        // Setup shared counter table
        db.execute("CREATE TABLE shared_counter (id INTEGER PRIMARY KEY, value INTEGER)")?;
        db.execute("INSERT INTO shared_counter (id, value) VALUES (1, 0)")?;

        // Test multiple sequential transactions that modify the same data
        for i in 1..=5 {
            let mut tx = db.begin_transaction()?;
            tx.execute(&format!(
                "UPDATE shared_counter SET value = {i} WHERE id = 1"
            ))?;
            tx.commit()?;
        }

        // Verify final value
        let result = db.query("SELECT value FROM shared_counter WHERE id = 1")?;
        let final_value = match &result.rows()[0][0] {
            SqlValue::Integer(v) => v,
            _ => panic!("Expected integer value"),
        };
        assert_eq!(*final_value, 5);

        // Test transaction with multiple operations
        {
            let mut tx = db.begin_transaction()?;
            tx.execute("INSERT INTO shared_counter (id, value) VALUES (2, 100)")?;
            tx.execute("INSERT INTO shared_counter (id, value) VALUES (3, 200)")?;
            tx.execute("UPDATE shared_counter SET value = value + 10 WHERE id >= 2")?;
            tx.commit()?;
        }

        // Verify all operations committed together
        let result = db.query("SELECT value FROM shared_counter WHERE id >= 2 ORDER BY id")?;
        assert_eq!(result.rows().len(), 2);

        let value2 = match &result.rows()[0][0] {
            SqlValue::Integer(v) => v,
            _ => panic!("Expected integer value"),
        };
        let value3 = match &result.rows()[1][0] {
            SqlValue::Integer(v) => v,
            _ => panic!("Expected integer value"),
        };

        assert_eq!(*value2, 110); // 100 + 10
        assert_eq!(*value3, 210); // 200 + 10

        println!("✓ Concurrent transaction patterns test passed");
        Ok(())
    })
}
