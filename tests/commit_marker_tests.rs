mod test_helpers;

#[cfg(test)]
mod commit_marker_tests {
    use tegdb::{Database, Result, SqlValue};
    use crate::test_helpers::run_with_both_backends;

    #[test]
    fn test_commit_marker_and_crash_recovery() -> Result<()> {
        run_with_both_backends("test_commit_marker_and_crash_recovery", |db_path| {
            let mut db = Database::open(db_path)?;

            // Create a test table
            db.execute("CREATE TABLE test_data (key TEXT PRIMARY KEY, value TEXT)")?;

            // Begin a transaction and commit it
            {
                let mut tx = db.begin_transaction()?;
                tx.execute("INSERT INTO test_data (key, value) VALUES ('key1', 'value1')")?;
                tx.execute("INSERT INTO test_data (key, value) VALUES ('key2', 'value2')")?;
                tx.commit()?;
            }

            // Begin another transaction but don't commit (simulate crash)
            {
                let mut tx = db.begin_transaction()?;
                tx.execute("INSERT INTO test_data (key, value) VALUES ('key3', 'value3')")?;
                // Don't commit - this should be rolled back on recovery
            }

            // Drop the first database instance to release the file lock
            drop(db);
            
            // Reopen the database to simulate crash recovery
            let mut db2 = Database::open(db_path)?;

            // Check that committed data is still there
            let result1 = db2.query("SELECT value FROM test_data WHERE key = 'key1'")?;
            assert_eq!(result1.rows().len(), 1);
            assert_eq!(result1.rows()[0][0], SqlValue::Text("value1".to_string()));

            let result2 = db2.query("SELECT value FROM test_data WHERE key = 'key2'")?;
            assert_eq!(result2.rows().len(), 1);
            assert_eq!(result2.rows()[0][0], SqlValue::Text("value2".to_string()));

            // Check that uncommitted data was rolled back
            let result3 = db2.query("SELECT value FROM test_data WHERE key = 'key3'")?;
            assert_eq!(result3.rows().len(), 0); // Should be empty

            Ok(())
        })
    }

    #[test]
    fn test_multiple_transactions_with_commit_markers() -> Result<()> {
        run_with_both_backends("test_multiple_transactions_with_commit_markers", |db_path| {
            let mut db = Database::open(db_path)?;

            // Create a test table
            db.execute("CREATE TABLE test_data (key TEXT PRIMARY KEY, value TEXT)")?;

            // Transaction 1: committed
            {
                let mut tx = db.begin_transaction()?;
                tx.execute("INSERT INTO test_data (key, value) VALUES ('tx1_key', 'tx1_value')")?;
                tx.commit()?;
            }

            // Transaction 2: committed
            {
                let mut tx = db.begin_transaction()?;
                tx.execute("INSERT INTO test_data (key, value) VALUES ('tx2_key', 'tx2_value')")?;
                tx.commit()?;
            }

            // Verify that both transactions were committed by checking their data
            let result1 = db.query("SELECT value FROM test_data WHERE key = 'tx1_key'")?;
            assert_eq!(result1.rows().len(), 1);
            assert_eq!(result1.rows()[0][0], SqlValue::Text("tx1_value".to_string()));

            let result2 = db.query("SELECT value FROM test_data WHERE key = 'tx2_key'")?;
            assert_eq!(result2.rows().len(), 1);
            assert_eq!(result2.rows()[0][0], SqlValue::Text("tx2_value".to_string()));

            Ok(())
        })
    }
}
