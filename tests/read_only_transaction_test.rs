mod test_helpers;

#[cfg(test)]
mod read_only_transaction_tests {
    use crate::test_helpers::run_with_both_backends;
    use tegdb::{Database, Result, SqlValue};

    #[test]
    fn test_read_only_transaction_optimization() -> Result<()> {
        run_with_both_backends("test_read_only_transaction_optimization", |db_path| {
            let mut db = Database::open(db_path)?;

            // Create a test table and populate with some data
            db.execute("CREATE TABLE test_data (key TEXT PRIMARY KEY, value TEXT)")?;
            db.execute("INSERT INTO test_data (key, value) VALUES ('key1', 'value1')")?;
            db.execute("INSERT INTO test_data (key, value) VALUES ('key2', 'value2')")?;

            // Perform multiple read-only transactions
            {
                // Read-only transaction 1 - simple SELECTs
                {
                    let mut tx = db.begin_transaction()?;
                    let _result1 = tx.query("SELECT value FROM test_data WHERE key = 'key1'")?;
                    let _result2 = tx.query("SELECT value FROM test_data WHERE key = 'key2'")?;
                    tx.commit()?;
                }

                // Read-only transaction 2 - range scan
                {
                    let mut tx = db.begin_transaction()?;
                    let _scan_results =
                        tx.query("SELECT * FROM test_data WHERE key >= 'key1' AND key < 'key3'")?;
                    tx.commit()?;
                }

                // Read-only transaction 3 - just commit without doing anything
                {
                    let tx = db.begin_transaction()?;
                    tx.commit()?;
                }
            }

            // Verify data is still accessible after read-only transactions
            let result1 = db.query("SELECT value FROM test_data WHERE key = 'key1'")?;
            assert_eq!(result1.rows().len(), 1);
            assert_eq!(result1.rows()[0][0], SqlValue::Text("value1".to_string()));

            let result2 = db.query("SELECT value FROM test_data WHERE key = 'key2'")?;
            assert_eq!(result2.rows().len(), 1);
            assert_eq!(result2.rows()[0][0], SqlValue::Text("value2".to_string()));

            // Now do a write transaction and verify it works
            {
                let mut tx = db.begin_transaction()?;
                tx.execute("INSERT INTO test_data (key, value) VALUES ('key3', 'value3')")?;
                tx.commit()?;
            }

            // Verify the write transaction worked
            let result3 = db.query("SELECT value FROM test_data WHERE key = 'key3'")?;
            assert_eq!(result3.rows().len(), 1);
            assert_eq!(result3.rows()[0][0], SqlValue::Text("value3".to_string()));

            Ok(())
        })
    }

    #[test]
    fn test_mixed_read_write_transaction() -> Result<()> {
        run_with_both_backends("test_mixed_read_write_transaction", |db_path| {
            let mut db = Database::open(db_path)?;

            // Create a test table and populate with some data
            db.execute("CREATE TABLE test_data (key TEXT PRIMARY KEY, value TEXT)")?;
            db.execute("INSERT INTO test_data (key, value) VALUES ('key1', 'value1')")?;

            // Perform a transaction that starts with reads but then does a write
            {
                let mut tx = db.begin_transaction()?;

                // Start with reads
                let _result1 = tx.query("SELECT value FROM test_data WHERE key = 'key1'")?;
                let _scan_results =
                    tx.query("SELECT * FROM test_data WHERE key >= 'key1' AND key < 'key3'")?;

                // Then do a write - this should make it a write transaction
                tx.execute("INSERT INTO test_data (key, value) VALUES ('key2', 'value2')")?;
                tx.commit()?;
            }

            // Verify the write transaction worked
            let result2 = db.query("SELECT value FROM test_data WHERE key = 'key2'")?;
            assert_eq!(result2.rows().len(), 1);
            assert_eq!(result2.rows()[0][0], SqlValue::Text("value2".to_string()));

            // Verify original data is still there
            let result1 = db.query("SELECT value FROM test_data WHERE key = 'key1'")?;
            assert_eq!(result1.rows().len(), 1);
            assert_eq!(result1.rows()[0][0], SqlValue::Text("value1".to_string()));

            Ok(())
        })
    }
}
