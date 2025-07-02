#[cfg(test)]
mod read_only_transaction_tests {
    use std::fs;
    use std::path::PathBuf;
    use tegdb::engine::Engine;

    #[test]
    fn test_read_only_transaction_optimization() {
        let test_db_path = PathBuf::from("/tmp/test_read_only_transaction.db");

        // Clean up if the file exists
        if test_db_path.exists() {
            std::fs::remove_file(&test_db_path).unwrap();
        }

        // Create a new engine and populate with some data
        {
            let mut engine = Engine::new(test_db_path.clone()).unwrap();
            engine.set(b"key1", b"value1".to_vec()).unwrap();
            engine.set(b"key2", b"value2".to_vec()).unwrap();
            drop(engine);
        }

        // Get the initial file size
        let initial_size = fs::metadata(&test_db_path).unwrap().len();

        // Perform multiple read-only transactions
        {
            let mut engine = Engine::new(test_db_path.clone()).unwrap();

            // Read-only transaction 1
            {
                let mut tx = engine.begin_transaction();
                let _val1 = tx.get(b"key1");
                let _val2 = tx.get(b"key2");
                tx.commit().unwrap();
            }

            // Read-only transaction 2
            {
                let mut tx = engine.begin_transaction();
                let _scan_results: Vec<_> = tx
                    .scan(b"key1".to_vec()..b"key3".to_vec())
                    .unwrap()
                    .collect();
                tx.commit().unwrap();
            }

            // Read-only transaction 3
            {
                let mut tx = engine.begin_transaction();
                // Just commit without doing anything
                tx.commit().unwrap();
            }

            drop(engine);
        }

        // Check that file size hasn't grown (no commit markers written)
        let final_size = fs::metadata(&test_db_path).unwrap().len();
        assert_eq!(
            initial_size, final_size,
            "Read-only transactions should not write commit markers"
        );

        // Verify data is still accessible after read-only transactions
        {
            let engine = Engine::new(test_db_path.clone()).unwrap();
            assert_eq!(engine.get(b"key1").as_deref(), Some(b"value1" as &[u8]));
            assert_eq!(engine.get(b"key2").as_deref(), Some(b"value2" as &[u8]));
        }

        // Now do a write transaction and verify file size increases
        {
            let mut engine = Engine::new(test_db_path.clone()).unwrap();
            {
                let mut tx = engine.begin_transaction();
                tx.set(b"key3", b"value3".to_vec()).unwrap();
                tx.commit().unwrap();
            }
            drop(engine);
        }

        // Check that file size has grown (commit marker was written)
        let after_write_size = fs::metadata(&test_db_path).unwrap().len();
        assert!(
            after_write_size > final_size,
            "Write transactions should write commit markers"
        );

        // Clean up
        std::fs::remove_file(&test_db_path).unwrap();
    }

    #[test]
    fn test_mixed_read_write_transaction() {
        let test_db_path = PathBuf::from("/tmp/test_mixed_transaction.db");

        // Clean up if the file exists
        if test_db_path.exists() {
            std::fs::remove_file(&test_db_path).unwrap();
        }

        // Create a new engine and populate with some data
        {
            let mut engine = Engine::new(test_db_path.clone()).unwrap();
            engine.set(b"key1", b"value1".to_vec()).unwrap();
            drop(engine);
        }

        // Get the initial file size
        let initial_size = fs::metadata(&test_db_path).unwrap().len();

        // Perform a transaction that starts with reads but then does a write
        {
            let mut engine = Engine::new(test_db_path.clone()).unwrap();
            {
                let mut tx = engine.begin_transaction();
                // Start with reads
                let _val1 = tx.get(b"key1");
                let _scan_results: Vec<_> = tx
                    .scan(b"key1".to_vec()..b"key3".to_vec())
                    .unwrap()
                    .collect();

                // Then do a write - this should make it a write transaction
                tx.set(b"key2", b"value2".to_vec()).unwrap();
                tx.commit().unwrap();
            }
            drop(engine);
        }

        // Check that file size has grown (commit marker was written because of the write)
        let final_size = fs::metadata(&test_db_path).unwrap().len();
        assert!(
            final_size > initial_size,
            "Transactions with writes should always write commit markers"
        );

        // Clean up
        std::fs::remove_file(&test_db_path).unwrap();
    }
}
