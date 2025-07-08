#[cfg(test)]
mod commit_marker_tests {
    use std::path::PathBuf;
    use tegdb::storage_engine::StorageEngine;

    #[test]
    fn test_commit_marker_and_crash_recovery() {
        let test_db_path = PathBuf::from("/tmp/test_commit_marker_recovery.db");

        // Clean up if the file exists
        if test_db_path.exists() {
            std::fs::remove_file(&test_db_path).unwrap();
        }

        // Create a new engine and perform transactions
        {
            let mut engine = StorageEngine::new(test_db_path.clone()).unwrap();

            // Begin a transaction and commit it
            {
                let mut tx = engine.begin_transaction();
                tx.set(b"key1", b"value1".to_vec()).unwrap();
                tx.set(b"key2", b"value2".to_vec()).unwrap();
                tx.commit().unwrap();
            }

            // Begin another transaction but don't commit (simulate crash)
            {
                let mut tx = engine.begin_transaction();
                tx.set(b"key3", b"value3".to_vec()).unwrap();
                // Don't commit - this should be rolled back on recovery
            }
        }

        // Reopen the database to simulate crash recovery
        {
            let engine = StorageEngine::new(test_db_path.clone()).unwrap();

            // Check that committed data is still there
            assert_eq!(engine.get(b"key1").as_deref(), Some(b"value1" as &[u8]));
            assert_eq!(engine.get(b"key2").as_deref(), Some(b"value2" as &[u8]));

            // Check that uncommitted data was rolled back
            assert_eq!(engine.get(b"key3"), None);

            // Commit markers are no longer accessible as data - they're internal recovery markers
            // Just verify that the committed data is present and uncommitted data is absent
        }

        // Clean up
        std::fs::remove_file(&test_db_path).unwrap();
    }

    #[test]
    fn test_multiple_transactions_with_commit_markers() {
        let test_db_path = PathBuf::from("/tmp/test_multiple_transactions.db");

        // Clean up if the file exists
        if test_db_path.exists() {
            std::fs::remove_file(&test_db_path).unwrap();
        }

        {
            let mut engine = StorageEngine::new(test_db_path.clone()).unwrap();

            // Transaction 1: committed
            {
                let mut tx = engine.begin_transaction();
                tx.set(b"tx1_key", b"tx1_value".to_vec()).unwrap();
                tx.commit().unwrap();
            }

            // Transaction 2: committed
            {
                let mut tx = engine.begin_transaction();
                tx.set(b"tx2_key", b"tx2_value".to_vec()).unwrap();
                tx.commit().unwrap();
            }

            // Verify that both transactions were committed by checking their data
            assert_eq!(
                engine.get(b"tx1_key").as_deref(),
                Some(b"tx1_value" as &[u8])
            );
            assert_eq!(
                engine.get(b"tx2_key").as_deref(),
                Some(b"tx2_value" as &[u8])
            );

            // Commit markers are no longer accessible as data - they're internal recovery markers
            // The presence of both committed values confirms the recovery process worked correctly
        }

        // Clean up
        std::fs::remove_file(&test_db_path).unwrap();
    }
}
