#[cfg(test)]
mod commit_marker_tests {
    use tegdb::engine::Engine;
    use std::path::PathBuf;
    
    #[test]
    fn test_commit_marker_and_crash_recovery() {
        let test_db_path = PathBuf::from("/tmp/test_commit_marker_recovery.db");
        
        // Clean up if the file exists
        if test_db_path.exists() {
            std::fs::remove_file(&test_db_path).unwrap();
        }
        
        // Create a new engine and perform transactions
        {
            let mut engine = Engine::new(test_db_path.clone()).unwrap();
            
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
            let engine = Engine::new(test_db_path.clone()).unwrap();
            
            // Check that committed data is still there
            assert_eq!(engine.get(b"key1").as_deref(), Some(b"value1" as &[u8]));
            assert_eq!(engine.get(b"key2").as_deref(), Some(b"value2" as &[u8]));
            
            // Check that uncommitted data was rolled back
            assert_eq!(engine.get(b"key3"), None);
            
            // Verify the commit marker is present
            assert!(engine.get(b"__tx_commit__").is_some());
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
            let mut engine = Engine::new(test_db_path.clone()).unwrap();
            
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
            
            // Verify commit marker contains the latest transaction ID
            let commit_marker = engine.get(b"__tx_commit__").unwrap();
            let tx_id = u64::from_be_bytes(commit_marker.as_ref().try_into().unwrap());
            assert_eq!(tx_id, 2); // Second transaction
        }
        
        // Clean up
        std::fs::remove_file(&test_db_path).unwrap();
    }
}
