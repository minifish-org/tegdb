use std::fs;
use std::io::Error;
use std::path::PathBuf;
use std::sync::Arc;
use tokio;
use std::time::Duration;
use std::sync::Mutex;
use std::collections::HashMap;

use tegdb::Database;

// Concurrent Testing Framework that works around Send limitations
pub struct ConcurrentTxnTest {
    pub db: Arc<Database>,
    pub path: PathBuf,
}

impl ConcurrentTxnTest {
    /// Create a new concurrent test environment
    pub async fn new(test_name: &str) -> Self {
        let path = PathBuf::from(format!("{}.db", test_name));
        
        // Clean up any previous test data
        if path.exists() {
            fs::remove_dir_all(&path).unwrap();
        }
        
        let db = Arc::new(Database::new(path.clone()).await);
        ConcurrentTxnTest { db, path }
    }
    
    /// A simple function to test concurrent write operations
    pub async fn test_concurrent_writers(&self, num_writers: usize) -> Vec<Result<(), Error>> {
        // First, set up the initial state
        {
            let mut txn = self.db.new_transaction().await;
            for i in 0..5 {
                let key = format!("key_{}", i).into_bytes();
                txn.insert(&key, format!("initial_value_{}", i).into_bytes()).await.unwrap();
            }
            txn.commit().await.unwrap();
        }
        
        // Create a HashMap to track which writers succeeded (HashMap doesn't require Clone)
        let results = Arc::new(Mutex::new(HashMap::new()));
        
        // Launch concurrent writers
        let mut handles = Vec::with_capacity(num_writers);
        
        for i in 0..num_writers {
            let db = self.db.clone();
            let results = Arc::clone(&results);
            
            // Spawn a writer task
            handles.push(tokio::spawn(async move {
                // Ensure staggered start for more concurrency
                tokio::time::sleep(Duration::from_millis(i as u64 * 5)).await;
                
                // Perform a write operation in this task
                let operation_result = Self::do_write_operation(db, i).await;
                
                // Store the result
                let mut results = results.lock().unwrap();
                results.insert(i, operation_result);
            }));
        }
        
        // Wait for all writers to complete
        for handle in handles {
            let _ = handle.await;
        }
        
        // Convert results to the expected format
        let mutex_guard = Arc::try_unwrap(results).unwrap().into_inner().unwrap();
        
        // Convert HashMap to Vec in order by index
        let mut result_vec = Vec::with_capacity(num_writers);
        for i in 0..num_writers {
            result_vec.push(mutex_guard.get(&i)
                .cloned()
                .unwrap_or(Err(Error::new(std::io::ErrorKind::Other, "Task didn't complete"))));
        }
        
        result_vec
    }
    
    /// Internal helper function to perform a write operation
    async fn do_write_operation(db: Arc<Database>, writer_id: usize) -> Result<(), Error> {
        // Create a new transaction
        let mut txn = db.new_transaction().await;
        
        // Try to update multiple keys
        let mut success = true;
        for i in 0..5 {
            let key = format!("key_{}", i).into_bytes();
            let value = format!("updated_by_writer_{}_at_{}", writer_id, i).into_bytes();
            
            if let Err(e) = txn.update(&key, value).await {
                println!("Writer {} failed to update key_{}: {}", writer_id, i, e);
                success = false;
                break;
            }
            
            println!("Writer {} updated key_{}", writer_id, i);
        }
        
        // Either commit or roll back the transaction
        if success {
            match txn.commit().await {
                Ok(_) => {
                    println!("Writer {} committed successfully", writer_id);
                    Ok(())
                },
                Err(e) => {
                    println!("Writer {} failed to commit: {}", writer_id, e);
                    Err(e)
                }
            }
        } else {
            txn.rollback().await?;
            Err(Error::new(std::io::ErrorKind::Other, "Operation failed, transaction rolled back"))
        }
    }
    
    /// Test for potential deadlocks with multiple writers trying to update the same key
    pub async fn test_deadlock_scenario(&self) -> Vec<Result<(), Error>> {
        // Set up a shared conflict key
        let conflict_key = b"shared_conflict_key".to_vec();
        
        {
            let mut txn = self.db.new_transaction().await;
            txn.insert(&conflict_key, b"initial_value".to_vec()).await.unwrap();
            txn.commit().await.unwrap();
        }
        
        // Number of concurrent transactions to attempt
        let num_transactions = 5;
        
        // Track results for each transaction
        let results = Arc::new(Mutex::new(HashMap::new()));
        
        // Spawn concurrent transaction tasks
        let mut handles = Vec::new();
        
        for i in 0..num_transactions {
            let db = self.db.clone();
            let key = conflict_key.clone();
            let results = Arc::clone(&results);
            
            handles.push(tokio::spawn(async move {
                // Stagger start times slightly
                tokio::time::sleep(Duration::from_millis(i as u64 * 3)).await;
                
                // Try to update the shared key
                let operation_result = Self::update_conflict_key(db, &key, i).await;
                
                // Store the result
                let mut results = results.lock().unwrap();
                results.insert(i, operation_result);
            }));
        }
        
        // Wait for all tasks
        for handle in handles {
            let _ = handle.await;
        }
        
        // Check the final state of the key
        let mut txn = self.db.new_transaction().await;
        let (value, _) = txn.select(&conflict_key).await.unwrap();
        if let Some(bytes) = &value {
            println!("Final value of conflict key: {}", String::from_utf8_lossy(bytes));
        }
        txn.rollback().await.unwrap();
        
        // Convert results to the expected format
        let mutex_guard = Arc::try_unwrap(results).unwrap().into_inner().unwrap();
        
        // Convert HashMap to Vec in order by index
        let mut result_vec = Vec::with_capacity(num_transactions);
        for i in 0..num_transactions {
            result_vec.push(mutex_guard.get(&i)
                .cloned()
                .unwrap_or(Err(Error::new(std::io::ErrorKind::Other, "Task didn't complete"))));
        }
        
        result_vec
    }
    
    /// Helper function to update a conflict key
    async fn update_conflict_key(db: Arc<Database>, key: &[u8], id: usize) -> Result<(), Error> {
        println!("Transaction {} attempting to acquire lock on shared key", id);
        
        let mut txn = db.new_transaction().await;
        let new_value = format!("updated_by_txn_{}", id).into_bytes();
        
        match txn.update(key, new_value).await {
            Ok(_) => {
                println!("Transaction {} updated the key successfully", id);
                match txn.commit().await {
                    Ok(_) => {
                        println!("Transaction {} committed successfully", id);
                        Ok(())
                    },
                    Err(e) => {
                        println!("Transaction {} failed to commit: {}", id, e);
                        // Note: can't rollback after a failed commit - the transaction is already consumed
                        Err(e)
                    }
                }
            },
            Err(e) => {
                println!("Transaction {} failed to update: {}", id, e);
                txn.rollback().await?;
                Err(e)
            }
        }
    }
    
    /// Test isolation with readers and writers
    pub async fn test_isolation_scenario(&self, readers: usize, writers: usize) -> (Vec<Result<(), Error>>, Vec<Result<(), Error>>) {
        // Set up initial data
        {
            let mut txn = self.db.new_transaction().await;
            for i in 0..5 {
                let key = format!("key_{}", i).into_bytes();
                txn.insert(&key, format!("initial_value_{}", i).into_bytes()).await.unwrap();
            }
            txn.commit().await.unwrap();
        }
        
        // Create result trackers using HashMaps instead of Vecs
        let reader_results = Arc::new(Mutex::new(HashMap::new()));
        let writer_results = Arc::new(Mutex::new(HashMap::new()));
        
        // Spawn reader tasks
        let mut reader_handles = Vec::new();
        for i in 0..readers {
            let db = self.db.clone();
            let results = Arc::clone(&reader_results);
            
            reader_handles.push(tokio::spawn(async move {
                // Stagger slightly
                tokio::time::sleep(Duration::from_millis(i as u64 * 2)).await;
                
                let operation_result = Self::do_read_operation(db, i).await;
                
                // Store the result
                let mut results = results.lock().unwrap();
                results.insert(i, operation_result);
            }));
        }
        
        // Spawn writer tasks
        let mut writer_handles = Vec::new();
        for i in 0..writers {
            let db = self.db.clone();
            let results = Arc::clone(&writer_results);
            
            writer_handles.push(tokio::spawn(async move {
                // Stagger slightly
                tokio::time::sleep(Duration::from_millis(i as u64 * 3)).await;
                
                let operation_result = Self::do_write_operation(db, i).await;
                
                // Store the result
                let mut results = results.lock().unwrap();
                results.insert(i, operation_result);
            }));
        }
        
        // Wait for readers
        for handle in reader_handles {
            let _ = handle.await;
        }
        
        // Wait for writers
        for handle in writer_handles {
            let _ = handle.await;
        }
        
        // Process the results
        let reader_guard = Arc::try_unwrap(reader_results).unwrap().into_inner().unwrap();
        let writer_guard = Arc::try_unwrap(writer_results).unwrap().into_inner().unwrap();
        
        // Convert results to vectors in numerical order
        let mut reader_vec = Vec::with_capacity(readers);
        for i in 0..readers {
            reader_vec.push(reader_guard.get(&i)
                .cloned()
                .unwrap_or(Err(Error::new(std::io::ErrorKind::Other, "Reader task didn't complete"))));
        }
        
        let mut writer_vec = Vec::with_capacity(writers);
        for i in 0..writers {
            writer_vec.push(writer_guard.get(&i)
                .cloned()
                .unwrap_or(Err(Error::new(std::io::ErrorKind::Other, "Writer task didn't complete"))));
        }
        
        (reader_vec, writer_vec)
    }
    
    /// Helper function for read operations
    async fn do_read_operation(db: Arc<Database>, reader_id: usize) -> Result<(), Error> {
        let mut txn = db.new_transaction().await;
        
        // Read some keys and log the values
        for i in 0..5 {
            let key = format!("key_{}", i).into_bytes();
            let (value, _) = txn.select(&key).await?;
            
            if let Some(bytes) = value {
                println!("Reader {} read key_{}: {}", reader_id, i, String::from_utf8_lossy(&bytes));
            } else {
                println!("Reader {} found key_{} is empty", reader_id, i);
            }
        }
        
        // Create a marker to show this reader was active
        let marker_key = format!("reader_{}_marker", reader_id).into_bytes();
        txn.insert(&marker_key, b"read_completed".to_vec()).await?;
        
        // Commit the transaction
        match txn.commit().await {
            Ok(_) => {
                println!("Reader {} completed successfully", reader_id);
                Ok(())
            },
            Err(e) => {
                println!("Reader {} failed to commit: {}", reader_id, e);
                Err(e)
            }
        }
    }
    
    /// Clean up test resources
    pub async fn cleanup(&self) {
        self.db.shutdown().await;
        if self.path.exists() {
            fs::remove_dir_all(&self.path).unwrap();
        }
    }
}

// Concurrent Deadlock Detection Test
#[tokio::test]
async fn test_deadlock_detection_concurrent() -> Result<(), Error> {
    // Create test framework
    let test = ConcurrentTxnTest::new("test_deadlock_concurrent").await;
    
    // Run the deadlock test scenario
    let results = test.test_deadlock_scenario().await;
    
    // Verify that all tasks completed (no indefinite blocking)
    assert_eq!(results.len(), 5, "Expected 5 transaction results");
    
    // Count how many succeeded
    let success_count = results.iter().filter(|r| r.is_ok()).count();
    println!("{} out of {} transactions succeeded", success_count, results.len());
    
    // At least one transaction should succeed
    assert!(success_count > 0, "Expected at least one transaction to succeed");
    
    // Clean up
    test.cleanup().await;
    Ok(())
}

// Concurrent Write Conflicts Test
#[tokio::test]
async fn test_write_conflicts_concurrent() -> Result<(), Error> {
    // Create test framework
    let test = ConcurrentTxnTest::new("test_write_conflicts_concurrent").await;
    
    // Run the concurrent writers test
    let num_writers = 10;
    let results = test.test_concurrent_writers(num_writers).await;
    
    // Verify all writers completed (didn't hang)
    assert_eq!(results.len(), num_writers, "Expected results from all writers");
    
    // Count successful writers
    let success_count = results.iter().filter(|r| r.is_ok()).count();
    println!("{} out of {} writers succeeded", success_count, results.len());
    
    // At least one writer should have succeeded
    assert!(success_count > 0, "Expected at least one writer to succeed");
    
    // Verify final state
    let mut txn = test.db.new_transaction().await;
    for i in 0..5 {
        let key = format!("key_{}", i).into_bytes();
        let (value, _) = txn.select(&key).await?;
        
        assert!(value.is_some(), "Key {} should have a value", i);
        let value_unwrapped = value.unwrap();
        let value_str = String::from_utf8_lossy(&value_unwrapped);
        println!("Final value for key_{}: {}", i, value_str);
    }
    txn.rollback().await?;
    
    // Clean up
    test.cleanup().await;
    Ok(())
}

// Concurrent Isolation Test
#[tokio::test]
async fn test_isolation_concurrent() -> Result<(), Error> {
    // Create test framework
    let test = ConcurrentTxnTest::new("test_isolation_concurrent").await;
    
    // Run the isolation test scenario with readers and writers
    let (reader_results, writer_results) = test.test_isolation_scenario(5, 3).await;
    
    // Verify readers completed successfully
    for (i, result) in reader_results.iter().enumerate() {
        match result {
            Ok(_) => println!("Reader {} completed successfully", i),
            Err(e) => println!("Reader {} failed with error: {}", i, e),
        }
    }
    
    // Count successful writers
    let successful_writers = writer_results.iter().filter(|r| r.is_ok()).count();
    println!("{} out of {} writers succeeded", successful_writers, writer_results.len());
    
    // At least one writer should have succeeded
    assert!(successful_writers > 0, "Expected at least one writer to succeed");
    
    // Verify final database state
    let mut txn = test.db.new_transaction().await;
    
    // Check data keys
    for i in 0..5 {
        let key = format!("key_{}", i).into_bytes();
        let (value, _) = txn.select(&key).await?;
        assert!(value.is_some(), "Key {} should have a value", i);
        
        let value_unwrapped = value.unwrap();
        let value_str = String::from_utf8_lossy(&value_unwrapped);
        println!("Final state for key_{}: {}", i, value_str);
    }
    
    // Check reader markers
    for i in 0..5 {
        let marker_key = format!("reader_{}_marker", i).into_bytes();
        let (marker, _) = txn.select(&marker_key).await?;
        
        if let Some(marker_bytes) = marker {
            println!("Reader {} marker found: {}", i, String::from_utf8_lossy(&marker_bytes));
        }
    }
    
    txn.rollback().await?;
    
    // Clean up
    test.cleanup().await;
    Ok(())
}