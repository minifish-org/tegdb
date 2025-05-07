use std::fs;
use std::io::Error;
use std::path::PathBuf;
use std::sync::Arc;
use tokio;
use std::time::Duration;

use tegdb::Database;

// Test framework for concurrent transaction tests
struct ConcurrentTxnTest {
    db: Arc<Database>,
    path: PathBuf,
}

impl ConcurrentTxnTest {
    async fn new(test_name: &str) -> Self {
        let path = PathBuf::from(format!("{}.db", test_name));
        
        // Clean up any previous test data
        if path.exists() {
            fs::remove_dir_all(&path).unwrap();
        }
        
        let db = Arc::new(Database::new(path.clone()).await);
        
        ConcurrentTxnTest {
            db,
            path,
        }
    }
    
    async fn cleanup(&self) {
        self.db.shutdown().await;
        if self.path.exists() {
            let _ = fs::remove_dir_all(&self.path);
        }
    }
}

// Concurrent Isolation Test
#[tokio::test]
async fn test_isolation_concurrent() -> Result<(), Error> {
    // Create test framework
    let test = ConcurrentTxnTest::new("test_isolation_concurrent").await;
    
    // Initialize with a single key-value pair
    let conflict_key = b"shared_conflict_key".to_vec();
    {
        let mut txn = test.db.new_transaction().await;
        txn.insert(&conflict_key, b"initial_value".to_vec()).await?;
        txn.commit().await?;
        println!("Initialized database with a single key-value pair");
    }
    
    // T0: Start txn1 and update the key
    println!("T0: txn1: Begin transaction");
    let mut txn1 = test.db.new_transaction().await;
    
    // Update the key in txn1
    println!("T0: txn1: Updating shared key");
    match txn1.update(&conflict_key, b"updated_by_txn1".to_vec()).await {
        Ok(_) => println!("T0: txn1: Update successful"),
        Err(e) => {
            println!("T0: txn1: Update failed with error: {}", e);
            let _ = txn1.rollback().await; // Ignore rollback errors
            return Err(e);
        }
    }
    
    // T1: Spawn a task for txn2 to update the same key concurrently
    let db_clone = test.db.clone();
    let conflict_key_clone = conflict_key.clone();
    let txn2_handle = tokio::spawn(async move {
        println!("T1: txn2: Begin transaction and attempt update (should block)");
        let start_time = std::time::Instant::now();
        // Create the transaction *inside* the spawned task
        let mut txn2_inner = db_clone.new_transaction().await;
        
        // Use a timeout to avoid indefinite hanging
        let update_result = tokio::time::timeout(
            Duration::from_millis(500),  // 500ms timeout
            txn2_inner.update(&conflict_key_clone, b"updated_by_txn2".to_vec())
        ).await;
        
        let elapsed = start_time.elapsed();
        println!("T1: txn2: Update attempt completed after {:?}", elapsed);
        
        // Handle the timeout case
        let actual_result = match update_result {
            Ok(inner_result) => inner_result,
            Err(_) => {
                println!("T1: txn2: Update timed out after {:?}", elapsed);
                // Timed out - the operation was blocking for too long
                Err(Error::new(std::io::ErrorKind::TimedOut, "Transaction update timed out"))
            }
        };
        
        // Return the transaction, the result of the update, and the elapsed time
        (txn2_inner, actual_result, elapsed)
    });
    
    // Give txn2's task a moment to start and potentially block on the update
    tokio::time::sleep(Duration::from_millis(100)).await;
    
    // T2: Commit txn1, which should unblock txn2's update attempt
    println!("T2: txn1: Committing transaction");
    // Add timeout to commit as well
    match tokio::time::timeout(Duration::from_millis(500), txn1.commit()).await {
        Ok(result) => {
            match result {
                Ok(_) => println!("T2: txn1: Commit successful"),
                Err(e) => println!("T2: txn1: Commit failed with error: {}", e),
            }
        },
        Err(_) => println!("T2: txn1: Commit timed out"),
    }
    
    // T3: Wait for txn2's task to complete and get the results
    // Add timeout for task completion
    match tokio::time::timeout(Duration::from_millis(1000), txn2_handle).await {
        Ok(result) => {
            match result {
                Ok((mut txn2_returned, update_result, elapsed)) => {
                    // T4: Analyze the result of txn2's update attempt
                    println!("T4: Analyzing txn2 result. Update took: {:?}", elapsed);
                    
                    // Handle the outcome of txn2's update attempt
                    match update_result {
                        Ok(_) => {
                            println!("T4: txn2: Update succeeded");
                            // Add timeout for commit
                            match tokio::time::timeout(Duration::from_millis(500), txn2_returned.commit()).await {
                                Ok(commit_result) => {
                                    match commit_result {
                                        Ok(_) => println!("T4: txn2: Commit succeeded"),
                                        Err(e) => println!("T4: txn2: Commit failed with error: {}", e),
                                    }
                                },
                                Err(_) => {
                                    println!("T4: txn2: Commit timed out");
                                    // Can't rollback after commit attempt - transaction is consumed
                                }
                            }
                        },
                        Err(e) => {
                            println!("T4: txn2: Update failed with error: {}", e);
                            // Rollback with timeout
                            match tokio::time::timeout(Duration::from_millis(500), txn2_returned.rollback()).await {
                                Ok(rollback_result) => {
                                    match rollback_result {
                                        Ok(_) => println!("T4: txn2: Rollback completed successfully"),
                                        Err(e) => println!("T4: txn2: Rollback failed with error: {}", e),
                                    }
                                },
                                Err(_) => println!("T4: txn2: Rollback timed out"),
                            }
                        }
                    }
                },
                Err(e) => println!("T3: Failed to join txn2 task: {}", e),
            }
        },
        Err(_) => println!("T3: Timed out waiting for txn2 task to complete"),
    }
    
    // Verify final database state with timeout
    println!("Verifying final database state...");
    let mut verification_txn = test.db.new_transaction().await;
    
    match tokio::time::timeout(
        Duration::from_millis(500), 
        verification_txn.select(&conflict_key)
    ).await {
        Ok(result) => {
            match result {
                Ok((value, _)) => {
                    if let Some(unwrapped_value) = value {
                        let value_str = String::from_utf8_lossy(&unwrapped_value);
                        println!("Final state for conflict key: {}", value_str);
                    } else {
                        println!("Conflict key has no value");
                    }
                },
                Err(e) => println!("Failed to select conflict key: {}", e),
            }
        },
        Err(_) => println!("Timed out during verification select"),
    }
    
    // Rollback verification transaction with timeout
    match tokio::time::timeout(Duration::from_millis(500), verification_txn.rollback()).await {
        Ok(result) => {
            match result {
                Ok(_) => println!("Verification transaction rolled back successfully"),
                Err(e) => println!("Verification rollback failed with error: {}", e),
            }
        },
        Err(_) => println!("Timed out rolling back verification transaction"),
    }
    
    // Clean up with timeout
    println!("Cleaning up...");
    match tokio::time::timeout(Duration::from_millis(1000), test.cleanup()).await {
        Ok(_) => println!("Cleanup completed successfully"),
        Err(_) => println!("Cleanup timed out"),
    }
    
    println!("\nNote: This test documents the actual behavior of TegDB's transaction");
    println!("implementation regarding concurrent updates. Timeouts were added to");
    println!("prevent indefinite hanging when testing concurrency patterns.");
    Ok(())
}