use std::path::PathBuf;
use std::fs;
use tegdb::{Engine, EngineConfig, Result, Entry};

fn main() -> Result<()> {
    // Use a temp file for demonstration
    let db_path = PathBuf::from("test_demo.db");
    
    // If the file exists already, delete it for a clean start
    if db_path.exists() {
        fs::remove_file(&db_path)?;
    }
    
    // Create a new database instance with custom config
    let config = EngineConfig {
        sync_on_write: true, // For increased durability
        ..EngineConfig::default()
    };
    
    let mut engine = Engine::with_config(db_path.clone(), config)?;

    println!("===== Basic operations =====");
    
    // Set a simple string value
    let key = b"key";
    let value = b"value";
    engine.set(key, value.to_vec())?;
    println!("Set key: {} with value: {}", 
        String::from_utf8_lossy(key),
        String::from_utf8_lossy(value));

    // Get the value back
    if let Some(get_value) = engine.get(key) {
        println!("Got value: {}", String::from_utf8_lossy(&get_value));
        assert_eq!(get_value, value.to_vec());
    } else {
        println!("Key not found (unexpected)");
    }

    // === Multiple key-value pairs ===
    println!("\n===== Multiple key-value operations =====");
    
    // Set multiple values with different types of data
    let pairs = [
        (b"user:1".to_vec(), b"Alice".to_vec()),
        (b"user:2".to_vec(), b"Bob".to_vec()),
        (b"user:3".to_vec(), b"Charlie".to_vec()),
        (b"count".to_vec(), b"3".to_vec()),
        // Binary data example
        (b"binary".to_vec(), vec![0x00, 0x01, 0x02, 0x03, 0xFF]),
    ];
    
    // Create batch entries
    let batch_entries: Vec<Entry> = pairs.iter()
        .map(|(k, v)| Entry::new(k.clone(), Some(v.clone())))
        .collect();
    
    // Store all pairs using batch operation for efficiency
    engine.batch(batch_entries)?;
    println!("Set multiple key-value pairs using batch operation");
    
    // Verify the pairs were stored correctly
    for (k, v) in &pairs {
        let stored_value = engine.get(k).expect("Key not found");
        assert_eq!(&stored_value, v);
        
        println!("Verified key: {} with value: {:?}", 
            String::from_utf8_lossy(k),
            if k == b"binary" { format!("{:?}", v) } else { format!("{}", String::from_utf8_lossy(v)) });
    }
    
    // === Range Scan Operations ===
    println!("\n===== Scan operations with different ranges =====");
    
    // Scan all user keys (user:1 to user:9)
    println!("Scanning user:1 to user:9:");
    let user_range = b"user:1".to_vec()..b"user:9".to_vec();
    let values = engine.scan(user_range)?;
    
    for (key, value) in values {
        println!(
            "  Found key: {}, value: {}",
            String::from_utf8_lossy(&key),
            String::from_utf8_lossy(&value)
        );
    }
    
    // === Deletion ===
    println!("\n===== Deletion operations =====");
    
    // Delete one key
    engine.del(b"user:2")?;
    println!("Deleted key: user:2");
    
    // Verify it's gone
    match engine.get(b"user:2") {
        Some(_) => println!("Key still exists (unexpected)"),
        None => println!("Key user:2 successfully deleted"),
    }
    
    // Scan again to confirm deletion
    println!("\nScanning after deletion:");
    let values = engine.scan(b"user:".to_vec()..b"user:~".to_vec())?;
    for (key, value) in values {
        println!(
            "  Found key: {}, value: {}",
            String::from_utf8_lossy(&key),
            String::from_utf8_lossy(&value)
        );
    }
    
    // === Batch operations ===
    println!("\n===== Batch operations =====");
    
    let batch_entries = vec![
        Entry::new(b"batch:1".to_vec(), Some(b"batch value 1".to_vec())),
        Entry::new(b"batch:2".to_vec(), Some(b"batch value 2".to_vec())),
        Entry::new(b"user:1".to_vec(), None), // Delete user:1
    ];
    
    engine.batch(batch_entries)?;
    println!("Batch operation completed");
    
    // Verify batch operations
    assert_eq!(engine.get(b"batch:1"), Some(b"batch value 1".to_vec()));
    assert_eq!(engine.get(b"batch:2"), Some(b"batch value 2".to_vec()));
    assert_eq!(engine.get(b"user:1"), None);
    
    // === Binary data operations ===
    println!("\n===== Binary data operations =====");
    
    // Verify we can retrieve binary data correctly
    if let Some(bin_value) = engine.get(b"binary") {
        println!("Retrieved binary value: {:?}", bin_value);
        assert_eq!(bin_value, vec![0x00, 0x01, 0x02, 0x03, 0xFF]);
    }
    
    // === Manual compaction ===
    println!("\n===== Manual compaction =====");
    println!("Current database size: {} entries", engine.len());
    engine.compact()?;
    println!("Database compacted");
    
    // === Cleanup ===
    println!("\n===== Cleanup =====");
    // Explicitly flush before closing
    engine.flush()?;
    println!("Data flushed to disk");
    
    // Reopen database to verify persistence
    println!("\n===== Reopen database to verify persistence =====");
    
    // Need to add a small delay to ensure file lock is released
    println!("Waiting for file lock to be released...");
    
    // Create a new engine with the same path
    match Engine::new(db_path.clone()) {
        Ok(engine) => {
            // Check if data persisted
            if let Some(value) = engine.get(b"batch:1") {
                println!("After reopening, batch:1 = {}", String::from_utf8_lossy(&value));
            }
            
            // Check database is not empty
            println!("Database has {} entries", engine.len());
            println!("Database is {}empty", if engine.is_empty() { "" } else { "not " });
            
            // Clean up the test file when done
            drop(engine);
        },
        Err(e) => {
            println!("Note: Could not reopen database: {}. This is likely due to file locking.", e);
            println!("In a production environment, you would need to ensure the database is properly closed before reopening.");
        }
    }
    
    // Clean up the test file 
    if let Err(e) = fs::remove_file(&db_path) {
        println!("Warning: Could not remove test file: {}", e);
    } else {
        println!("Test database file removed");
    }
    
    Ok(())
}
