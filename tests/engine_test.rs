use std::path::PathBuf;
use std::fs;
use std::env;
use tegdb::{Engine, Result, Entry};

/// Creates a unique temporary file path for tests
fn temp_db_path(prefix: &str) -> PathBuf {
    let mut path = env::temp_dir();
    path.push(format!("tegdb_test_{}_{}", prefix, std::process::id()));
    path
}

#[test]
fn test_engine() -> Result<()> {
    let path = temp_db_path("basic");
    if path.exists() {
        fs::remove_file(&path)?;
    }
    
    let mut engine = Engine::new(path.clone())?;

    // Test set and get
    let key = b"key";
    let value = b"value";
    engine.set(key, value.to_vec())?;
    let get_value = engine.get(key).unwrap();
    assert_eq!(
        get_value,
        value,
        "Expected: {}, Got: {}",
        String::from_utf8_lossy(value),
        String::from_utf8_lossy(&get_value)
    );
    
    // Test del
    engine.del(key)?;
    let get_value = engine.get(key);
    assert_eq!(
        get_value,
        None,
        "Expected: None, Got: {}",
        String::from_utf8_lossy(get_value.as_deref().unwrap_or_default())
    );

    // Test scan
    let start_key = b"a";
    let end_key = b"z";
    engine.set(start_key, b"start_value".to_vec())?;
    engine.set(end_key, b"end_value".to_vec())?;
    
    let mut end_key_extended = Vec::new();
    end_key_extended.extend_from_slice(end_key);
    end_key_extended.push(0);

    let result = engine
        .scan(start_key.to_vec()..end_key_extended)?
        .collect::<Vec<_>>();
    assert_eq!(result.len(), 2);

    // Cleanup
    fs::remove_file(path)?;
    
    Ok(())
}

#[test]
fn test_persistence() -> Result<()> {
    let path = temp_db_path("persistence");
    if path.exists() {
        fs::remove_file(&path)?;
    }

    // Create engine and set values
    {
        let mut engine = Engine::new(path.clone())?;
        
        engine.set(b"key1", b"value1".to_vec())?;
        engine.set(b"key2", b"value2".to_vec())?;
        engine.set(b"key3", b"value3".to_vec())?;
        
        // Explicitly drop the engine to ensure file is closed properly
        drop(engine);
    }

    // Reopen and verify values
    {
        let engine = Engine::new(path.clone())?;
        
        let value1 = engine.get(b"key1").unwrap();
        let value2 = engine.get(b"key2").unwrap();
        let value3 = engine.get(b"key3").unwrap();
        
        assert_eq!(value1, b"value1", "Value for key1 not persisted correctly");
        assert_eq!(value2, b"value2", "Value for key2 not persisted correctly");
        assert_eq!(value3, b"value3", "Value for key3 not persisted correctly");
        
        // Drop engine again to ensure changes are persisted
        drop(engine);
    }

    // Reopen, update some values, and verify again
    {
        let mut engine = Engine::new(path.clone())?;
        
        engine.set(b"key2", b"updated_value".to_vec())?;
        engine.set(b"key4", b"value4".to_vec())?;
        
        // Explicitly drop the engine to ensure file is closed properly
        drop(engine);
    }

    // Reopen and verify updated values
    {
        let engine = Engine::new(path.clone())?;
        
        let value1 = engine.get(b"key1").unwrap();
        let value2 = engine.get(b"key2").unwrap();
        let value3 = engine.get(b"key3").unwrap();
        let value4 = engine.get(b"key4").unwrap();
        
        assert_eq!(value1, b"value1", "Original value for key1 was lost");
        assert_eq!(value2, b"updated_value", "Value for key2 not updated correctly");
        assert_eq!(value3, b"value3", "Original value for key3 was lost");
        assert_eq!(value4, b"value4", "New value for key4 not persisted correctly");
        
        // Drop engine again
        drop(engine);
    }

    // Reopen, delete a key, and verify again
    {
        let mut engine = Engine::new(path.clone())?;
        
        engine.del(b"key3")?;
        
        // Explicitly drop the engine to ensure file is closed properly
        drop(engine);
    }

    // Reopen and verify deletion
    {
        let engine = Engine::new(path.clone())?;
        
        let value3 = engine.get(b"key3");
        
        assert_eq!(value3, None, "Key3 was not deleted properly");
        
        // Drop engine for the last time
        drop(engine);
    }

    // Cleanup
    fs::remove_file(path)?;
    
    Ok(())
}

#[test]
fn test_basic_operations() -> Result<()> {
    let path = temp_db_path("basic_ops");
    if path.exists() {
        fs::remove_file(&path)?;
    }
    
    let mut engine = Engine::new(path.clone())?;
    
    engine.set(b"key", b"value".to_vec())?;
    let value = engine.get(b"key").unwrap();
    assert_eq!(value, b"value");
    
    fs::remove_file(path)?;
    
    Ok(())
}

#[test]
fn test_concurrent_access() -> Result<()> {
    let path = temp_db_path("concurrent");
    if path.exists() {
        fs::remove_file(&path)?;
    }
    
    // First engine instance
    let engine1 = Engine::new(path.clone())?;
    
    // This should fail - file should be locked
    let engine2_result = Engine::new(path.clone());
    assert!(engine2_result.is_err(), "Second engine should not be able to open locked database");
    
    // Clean up
    drop(engine1);
    
    // After dropping the first instance, we should be able to open it again
    let _engine3 = Engine::new(path.clone())?;
    
    fs::remove_file(path)?;
    
    Ok(())
}

#[test]
fn test_batch_operations() -> Result<()> {
    let path = temp_db_path("batch");
    if path.exists() {
        fs::remove_file(&path)?;
    }
    
    let mut engine = Engine::new(path.clone())?;
    
    // Create some entries for the batch operation
    let entries = vec![
        Entry::new(b"batch:1".to_vec(), Some(b"value1".to_vec())),
        Entry::new(b"batch:2".to_vec(), Some(b"value2".to_vec())),
        Entry::new(b"batch:3".to_vec(), Some(b"value3".to_vec())),
    ];
    
    // Perform the batch operation
    engine.batch(entries)?;
    
    // Verify the entries were written
    assert_eq!(engine.get(b"batch:1"), Some(b"value1".to_vec()));
    assert_eq!(engine.get(b"batch:2"), Some(b"value2".to_vec()));
    assert_eq!(engine.get(b"batch:3"), Some(b"value3".to_vec()));
    
    // Use batch to delete an entry
    let delete_entries = vec![
        Entry::new(b"batch:2".to_vec(), None),
    ];
    engine.batch(delete_entries)?;
    
    // Verify deletion
    assert_eq!(engine.get(b"batch:2"), None);
    
    // Clean up
    fs::remove_file(path)?;
    
    Ok(())
}

#[test]
fn test_empty_string_values() -> Result<()> {
    let path = temp_db_path("empty");
    if path.exists() {
        fs::remove_file(&path)?;
    }
    
    let mut engine = Engine::new(path.clone())?;
    
    // Empty value should be treated as delete
    engine.set(b"key1", vec![])?;
    assert_eq!(engine.get(b"key1"), None);
    
    // Set a value first
    engine.set(b"key2", b"value".to_vec())?;
    assert_eq!(engine.get(b"key2"), Some(b"value".to_vec()));
    
    // Then set it to empty (should delete)
    engine.set(b"key2", vec![])?;
    assert_eq!(engine.get(b"key2"), None);
    
    // Clean up
    fs::remove_file(path)?;
    
    Ok(())
}

#[test]
fn test_len_and_empty() -> Result<()> {
    let path = temp_db_path("len");
    if path.exists() {
        fs::remove_file(&path)?;
    }
    
    let mut engine = Engine::new(path.clone())?;
    
    // Should start empty
    assert_eq!(engine.len(), 0);
    assert!(engine.is_empty());
    
    // Add some entries
    engine.set(b"key1", b"value1".to_vec())?;
    engine.set(b"key2", b"value2".to_vec())?;
    
    // Should have 2 entries
    assert_eq!(engine.len(), 2);
    assert!(!engine.is_empty());
    
    // Delete an entry
    engine.del(b"key1")?;
    
    // Should have 1 entry
    assert_eq!(engine.len(), 1);
    assert!(!engine.is_empty());
    
    // Delete the last entry
    engine.del(b"key2")?;
    
    // Should be empty again
    assert_eq!(engine.len(), 0);
    assert!(engine.is_empty());
    
    // Clean up
    fs::remove_file(path)?;
    
    Ok(())
}

#[test]
fn test_engine_basic_operations_moved() -> Result<()> {
    let path = temp_db_path("basic_ops_moved");
    if path.exists() {
        fs::remove_file(&path)?;
    }
    
    let mut engine = Engine::new(path.clone())?;
    
    // Set and get
    engine.set(b"key1", b"value1".to_vec())?;
    assert_eq!(engine.get(b"key1"), Some(b"value1".to_vec()));
    
    // Delete
    engine.del(b"key1")?;
    assert_eq!(engine.get(b"key1"), None);
    
    // Cleanup
    fs::remove_file(path)?;
    
    Ok(())
}
