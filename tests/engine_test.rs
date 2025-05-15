use std::path::PathBuf;
use std::fs;
use tegdb::Engine;

#[test]
fn test_engine() {
    let path = PathBuf::from("test.db");
    let mut engine = Engine::new(path.clone());

    // Test set and get
    let key = b"key";
    let value = b"value";
    engine.set(key, value.to_vec()).unwrap();
    let get_value = engine.get(key).unwrap();
    assert_eq!(
        get_value,
        value,
        "Expected: {}, Got: {}",
        String::from_utf8_lossy(value),
        String::from_utf8_lossy(&get_value)
    );

    // Test del
    engine.del(key).unwrap();
    let get_value = engine.get(key);
    assert_eq!(
        get_value,
        None,
        "Expected: {}, Got: {}",
        String::from_utf8_lossy(&[]),
        String::from_utf8_lossy(get_value.as_deref().unwrap_or_default())
    );

    // Test scan
    let start_key = b"a";
    let end_key = b"z";
    engine.set(start_key, b"start_value".to_vec()).unwrap();
    engine.set(end_key, b"end_value".to_vec()).unwrap();
    let mut end_key_extended = Vec::new();
    end_key_extended.extend_from_slice(end_key);
    end_key_extended.extend_from_slice(&[1u8]);
    let result = engine
        .scan(start_key.to_vec()..end_key_extended)
        .unwrap()
        .collect::<Vec<_>>();
    let expected = vec![
        (start_key.to_vec(), b"start_value".to_vec()),
        (end_key.to_vec(), b"end_value".to_vec()),
    ];
    let expected_strings: Vec<(String, String)> = expected
        .iter()
        .map(|(k, v)| {
            (
                String::from_utf8_lossy(k).into_owned(),
                String::from_utf8_lossy(v).into_owned(),
            )
        })
        .collect();
    let result_strings: Vec<(String, String)> = result
        .iter()
        .map(|(k, v)| {
            (
                String::from_utf8_lossy(k).into_owned(),
                String::from_utf8_lossy(v).into_owned(),
            )
        })
        .collect();
    assert_eq!(
        result_strings, expected_strings,
        "Expected: {:?}, Got: {:?}",
        expected_strings, result_strings
    );

    // Clean up
    drop(engine);
    fs::remove_file(path).unwrap();
}

#[test]
fn test_engine_durability() {
    // Create a test database file path
    let path = PathBuf::from("durability_test.db");
    
    // Remove any existing test file to start clean
    let _ = fs::remove_file(&path);
    
    // Insert test data
    {
        let mut engine = Engine::new(path.clone());
        
        // Insert some key-value pairs
        engine.set(b"key1", b"value1".to_vec()).unwrap();
        engine.set(b"key2", b"value2".to_vec()).unwrap();
        engine.set(b"key3", b"value3".to_vec()).unwrap();
        
        // Explicitly drop the engine to ensure file is closed properly
        drop(engine);
    }
    
    // Reopen the database and verify data persistence
    {
        let mut engine = Engine::new(path.clone());
        
        // Verify the data is still there
        let value1 = engine.get(b"key1").unwrap();
        let value2 = engine.get(b"key2").unwrap();
        let value3 = engine.get(b"key3").unwrap();
        
        assert_eq!(value1, b"value1", "Value for key1 not persisted correctly");
        assert_eq!(value2, b"value2", "Value for key2 not persisted correctly");
        assert_eq!(value3, b"value3", "Value for key3 not persisted correctly");
        
        // Test updating a value and adding a new one
        engine.set(b"key2", b"updated_value".to_vec()).unwrap();
        engine.set(b"key4", b"value4".to_vec()).unwrap();
        
        // Drop engine again to ensure changes are persisted
        drop(engine);
    }
    
    // Open a third time to verify the updates were persisted
    {
        let mut engine = Engine::new(path.clone());
        
        // Check original keys
        let value1 = engine.get(b"key1").unwrap();
        let value2 = engine.get(b"key2").unwrap();
        let value3 = engine.get(b"key3").unwrap();
        let value4 = engine.get(b"key4").unwrap();
        
        assert_eq!(value1, b"value1", "Original value for key1 was lost");
        assert_eq!(value2, b"updated_value", "Updated value for key2 was not persisted");
        assert_eq!(value3, b"value3", "Original value for key3 was lost");
        assert_eq!(value4, b"value4", "New value for key4 was not persisted");
        
        // Test deletion persistence
        engine.del(b"key3").unwrap();
        
        // Drop engine again
        drop(engine);
    }
    
    // Open one final time to verify deletion was persisted
    {
        let mut engine = Engine::new(path.clone());
        
        // Verify key3 is gone
        let value3 = engine.get(b"key3");
        assert!(value3.is_none(), "Deletion of key3 was not persisted");
        
        // Clean up
        drop(engine);
        fs::remove_file(path).unwrap();
    }
}

#[cfg(test)]
mod thread_safety_tests {
    use super::*;
    
    #[test]
    fn test_file_locking_prevents_concurrent_access() {
        let path = PathBuf::from("file_lock_test.db");
        let _ = fs::remove_file(&path);
        
        // Create the first engine instance
        let engine1 = Engine::new(path.clone());
        
        // Try to create a second engine instance with the same path
        // This should fail because the file is locked by the first instance
        let result = std::panic::catch_unwind(|| {
            Engine::new(path.clone())
        });
        
        // Verify the second instance failed to create
        assert!(result.is_err(), "Expected second Engine instance to fail due to file locking");
        
        // Drop the first instance to release the lock
        drop(engine1);
        
        // Now we should be able to create a new instance
        let engine2 = Engine::new(path.clone());
        
        // Clean up
        drop(engine2);
        fs::remove_file(path).unwrap();
    }
    
    #[test]
    fn test_engine_single_thread_safety() {
        // This test demonstrates that Engine works fine within a single thread
        let path = PathBuf::from("single_thread_test.db");
        let _ = fs::remove_file(&path);
        
        // Create and use engine in the same thread
        let mut engine = Engine::new(path.clone());
        engine.set(b"key", b"value".to_vec()).unwrap();
        let value = engine.get(b"key").unwrap();
        assert_eq!(value, b"value");
        
        drop(engine);
        fs::remove_file(path).unwrap();
    }

    // Test that we can use multiple independent Engine instances
    // within the same thread (but not concurrently)
    #[test]
    fn test_multiple_engines_same_thread() {
        let path1 = PathBuf::from("engine1_test.db");
        let path2 = PathBuf::from("engine2_test.db");
        
        let _ = fs::remove_file(&path1);
        let _ = fs::remove_file(&path2);
        
        // Use first engine
        {
            let mut engine1 = Engine::new(path1.clone());
            engine1.set(b"key1", b"value1".to_vec()).unwrap();
            drop(engine1);
        }
        
        // Use second engine
        {
            let mut engine2 = Engine::new(path2.clone());
            engine2.set(b"key2", b"value2".to_vec()).unwrap();
            drop(engine2);
        }
        
        // Reopen both engines to verify data
        {
            let mut engine1 = Engine::new(path1.clone());
            let value1 = engine1.get(b"key1").unwrap();
            assert_eq!(value1, b"value1");
            drop(engine1);
            
            let mut engine2 = Engine::new(path2.clone());
            let value2 = engine2.get(b"key2").unwrap();
            assert_eq!(value2, b"value2");
            drop(engine2);
        }
        
        // Clean up
        fs::remove_file(path1).unwrap();
        fs::remove_file(path2).unwrap();
    }
}
