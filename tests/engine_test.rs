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

#[test]
fn test_overwrite_key() -> Result<()> {
    let path = temp_db_path("overwrite");
    if path.exists() {
        fs::remove_file(&path)?;
    }
    let mut engine = Engine::new(path.clone())?;

    engine.set(b"key1", b"value1".to_vec())?;
    assert_eq!(engine.get(b"key1"), Some(b"value1".to_vec()));

    // Overwrite key1
    engine.set(b"key1", b"value_new".to_vec())?;
    assert_eq!(engine.get(b"key1"), Some(b"value_new".to_vec()));

    fs::remove_file(path)?;
    Ok(())
}

#[test]
fn test_delete_non_existent_key() -> Result<()> {
    let path = temp_db_path("del_non_existent");
    if path.exists() {
        fs::remove_file(&path)?;
    }
    let mut engine = Engine::new(path.clone())?;

    // Delete a key that doesn't exist
    engine.del(b"non_existent_key")?;
    assert_eq!(engine.get(b"non_existent_key"), None);

    fs::remove_file(path)?;
    Ok(())
}

#[test]
fn test_scan_empty_db() -> Result<()> {
    let path = temp_db_path("scan_empty");
    if path.exists() {
        fs::remove_file(&path)?;
    }
    let engine = Engine::new(path.clone())?;

    let scan_result = engine.scan(b"a".to_vec()..b"z".to_vec())?.collect::<Vec<_>>();
    assert!(scan_result.is_empty());

    fs::remove_file(path)?;
    Ok(())
}

#[test]
fn test_scan_no_match() -> Result<()> {
    let path = temp_db_path("scan_no_match");
    if path.exists() {
        fs::remove_file(&path)?;
    }
    let mut engine = Engine::new(path.clone())?;

    engine.set(b"key1", b"value1".to_vec())?;
    engine.set(b"key2", b"value2".to_vec())?;

    // Scan a range that won't match any existing keys
    let scan_result = engine.scan(b"x".to_vec()..b"z".to_vec())?.collect::<Vec<_>>();
    assert!(scan_result.is_empty());

    fs::remove_file(path)?;
    Ok(())
}

#[test]
fn test_special_characters_keys_values() -> Result<()> {
    let path = temp_db_path("special_chars");
    if path.exists() {
        fs::remove_file(&path)?;
    }
    let mut engine = Engine::new(path.clone())?;

    let keys = vec![
        b"key with spaces".to_vec(),
        b"key/with/slashes".to_vec(),
        b"key\\with\\backslashes".to_vec(),
        b"key\"with\'quotes".to_vec(),
        b"key\twith\nnewlines".to_vec(),
        vec![0, 1, 2, 3, 4, 5], // Non-UTF8 key
    ];
    let values = vec![
        b"value with spaces".to_vec(),
        b"value/with/slashes".to_vec(),
        b"value\\with\\backslashes".to_vec(),
        b"value\"with\'quotes".to_vec(),
        b"value\twith\nnewlines".to_vec(),
        vec![10, 20, 30, 40, 50], // Non-UTF8 value
    ];

    for i in 0..keys.len() {
        engine.set(&keys[i], values[i].clone())?;
        assert_eq!(engine.get(&keys[i]), Some(values[i].clone()));
    }

    fs::remove_file(path)?;
    Ok(())
}

#[test]
fn test_batch_mixed_operations() -> Result<()> {
    let path = temp_db_path("batch_mixed");
    if path.exists() {
        fs::remove_file(&path)?;
    }
    let mut engine = Engine::new(path.clone())?;

    // Initial setup
    engine.set(b"key1", b"initial1".to_vec())?;
    engine.set(b"key2", b"initial2".to_vec())?;
    engine.set(b"key3", b"initial3".to_vec())?;

    let entries = vec![
        Entry::new(b"key1".to_vec(), Some(b"updated1".to_vec())), // Update
        Entry::new(b"key2".to_vec(), None),                       // Delete
        Entry::new(b"key4".to_vec(), Some(b"new4".to_vec())),     // Insert
    ];

    engine.batch(entries)?;

    assert_eq!(engine.get(b"key1"), Some(b"updated1".to_vec()));
    assert_eq!(engine.get(b"key2"), None);
    assert_eq!(engine.get(b"key3"), Some(b"initial3".to_vec())); // Unchanged
    assert_eq!(engine.get(b"key4"), Some(b"new4".to_vec()));

    fs::remove_file(path)?;
    Ok(())
}

#[test]
fn test_batch_empty() -> Result<()> {
    let path = temp_db_path("batch_empty");
    if path.exists() {
        fs::remove_file(&path)?;
    }
    let mut engine = Engine::new(path.clone())?;

    engine.set(b"key1", b"value1".to_vec())?;
    let initial_len = engine.len();

    let entries: Vec<Entry> = vec![];
    engine.batch(entries)?;

    assert_eq!(engine.get(b"key1"), Some(b"value1".to_vec()));
    assert_eq!(engine.len(), initial_len);

    fs::remove_file(path)?;
    Ok(())
}

#[test]
#[should_panic(expected = "range start is greater than range end in BTreeMap")]
fn test_scan_reverse_range() {
    let path = temp_db_path("scan_reverse");
    if path.exists() {
        fs::remove_file(&path).unwrap();
    }
    let mut engine = Engine::new(path.clone()).unwrap();

    engine.set(b"a", b"val_a".to_vec()).unwrap();
    engine.set(b"b", b"val_b".to_vec()).unwrap();
    engine.set(b"c", b"val_c".to_vec()).unwrap();

    // Start key is greater than end key
    // This should panic based on BTreeMap behavior
    let _scan_result = engine.scan(b"c".to_vec()..b"a".to_vec()).unwrap().collect::<Vec<_>>();
    
    // The following lines will not be reached if the panic occurs as expected.
    fs::remove_file(path).unwrap();
}

#[test]
fn test_persistence_after_batch() -> Result<()> {
    let path = temp_db_path("persistence_batch");
    if path.exists() {
        fs::remove_file(&path)?;
    }

    {
        let mut engine = Engine::new(path.clone())?;
        let entries = vec![
            Entry::new(b"batch_key1".to_vec(), Some(b"val1".to_vec())),
            Entry::new(b"batch_key2".to_vec(), Some(b"val2".to_vec())),
        ];
        engine.batch(entries)?;
        engine.set(b"single_key", b"val_single".to_vec())?; // Mix with single set
        let entries_delete = vec![
            Entry::new(b"batch_key1".to_vec(), None),
        ];
        engine.batch(entries_delete)?;

        drop(engine);
    }

    {
        let engine = Engine::new(path.clone())?;
        assert_eq!(engine.get(b"batch_key1"), None);
        assert_eq!(engine.get(b"batch_key2"), Some(b"val2".to_vec()));
        assert_eq!(engine.get(b"single_key"), Some(b"val_single".to_vec()));
        drop(engine);
    }

    fs::remove_file(path)?;
    Ok(())
}

#[test]
fn test_len_is_empty_after_batch() -> Result<()> {
    let path = temp_db_path("len_batch");
    if path.exists() {
        fs::remove_file(&path)?;
    }
    let mut engine = Engine::new(path.clone())?;

    assert!(engine.is_empty());
    assert_eq!(engine.len(), 0);

    let entries_insert = vec![
        Entry::new(b"k1".to_vec(), Some(b"v1".to_vec())),
        Entry::new(b"k2".to_vec(), Some(b"v2".to_vec())),
    ];
    engine.batch(entries_insert)?;
    assert!(!engine.is_empty());
    assert_eq!(engine.len(), 2);

    let entries_update_delete = vec![
        Entry::new(b"k1".to_vec(), Some(b"v1_new".to_vec())), // Update
        Entry::new(b"k2".to_vec(), None),                  // Delete
        Entry::new(b"k3".to_vec(), Some(b"v3".to_vec())),  // Insert
    ];
    engine.batch(entries_update_delete)?;
    assert!(!engine.is_empty());
    assert_eq!(engine.len(), 2); // k1 updated, k2 deleted, k3 inserted

    let entries_delete_all = vec![
        Entry::new(b"k1".to_vec(), None),
        Entry::new(b"k3".to_vec(), None),
    ];
    engine.batch(entries_delete_all)?;
    assert!(engine.is_empty());
    assert_eq!(engine.len(), 0);

    fs::remove_file(path)?;
    Ok(())
}

#[test]
fn test_scan_boundary_conditions() -> Result<()> {
    let path = temp_db_path("scan_boundaries");
    if path.exists() {
        fs::remove_file(&path)?;
    }
    let mut engine = Engine::new(path.clone())?;

    engine.set(b"key1", b"value1".to_vec())?;
    engine.set(b"key2", b"value2".to_vec())?;
    engine.set(b"key3", b"value3".to_vec())?;

    // Scan for "key1" (exclusive end for "key2")
    let result1 = engine.scan(b"key1".to_vec()..b"key2".to_vec())?.collect::<Vec<_>>();
    assert_eq!(result1.len(), 1);
    assert_eq!(result1[0].0, b"key1".to_vec());
    assert_eq!(result1[0].1, b"value1".to_vec());

    // Scan for "key2" and "key3"
    // To include "key3", the end of the range must be > "key3"
    let mut end_key3_inclusive = b"key3".to_vec();
    end_key3_inclusive.push(0); // Makes it "key3\x00"
    let result2 = engine.scan(b"key2".to_vec()..end_key3_inclusive)?.collect::<Vec<_>>();
    assert_eq!(result2.len(), 2);
    assert_eq!(result2[0].0, b"key2".to_vec());
    assert_eq!(result2[0].1, b"value2".to_vec());
    assert_eq!(result2[1].0, b"key3".to_vec());
    assert_eq!(result2[1].1, b"value3".to_vec());

    // Scan a range that includes nothing (e.g., between "key1" and "key2" but not including them)
    let mut start_after_key1 = b"key1".to_vec();
    start_after_key1.push(255); // e.g., "key1\xff"
    let result3 = engine.scan(start_after_key1..b"key2".to_vec())?.collect::<Vec<_>>();
    assert!(result3.is_empty());

    // Scan all keys (range from before "key1" to after "key3")
    let mut end_beyond_key3 = b"key3".to_vec();
    end_beyond_key3.push(0); 
    let result_all = engine.scan(b"key0".to_vec()..end_beyond_key3)?.collect::<Vec<_>>();
    assert_eq!(result_all.len(), 3);
    assert_eq!(result_all[0].0, b"key1".to_vec());
    assert_eq!(result_all[1].0, b"key2".to_vec());
    assert_eq!(result_all[2].0, b"key3".to_vec());

    // Scan with a start key that doesn't exist but is before the first key
    let result_before_all = engine.scan(b"a".to_vec()..b"key2".to_vec())?.collect::<Vec<_>>();
    assert_eq!(result_before_all.len(), 1);
    assert_eq!(result_before_all[0].0, b"key1".to_vec());

    // Scan with an end key that includes the last key
    let result_includes_last = engine.scan(b"key3".to_vec()..b"z".to_vec())?.collect::<Vec<_>>();
    assert_eq!(result_includes_last.len(), 1);
    assert_eq!(result_includes_last[0].0, b"key3".to_vec());

    fs::remove_file(path)?;
    Ok(())
}

#[test]
fn test_batch_with_duplicate_keys_in_batch() -> Result<()> {
    let path = temp_db_path("batch_duplicates");
    if path.exists() {
        fs::remove_file(&path)?;
    }
    let mut engine = Engine::new(path.clone())?;

    engine.set(b"key_A", b"initial_A".to_vec())?; 
    engine.set(b"key_B", b"initial_B".to_vec())?; 

    let entries = vec![
        Entry::new(b"key_A".to_vec(), Some(b"value_A1".to_vec())), // First op on key_A
        Entry::new(b"key_C".to_vec(), Some(b"value_C1".to_vec())), // New key
        Entry::new(b"key_A".to_vec(), Some(b"value_A2".to_vec())), // Second op on key_A
        Entry::new(b"key_D".to_vec(), Some(b"value_D1".to_vec())), // Another new key
        Entry::new(b"key_C".to_vec(), None),                       // Delete key_C
        Entry::new(b"key_A".to_vec(), Some(b"value_A3".to_vec())), // Third op on key_A (final value)
    ];

    engine.batch(entries)?;

    assert_eq!(engine.get(b"key_A"), Some(b"value_A3".to_vec()));
    assert_eq!(engine.get(b"key_B"), Some(b"initial_B".to_vec())); // Unchanged
    assert_eq!(engine.get(b"key_C"), None); // Deleted within batch
    assert_eq!(engine.get(b"key_D"), Some(b"value_D1".to_vec())); // Inserted

    // Expected keys: key_A, key_B, key_D
    assert_eq!(engine.len(), 3);

    fs::remove_file(path)?;
    Ok(())
}
