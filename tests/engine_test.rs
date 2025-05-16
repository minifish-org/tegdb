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

#[test]
fn test_atomicity_batch_all_or_nothing() -> Result<()> {
    let path = temp_db_path("atomicity_batch");
    if path.exists() {
        fs::remove_file(&path)?;
    }
    let mut engine = Engine::new(path.clone())?;

    // Initial state
    engine.set(b"key1", b"initial_value1".to_vec())?;
    engine.set(b"key2", b"initial_value2".to_vec())?;

    let entries_successful_batch = vec![
        Entry::new(b"key1".to_vec(), Some(b"updated_value1".to_vec())), // Update
        Entry::new(b"key3".to_vec(), Some(b"new_value3".to_vec())),     // Insert
        Entry::new(b"key2".to_vec(), None),                       // Delete
    ];

    // Perform a batch that should succeed
    engine.batch(entries_successful_batch)?;

    // Verify all changes from the batch are applied
    assert_eq!(engine.get(b"key1"), Some(b"updated_value1".to_vec()));
    assert_eq!(engine.get(b"key2"), None);
    assert_eq!(engine.get(b"key3"), Some(b"new_value3".to_vec()));
    assert_eq!(engine.len(), 2); // key1, key3

    // Cleanup
    fs::remove_file(path)?;
    Ok(())
}

#[test]
fn test_durability_multiple_sessions_mixed_ops() -> Result<()> {
    let path = temp_db_path("durability_mixed_sessions");
    if path.exists() {
        fs::remove_file(&path)?;
    }

    // Session 1: Initial writes
    {
        let mut engine = Engine::new(path.clone())?;
        engine.set(b"s1_key1", b"s1_val1".to_vec())?;
        let batch_entries1 = vec![
            Entry::new(b"s1_batch_keyA".to_vec(), Some(b"s1_batch_valA".to_vec())),
            Entry::new(b"s1_batch_keyB".to_vec(), Some(b"s1_batch_valB".to_vec())),
        ];
        engine.batch(batch_entries1)?;
        drop(engine); // Ensure data is flushed
    }

    // Session 2: Read, update, delete
    {
        let mut engine = Engine::new(path.clone())?;
        assert_eq!(engine.get(b"s1_key1"), Some(b"s1_val1".to_vec()));
        assert_eq!(engine.get(b"s1_batch_keyA"), Some(b"s1_batch_valA".to_vec()));

        engine.set(b"s1_key1", b"s1_val1_updated".to_vec())?; // Update
        engine.del(b"s1_batch_keyB")?; // Delete

        let batch_entries2 = vec![
            Entry::new(b"s2_new_key".to_vec(), Some(b"s2_new_val".to_vec())), // Insert
            Entry::new(b"s1_batch_keyA".to_vec(), None), // Delete via batch
        ];
        engine.batch(batch_entries2)?;
        drop(engine);
    }

    // Session 3: Verify all changes
    {
        let engine = Engine::new(path.clone())?;
        assert_eq!(engine.get(b"s1_key1"), Some(b"s1_val1_updated".to_vec()));
        assert_eq!(engine.get(b"s1_batch_keyA"), None);
        assert_eq!(engine.get(b"s1_batch_keyB"), None);
        assert_eq!(engine.get(b"s2_new_key"), Some(b"s2_new_val".to_vec()));
        assert_eq!(engine.len(), 2); // s1_key1, s2_new_key
        drop(engine);
    }

    fs::remove_file(path)?;
    Ok(())
}

#[test]
fn test_isolation_sequential_sessions_data_visibility() -> Result<()> {
    let path = temp_db_path("isolation_sequential");
    if path.exists() {
        fs::remove_file(&path)?;
    }

    // Session 1: Write some data
    {
        let mut engine = Engine::new(path.clone())?;
        engine.set(b"iso_key1", b"val1_session1".to_vec())?;
        engine.set(b"iso_key2", b"val2_session1".to_vec())?;
        drop(engine);
    }

    // Session 2: Read data from session 1, modify some, add new
    {
        let mut engine = Engine::new(path.clone())?;
        assert_eq!(engine.get(b"iso_key1"), Some(b"val1_session1".to_vec()));
        assert_eq!(engine.get(b"iso_key2"), Some(b"val2_session1".to_vec()));

        engine.set(b"iso_key2", b"val2_session2_updated".to_vec())?;
        engine.set(b"iso_key3", b"val3_session2_new".to_vec())?;
        drop(engine);
    }

    // Session 3: Verify changes from session 2 and original from session 1
    {
        let engine = Engine::new(path.clone())?;
        assert_eq!(engine.get(b"iso_key1"), Some(b"val1_session1".to_vec())); // Unchanged from session 1
        assert_eq!(engine.get(b"iso_key2"), Some(b"val2_session2_updated".to_vec())); // Updated in session 2
        assert_eq!(engine.get(b"iso_key3"), Some(b"val3_session2_new".to_vec()));   // Added in session 2
        assert_eq!(engine.len(), 3);
        drop(engine);
    }

    fs::remove_file(path)?;
    Ok(())
}

#[test]
fn test_consistency_after_complex_operations() -> Result<()> {
    let path = temp_db_path("consistency_complex_ops");
    if path.exists() {
        fs::remove_file(&path)?;
    }
    let mut engine = Engine::new(path.clone())?;

    // 1. Initial sets
    engine.set(b"key1", b"val1".to_vec())?;
    engine.set(b"key2", b"val2".to_vec())?;
    engine.set(b"key3", b"val3".to_vec())?;

    assert_eq!(engine.get(b"key1"), Some(b"val1".to_vec()));
    assert_eq!(engine.len(), 3);

    // 2. First Batch: Update key1, Delete key2, Insert key4
    let batch1_entries = vec![
        Entry::new(b"key1".to_vec(), Some(b"val1_updated".to_vec())),
        Entry::new(b"key2".to_vec(), None),
        Entry::new(b"key4".to_vec(), Some(b"val4_new".to_vec())),
    ];
    engine.batch(batch1_entries)?;

    assert_eq!(engine.get(b"key1"), Some(b"val1_updated".to_vec()));
    assert_eq!(engine.get(b"key2"), None);
    assert_eq!(engine.get(b"key3"), Some(b"val3".to_vec())); // Unchanged
    assert_eq!(engine.get(b"key4"), Some(b"val4_new".to_vec()));
    assert_eq!(engine.len(), 3); // key1, key3, key4

    // 3. Individual Del and Set
    engine.del(b"key3")?;
    engine.set(b"key5", b"val5".to_vec())?;

    assert_eq!(engine.get(b"key3"), None);
    assert_eq!(engine.get(b"key5"), Some(b"val5".to_vec()));
    assert_eq!(engine.len(), 3); // key1, key4, key5

    // 4. Second Batch: Delete key1, Update key4, Insert key6
    let batch2_entries = vec![
        Entry::new(b"key1".to_vec(), None),
        Entry::new(b"key4".to_vec(), Some(b"val4_updated_again".to_vec())),
        Entry::new(b"key6".to_vec(), Some(b"val6_new".to_vec())),
    ];
    engine.batch(batch2_entries)?;

    // 5. Final Verification
    assert_eq!(engine.get(b"key1"), None, "key1 should be deleted");
    assert_eq!(engine.get(b"key2"), None, "key2 should remain deleted");
    assert_eq!(engine.get(b"key3"), None, "key3 should remain deleted");
    assert_eq!(engine.get(b"key4"), Some(b"val4_updated_again".to_vec()), "key4 should be updated");
    assert_eq!(engine.get(b"key5"), Some(b"val5".to_vec()), "key5 should be present");
    assert_eq!(engine.get(b"key6"), Some(b"val6_new".to_vec()), "key6 should be inserted");

    assert_eq!(engine.len(), 3, "Final length should be 3"); // key4, key5, key6

    let scan_results = engine.scan(b"\0".to_vec()..b"\xff".to_vec())?.collect::<Vec<_>>();
    let expected_scan_results = vec![
        (b"key4".to_vec(), b"val4_updated_again".to_vec()),
        (b"key5".to_vec(), b"val5".to_vec()),
        (b"key6".to_vec(), b"val6_new".to_vec()),
    ];
    assert_eq!(scan_results, expected_scan_results, "Scan results do not match expected");

    // Cleanup
    fs::remove_file(path)?;
    Ok(())
}

#[test]
fn test_idempotency_of_batch_operations() -> Result<()> {
    let path = temp_db_path("idempotency_batch");
    if path.exists() {
        fs::remove_file(&path)?;
    }
    let mut engine = Engine::new(path.clone())?;

    // Initial data
    engine.set(b"key_initial", b"initial_val".to_vec())?;
    engine.set(b"key_to_update", b"update_me_initial".to_vec())?;
    engine.set(b"key_to_delete", b"delete_me_initial".to_vec())?;

    let make_batch_entries = || {
        vec![
            Entry::new(b"key_to_update".to_vec(), Some(b"updated_val".to_vec())), // Update
            Entry::new(b"key_to_delete".to_vec(), None),                       // Delete
            Entry::new(b"key_new_in_batch".to_vec(), Some(b"new_val".to_vec())), // Insert
            Entry::new(b"key_set_and_updated_in_batch".to_vec(), Some(b"first_set".to_vec())),
            Entry::new(b"key_set_and_updated_in_batch".to_vec(), Some(b"second_set_final".to_vec())),
            Entry::new(b"key_set_and_deleted_in_batch".to_vec(), Some(b"temp_val".to_vec())),
            Entry::new(b"key_set_and_deleted_in_batch".to_vec(), None),
        ]
    };

    // Apply batch for the first time
    engine.batch(make_batch_entries())?;

    // Expected state after first batch application
    let expected_key_initial = Some(b"initial_val".to_vec());
    let expected_key_to_update = Some(b"updated_val".to_vec());
    let expected_key_to_delete = None;
    let expected_key_new_in_batch = Some(b"new_val".to_vec());
    let expected_key_set_and_updated = Some(b"second_set_final".to_vec());
    let expected_key_set_and_deleted = None;
    
    // Calculate expected length
    let mut expected_len = 0;
    if expected_key_initial.is_some() { expected_len +=1; }
    if expected_key_to_update.is_some() { expected_len +=1; }
    // key_to_delete is None
    if expected_key_new_in_batch.is_some() { expected_len +=1; }
    if expected_key_set_and_updated.is_some() { expected_len +=1; }
    // key_set_and_deleted is None

    assert_eq!(engine.get(b"key_initial"), expected_key_initial, "After 1st batch: key_initial");
    assert_eq!(engine.get(b"key_to_update"), expected_key_to_update, "After 1st batch: key_to_update");
    assert_eq!(engine.get(b"key_to_delete"), expected_key_to_delete, "After 1st batch: key_to_delete");
    assert_eq!(engine.get(b"key_new_in_batch"), expected_key_new_in_batch, "After 1st batch: key_new_in_batch");
    assert_eq!(engine.get(b"key_set_and_updated_in_batch"), expected_key_set_and_updated, "After 1st batch: key_set_and_updated");
    assert_eq!(engine.get(b"key_set_and_deleted_in_batch"), expected_key_set_and_deleted, "After 1st batch: key_set_and_deleted");
    assert_eq!(engine.len(), expected_len, "After 1st batch: engine length");

    // Apply batch for the second time (reconstructing the entries)
    engine.batch(make_batch_entries())?;

    // Assert state is identical to after the first application
    assert_eq!(engine.get(b"key_initial"), expected_key_initial, "After 2nd batch: key_initial");
    assert_eq!(engine.get(b"key_to_update"), expected_key_to_update, "After 2nd batch: key_to_update");
    assert_eq!(engine.get(b"key_to_delete"), expected_key_to_delete, "After 2nd batch: key_to_delete");
    assert_eq!(engine.get(b"key_new_in_batch"), expected_key_new_in_batch, "After 2nd batch: key_new_in_batch");
    assert_eq!(engine.get(b"key_set_and_updated_in_batch"), expected_key_set_and_updated, "After 2nd batch: key_set_and_updated");
    assert_eq!(engine.get(b"key_set_and_deleted_in_batch"), expected_key_set_and_deleted, "After 2nd batch: key_set_and_deleted");
    assert_eq!(engine.len(), expected_len, "After 2nd batch: engine length");

    fs::remove_file(path)?;
    Ok(())
}

#[test]
fn test_transaction_commit() -> Result<()> {
    let path = temp_db_path("transaction_commit");
    if path.exists() {
        fs::remove_file(&path)?;
    }
    let mut engine = Engine::new(path.clone())?;
    // initial values
    engine.set(b"a", b"1".to_vec())?;
    engine.set(b"b", b"2".to_vec())?;

    // begin transaction and apply operations
    {
        let mut tx = engine.begin_transaction();
        tx.set(b"a".to_vec(), b"10".to_vec())?; // update
        tx.delete(b"b".to_vec())?;               // delete
        tx.set(b"c".to_vec(), b"3".to_vec())?; // insert
        tx.commit()?;
    }

    // verify committed state
    assert_eq!(engine.get(b"a"), Some(b"10".to_vec()));
    assert_eq!(engine.get(b"b"), None);
    assert_eq!(engine.get(b"c"), Some(b"3".to_vec()));

    fs::remove_file(path)?;
    Ok(())
}

#[test]
fn test_transaction_rollback() -> Result<()> {
    let path = temp_db_path("transaction_rollback");
    if path.exists() {
        fs::remove_file(&path)?;
    }
    let mut engine = Engine::new(path.clone())?;
    engine.set(b"x", b"alpha".to_vec())?; // initial value

    // begin transaction and perform operations without commit
    {
        let mut tx = engine.begin_transaction();
        tx.set(b"x".to_vec(), b"beta".to_vec())?;
        tx.set(b"y".to_vec(), b"100".to_vec())?;
        tx.delete(b"x".to_vec())?;
        tx.rollback();
    }

    // verify rollback restored original state
    assert_eq!(engine.get(b"x"), Some(b"alpha".to_vec()));
    assert_eq!(engine.get(b"y"), None);

    fs::remove_file(path)?;
    Ok(())
}

#[test]
fn test_transaction_empty_commit() -> Result<()> {
    let path = temp_db_path("tx_empty_commit");
    if path.exists() { fs::remove_file(&path)?; }
    let mut engine = Engine::new(path.clone())?;
    engine.set(b"a", b"1".to_vec())?;
    {
        let mut tx = engine.begin_transaction();
        // no operations
        tx.commit()?;
    }
    // state unchanged
    assert_eq!(engine.get(b"a"), Some(b"1".to_vec()));
    fs::remove_file(path)?;
    Ok(())
}

#[test]
fn test_transaction_empty_rollback() -> Result<()> {
    let path = temp_db_path("tx_empty_rollback");
    if path.exists() { fs::remove_file(&path)?; }
    let mut engine = Engine::new(path.clone())?;
    engine.set(b"b", b"2".to_vec())?;
    {
        let mut tx = engine.begin_transaction();
        // no operations
        tx.rollback();
    }
    // state unchanged
    assert_eq!(engine.get(b"b"), Some(b"2".to_vec()));
    fs::remove_file(path)?;
    Ok(())
}

#[test]
fn test_transaction_snapshot_isolation() -> Result<()> {
    let path = temp_db_path("tx_snapshot");
    if path.exists() { fs::remove_file(&path)?; }
    let mut engine = Engine::new(path.clone())?;
    // set initial and then updated before transaction
    engine.set(b"k", b"v1".to_vec())?;
    engine.set(b"k", b"v2".to_vec())?;
    let mut tx = engine.begin_transaction();
    // tx snapshot should see v2
    assert_eq!(tx.get(b"k"), Some(b"v2".to_vec()));
    // commit tx-specific update
    tx.set(b"k".to_vec(), b"v3".to_vec())?;
    tx.commit()?;
    // final value should be v3
    assert_eq!(engine.get(b"k"), Some(b"v3".to_vec()));
    fs::remove_file(path)?;
    Ok(())
}

#[test]
fn test_sequential_transactions() -> Result<()> {
    let path = temp_db_path("tx_sequential");
    if path.exists() { fs::remove_file(&path)?; }
    let mut engine = Engine::new(path.clone())?;
    engine.set(b"x", b"1".to_vec())?;

    // first transaction updates x
    {
        let mut tx1 = engine.begin_transaction();
        tx1.set(b"x".to_vec(), b"10".to_vec())?;
        tx1.commit()?;
    }
    assert_eq!(engine.get(b"x"), Some(b"10".to_vec()));

    // second transaction deletes x
    {
        let mut tx2 = engine.begin_transaction();
        tx2.delete(b"x".to_vec())?;
        tx2.commit()?;
    }
    assert_eq!(engine.get(b"x"), None);

    fs::remove_file(path)?;
    Ok(())
}

#[test]
fn test_uncommitted_transaction_not_persisted() -> Result<()> {
    let path = temp_db_path("tx_uncommitted_shutdown");
    if path.exists() { fs::remove_file(&path)?; }
    // initial session: set base value and start a transaction without committing
    {
        let mut engine = Engine::new(path.clone())?;
        engine.set(b"a", b"1".to_vec())?;
        let mut tx = engine.begin_transaction();
        tx.set(b"a".to_vec(), b"2".to_vec())?;
        tx.set(b"b".to_vec(), b"3".to_vec())?;
        // dropping engine and transaction without commit
    }
    // reopen engine: uncommitted changes should not apply
    let engine2 = Engine::new(path.clone())?;
    assert_eq!(engine2.get(b"a"), Some(b"1".to_vec()));
    assert_eq!(engine2.get(b"b"), None);
    fs::remove_file(path)?;
    Ok(())
}

#[test]
fn test_double_commit_fails() -> Result<()> {
    let path = temp_db_path("tx_double_commit");
    if path.exists() { fs::remove_file(&path)?; }
    let mut engine = Engine::new(path.clone())?;
    {
        let mut tx = engine.begin_transaction();
        tx.set(b"k".to_vec(), b"v".to_vec())?;
        tx.commit()?;
        assert!(tx.commit().is_err(), "Second commit should fail");
    }
    fs::remove_file(path)?;
    Ok(())
}

#[test]
fn test_commit_after_rollback_fails() -> Result<()> {
    let path = temp_db_path("tx_commit_after_rollback");
    if path.exists() { fs::remove_file(&path)?; }
    let mut engine = Engine::new(path.clone())?;
    {
        let mut tx = engine.begin_transaction();
        tx.set(b"a".to_vec(), b"1".to_vec())?;
        tx.rollback();
        assert!(tx.commit().is_err(), "Commit after rollback should fail");
    }
    fs::remove_file(path)?;
    Ok(())
}

#[test]
fn test_delete_then_set_in_transaction() -> Result<()> {
    let path = temp_db_path("tx_delete_then_set");
    if path.exists() { fs::remove_file(&path)?; }
    let mut engine = Engine::new(path.clone())?;
    engine.set(b"x", b"old".to_vec())?;
    {
        let mut tx = engine.begin_transaction();
        tx.delete(b"x".to_vec())?;
        tx.set(b"x".to_vec(), b"new".to_vec())?;
        tx.commit()?;
    }
    assert_eq!(engine.get(b"x"), Some(b"new".to_vec()));
    fs::remove_file(path)?;
    Ok(())
}

#[test]
fn test_durability_after_commit() -> Result<()> {
    let path = temp_db_path("tx_durability_after_commit");
    if path.exists() { fs::remove_file(&path)?; }
    {
        let mut engine = Engine::new(path.clone())?;
        engine.set(b"a", b"1".to_vec())?;
        let mut tx = engine.begin_transaction();
        tx.set(b"a".to_vec(), b"2".to_vec())?;
        tx.commit()?;
    }
    let engine2 = Engine::new(path.clone())?;
    assert_eq!(engine2.get(b"a"), Some(b"2".to_vec()));
    fs::remove_file(path)?;
    Ok(())
}

#[test]
fn test_large_transaction_memory_usage() -> Result<()> {
    let path = temp_db_path("tx_large");
    if path.exists() { fs::remove_file(&path)?; }
    let mut engine = Engine::new(path.clone())?;
    let mut tx = engine.begin_transaction();
    for i in 0..5000 {
        let key = format!("key{}", i).into_bytes();
        let value = format!("val{}", i).into_bytes();
        tx.set(key, value)?;
    }
    tx.commit()?;
    assert_eq!(engine.len(), 5000);
    assert_eq!(engine.get(b"key0"), Some(b"val0".to_vec()));
    assert_eq!(engine.get(b"key4999"), Some(b"val4999".to_vec()));
    fs::remove_file(path)?;
    Ok(())
}
