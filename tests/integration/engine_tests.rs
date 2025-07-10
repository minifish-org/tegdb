//! - Storage engine operations

use std::env;
use std::fs;
use std::path::PathBuf;
use tegdb::Result;
use tegdb::storage_engine::{EngineConfig, StorageEngine};

#[path = "../helpers/test_helpers.rs"]
mod test_helpers;

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

    let mut engine = StorageEngine::new(path.clone())?;

    // Test set and get
    let key = b"key";
    let value = b"value";
    engine.set(key, value.to_vec())?;
    let get_value = engine.get(key).unwrap();
    assert_eq!(
        &*get_value,
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
        let mut engine = StorageEngine::new(path.clone())?;

        engine.set(b"key1", b"value1".to_vec())?;
        engine.set(b"key2", b"value2".to_vec())?;
        engine.set(b"key3", b"value3".to_vec())?;

        // Explicitly drop the engine to ensure file is closed properly
        drop(engine);
    }

    // Reopen and verify values
    {
        let engine = StorageEngine::new(path.clone())?;

        let value1 = engine.get(b"key1").unwrap();
        let value2 = engine.get(b"key2").unwrap();
        let value3 = engine.get(b"key3").unwrap();

        assert_eq!(
            &*value1, b"value1",
            "Value for key1 not persisted correctly"
        );
        assert_eq!(
            &*value2, b"value2",
            "Value for key2 not persisted correctly"
        );
        assert_eq!(
            &*value3, b"value3",
            "Value for key3 not persisted correctly"
        );

        // Drop engine again to ensure changes are persisted
        drop(engine);
    }

    // Reopen, update some values, and verify again
    {
        let mut engine = StorageEngine::new(path.clone())?;

        engine.set(b"key2", b"updated_value".to_vec())?;
        engine.set(b"key4", b"value4".to_vec())?;

        // Explicitly drop the engine to ensure file is closed properly
        drop(engine);
    }

    // Reopen and verify updated values
    {
        let engine = StorageEngine::new(path.clone())?;

        let value1 = engine.get(b"key1").unwrap();
        let value2 = engine.get(b"key2").unwrap();
        let value3 = engine.get(b"key3").unwrap();
        let value4 = engine.get(b"key4").unwrap();

        assert_eq!(&*value1, b"value1", "Original value for key1 was lost");
        assert_eq!(
            &*value2, b"updated_value",
            "Value for key2 not updated correctly"
        );
        assert_eq!(&*value3, b"value3", "Original value for key3 was lost");
        assert_eq!(
            &*value4, b"value4",
            "New value for key4 not persisted correctly"
        );

        // Drop engine again
        drop(engine);
    }

    // Reopen, delete a key, and verify again
    {
        let mut engine = StorageEngine::new(path.clone())?;

        engine.del(b"key3")?;

        // Explicitly drop the engine to ensure file is closed properly
        drop(engine);
    }

    // Reopen and verify deletion
    {
        let engine = StorageEngine::new(path.clone())?;

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

    let mut engine = StorageEngine::new(path.clone())?;

    engine.set(b"key", b"value".to_vec())?;
    let value = engine.get(b"key").unwrap();
    assert_eq!(&*value, b"value");

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
    let engine1 = StorageEngine::new(path.clone())?;

    // This should fail - file should be locked
    let engine2_result = StorageEngine::new(path.clone());
    assert!(
        engine2_result.is_err(),
        "Second engine should not be able to open locked database"
    );

    // Clean up
    drop(engine1);

    // After dropping the first instance, we should be able to open it again
    let _engine3 = StorageEngine::new(path.clone())?;

    fs::remove_file(path)?;

    Ok(())
}

#[test]
fn test_empty_string_values() -> Result<()> {
    let path = temp_db_path("empty");
    if path.exists() {
        fs::remove_file(&path)?;
    }

    let mut engine = StorageEngine::new(path.clone())?;

    // Empty value should be treated as delete
    engine.set(b"key1", vec![])?;
    assert_eq!(engine.get(b"key1"), None);

    // Set a value first
    engine.set(b"key2", b"value".to_vec())?;
    assert_eq!(
        engine.get(b"key2").map(|a| a.as_ref().to_vec()),
        Some(b"value".to_vec())
    );

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

    let mut engine = StorageEngine::new(path.clone())?;

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

    let mut engine = StorageEngine::new(path.clone())?;

    // Set and get
    engine.set(b"key1", b"value1".to_vec())?;
    assert_eq!(
        engine.get(b"key1").map(|a| a.as_ref().to_vec()),
        Some(b"value1".to_vec())
    );

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
    let mut engine = StorageEngine::new(path.clone())?;

    engine.set(b"key1", b"value1".to_vec())?;
    assert_eq!(
        engine.get(b"key1").map(|a| a.as_ref().to_vec()),
        Some(b"value1".to_vec())
    );

    // Overwrite key1
    engine.set(b"key1", b"value_new".to_vec())?;
    assert_eq!(
        engine.get(b"key1").map(|a| a.as_ref().to_vec()),
        Some(b"value_new".to_vec())
    );

    fs::remove_file(path)?;
    Ok(())
}

#[test]
fn test_delete_non_existent_key() -> Result<()> {
    let path = temp_db_path("del_non_existent");
    if path.exists() {
        fs::remove_file(&path)?;
    }
    let mut engine = StorageEngine::new(path.clone())?;

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
    let engine = StorageEngine::new(path.clone())?;

    let scan_result = engine
        .scan(b"a".to_vec()..b"z".to_vec())?
        .collect::<Vec<_>>();
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
    let mut engine = StorageEngine::new(path.clone())?;

    engine.set(b"key1", b"value1".to_vec())?;
    engine.set(b"key2", b"value2".to_vec())?;

    // Scan a range that won't match any existing keys
    let scan_result = engine
        .scan(b"x".to_vec()..b"z".to_vec())?
        .collect::<Vec<_>>();
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
    let mut engine = StorageEngine::new(path.clone())?;

    let keys = [
        b"key with spaces".to_vec(),
        b"key/with/slashes".to_vec(),
        b"key\\with\\backslashes".to_vec(),
        b"key\"with\'quotes".to_vec(),
        b"key\twith\nnewlines".to_vec(),
        vec![0, 1, 2, 3, 4, 5], // Non-UTF8 key
    ];
    let values = [
        b"value with spaces".to_vec(),
        b"value/with/slashes".to_vec(),
        b"value\\with\\backslashes".to_vec(),
        b"value\"with\'quotes".to_vec(),
        b"value\twith\nnewlines".to_vec(),
        vec![10, 20, 30, 40, 50], // Non-UTF8 value
    ];

    for i in 0..keys.len() {
        engine.set(&keys[i], values[i].clone())?;
        assert_eq!(
            engine.get(&keys[i]).map(|a| a.as_ref().to_vec()),
            Some(values[i].clone())
        );
    }

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
    let mut engine = StorageEngine::new(path.clone()).unwrap();

    engine.set(b"a", b"val_a".to_vec()).unwrap();
    engine.set(b"b", b"val_b".to_vec()).unwrap();
    engine.set(b"c", b"val_c".to_vec()).unwrap();

    // Start key is greater than end key
    // This should panic based on BTreeMap behavior
    let _scan_result = engine
        .scan(b"c".to_vec()..b"a".to_vec())
        .unwrap()
        .collect::<Vec<_>>();

    // The following lines will not be reached if the panic occurs as expected.
    fs::remove_file(path).unwrap();
}

#[test]
fn test_scan_boundary_conditions() -> Result<()> {
    let path = temp_db_path("scan_boundaries");
    if path.exists() {
        fs::remove_file(&path)?;
    }
    let mut engine = StorageEngine::new(path.clone())?;

    engine.set(b"key1", b"value1".to_vec())?;
    engine.set(b"key2", b"value2".to_vec())?;
    engine.set(b"key3", b"value3".to_vec())?;

    // Scan for "key1" (exclusive end for "key2")
    let result1 = engine
        .scan(b"key1".to_vec()..b"key2".to_vec())?
        .collect::<Vec<_>>();
    assert_eq!(result1.len(), 1);
    assert_eq!(result1[0].0, b"key1".to_vec());
    assert_eq!(result1[0].1.as_ref(), b"value1");

    // Scan for "key2" and "key3"
    // To include "key3", the end of the range must be > "key3"
    let mut end_key3_inclusive = b"key3".to_vec();
    end_key3_inclusive.push(0); // Makes it "key3\x00"
    let result2 = engine
        .scan(b"key2".to_vec()..end_key3_inclusive)?
        .collect::<Vec<_>>();
    assert_eq!(result2.len(), 2);
    assert_eq!(result2[0].0, b"key2".to_vec());
    assert_eq!(result2[0].1.as_ref(), b"value2");
    assert_eq!(result2[1].0, b"key3".to_vec());
    assert_eq!(result2[1].1.as_ref(), b"value3");

    // Scan a range that includes nothing (e.g., between "key1" and "key2" but not including them)
    let mut start_after_key1 = b"key1".to_vec();
    start_after_key1.push(255); // e.g., "key1\xff"
    let result3 = engine
        .scan(start_after_key1..b"key2".to_vec())?
        .collect::<Vec<_>>();
    assert!(result3.is_empty());

    // Scan all keys (range from before "key1" to after "key3")
    let mut end_beyond_key3 = b"key3".to_vec();
    end_beyond_key3.push(0);
    let result_all = engine
        .scan(b"key0".to_vec()..end_beyond_key3)?
        .collect::<Vec<_>>();
    assert_eq!(result_all.len(), 3);
    assert_eq!(result_all[0].0, b"key1".to_vec());
    assert_eq!(result_all[1].0, b"key2".to_vec());
    assert_eq!(result_all[2].0, b"key3".to_vec());

    // Scan with a start key that doesn't exist but is before the first key
    let result_before_all = engine
        .scan(b"a".to_vec()..b"key2".to_vec())?
        .collect::<Vec<_>>();
    assert_eq!(result_before_all.len(), 1);
    assert_eq!(result_before_all[0].0, b"key1".to_vec());

    // Scan with an end key that includes the last key
    let result_includes_last = engine
        .scan(b"key3".to_vec()..b"z".to_vec())?
        .collect::<Vec<_>>();
    assert_eq!(result_includes_last.len(), 1);
    assert_eq!(result_includes_last[0].0, b"key3".to_vec());

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
        let mut engine = StorageEngine::new(path.clone())?;
        engine.set(b"iso_key1", b"val1_session1".to_vec())?;
        engine.set(b"iso_key2", b"val2_session1".to_vec())?;
        drop(engine);
    }

    // Session 2: Read data from session 1, modify some, add new
    {
        let mut engine = StorageEngine::new(path.clone())?;
        assert_eq!(
            engine.get(b"iso_key1").map(|a| a.as_ref().to_vec()),
            Some(b"val1_session1".to_vec())
        );
        assert_eq!(
            engine.get(b"iso_key2").map(|a| a.as_ref().to_vec()),
            Some(b"val2_session1".to_vec())
        );

        engine.set(b"iso_key2", b"val2_session2_updated".to_vec())?;
        engine.set(b"iso_key3", b"val3_session2_new".to_vec())?;
        drop(engine);
    }

    // Session 3: Verify changes from session 2 and original from session 1
    {
        let engine = StorageEngine::new(path.clone())?;
        assert_eq!(
            engine.get(b"iso_key1").map(|a| a.as_ref().to_vec()),
            Some(b"val1_session1".to_vec())
        ); // Unchanged from session 1
        assert_eq!(
            engine.get(b"iso_key2").map(|a| a.as_ref().to_vec()),
            Some(b"val2_session2_updated".to_vec())
        ); // Updated in session 2
        assert_eq!(
            engine.get(b"iso_key3").map(|a| a.as_ref().to_vec()),
            Some(b"val3_session2_new".to_vec())
        ); // Added in session 2
        assert_eq!(engine.len(), 3);
        drop(engine);
    }

    fs::remove_file(path)?;
    Ok(())
}

#[test]
fn test_engine_value_size_limit() {
    let path = temp_db_path("value_limit");
    if path.exists() {
        fs::remove_file(&path).unwrap();
    }
    // configure engine to allow only 1-byte values
    let config = EngineConfig {
        max_value_size: 1,
        ..Default::default()
    };
    let mut engine = StorageEngine::with_config(path.clone(), config).unwrap();
    // setting oversized value should error
    let err = engine.set(b"k", vec![0, 1]);
    assert!(
        err.is_err(),
        "Expected engine.set error for oversized value"
    );
    // valid value
    engine.set(b"k", vec![0]).unwrap();
    assert_eq!(engine.get(b"k").map(|a| a.as_ref().to_vec()), Some(vec![0]));
    fs::remove_file(path).unwrap();
}
