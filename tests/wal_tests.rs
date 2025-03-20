use std::path::PathBuf;
use std::fs;
use std::io::Write;
use tegdb::wal::Wal;

#[test]
fn test_wal_basic_operations() {
    let path = PathBuf::from("test_wal.db");
    fs::remove_dir_all(&path).ok();
    
    // Create a new WAL
    let wal = Wal::new(path.clone());
    
    // Test data
    let test_data = vec![
        (b"key1".to_vec(), b"value1".to_vec()),
        (b"key2".to_vec(), b"value2".to_vec()),
        (b"key3".to_vec(), b"value3".to_vec()),
    ];
    
    // Write test data
    for (key, value) in &test_data {
        wal.write_entry(key, value);
    }
    
    // Force flush to ensure data is written
    wal.writer.flush();
    
    // Build key map and verify counts
    let (key_map, (insert_count, remove_count)) = wal.build_key_map();
    assert_eq!(insert_count, 3, "Expected 3 inserts, got {}", insert_count);
    assert_eq!(remove_count, 0, "Expected 0 removals, got {}", remove_count);
    
    // Verify data in key map
    for (key, value) in &test_data {
        let entry = key_map.get(key).expect("Key not found in map");
        assert_eq!(entry.value(), value, 
            "Value mismatch for key: {}, Expected: {}, Got: {}", 
            String::from_utf8_lossy(key),
            String::from_utf8_lossy(value),
            String::from_utf8_lossy(entry.value()));
    }
    
    // Cleanup
    fs::remove_dir_all(&path).unwrap();
}

#[test]
fn test_wal_deletions() {
    let path = PathBuf::from("test_wal_deletions.db");
    fs::remove_dir_all(&path).ok();
    
    // Create a new WAL
    let wal = Wal::new(path.clone());
    
    // Write some data
    let key = b"test_key".to_vec();
    let value = b"test_value".to_vec();
    wal.write_entry(&key, &value);
    wal.writer.flush();
    
    // Delete the key by writing an empty value
    wal.write_entry(&key, &[]);
    wal.writer.flush();
    
    // Build key map and verify counts
    let (key_map, (insert_count, remove_count)) = wal.build_key_map();
    assert_eq!(insert_count, 1, "Expected 1 insert, got {}", insert_count);
    assert_eq!(remove_count, 1, "Expected 1 removal, got {}", remove_count);
    
    // Verify key is not in map
    assert!(key_map.get(&key).is_none(), "Key should not exist in map after deletion");
    
    // Cleanup
    fs::remove_dir_all(&path).unwrap();
}

#[test]
fn test_wal_multiple_files() {
    let path = PathBuf::from("test_wal_multiple.db");
    fs::remove_dir_all(&path).ok();
    
    // Create initial WAL and write some data
    {
        let wal = Wal::new(path.clone());
        wal.write_entry(b"key1", b"value1");
        wal.write_entry(b"key2", b"value2");
        wal.writer.flush();
    }
    
    // Create a new WAL (simulating compaction)
    {
        let wal = Wal::new(path.clone());
        wal.write_entry(b"key2", b"value2_new"); // Update existing key
        wal.write_entry(b"key3", b"value3");     // Add new key
        wal.write_entry(b"key1", &[]);           // Delete key1
        wal.writer.flush();
    }
    
    // Build key map from both files
    let wal = Wal::new(path.clone());
    let (key_map, (insert_count, remove_count)) = wal.build_key_map();
    
    // Verify counts
    assert_eq!(insert_count, 3, "Expected 3 inserts, got {}", insert_count);
    assert_eq!(remove_count, 1, "Expected 1 removal, got {}", remove_count);
    
    // Verify final state
    assert!(key_map.get(&b"key1".to_vec()).is_none(), "key1 should be deleted");
    assert_eq!(key_map.get(&b"key2".to_vec()).unwrap().value(), b"value2_new", "key2 should have new value");
    assert_eq!(key_map.get(&b"key3".to_vec()).unwrap().value(), b"value3", "key3 should exist");
    
    // Cleanup
    fs::remove_dir_all(&path).unwrap();
}

#[test]
fn test_wal_large_values() {
    let path = PathBuf::from("test_wal_large.db");
    fs::remove_dir_all(&path).ok();
    
    // Create a new WAL
    let wal = Wal::new(path.clone());
    
    // Create large test data
    let key = b"large_key".to_vec();
    let value = vec![0u8; 1024 * 1024]; // 1MB value
    
    // Write large value
    wal.write_entry(&key, &value);
    wal.writer.flush();
    
    // Build key map and verify
    let (key_map, (insert_count, remove_count)) = wal.build_key_map();
    assert_eq!(insert_count, 1, "Expected 1 insert, got {}", insert_count);
    assert_eq!(remove_count, 0, "Expected 0 removals, got {}", remove_count);
    
    // Verify data
    let entry = key_map.get(&key).expect("Key not found in map");
    assert_eq!(entry.value(), &value, "Value mismatch for large key");
    
    // Cleanup
    fs::remove_dir_all(&path).unwrap();
}

#[test]
fn test_wal_corrupted_file() {
    let path = PathBuf::from("test_wal_corrupted.db");
    fs::remove_dir_all(&path).ok();
    
    // Create a new WAL
    let wal = Wal::new(path.clone());
    
    // Write some valid data
    wal.write_entry(b"key1", b"value1");
    wal.write_entry(b"key2", b"value2");
    wal.writer.flush();
    
    // Manually corrupt the file by appending invalid data
    {
        let mut file = std::fs::OpenOptions::new()
            .append(true)
            .open(wal.path.clone())
            .unwrap();
        file.write_all(&[0xFF; 100]).unwrap();
    }
    
    // Build key map - should still work with valid entries
    let (key_map, (insert_count, remove_count)) = wal.build_key_map();
    assert_eq!(insert_count, 2, "Expected 2 inserts, got {}", insert_count);
    assert_eq!(remove_count, 0, "Expected 0 removals, got {}", remove_count);
    
    // Verify valid data is still accessible
    assert_eq!(key_map.get(&b"key1".to_vec()).unwrap().value(), b"value1", "key1 should still be valid");
    assert_eq!(key_map.get(&b"key2".to_vec()).unwrap().value(), b"value2", "key2 should still be valid");
    
    // Cleanup
    fs::remove_dir_all(&path).unwrap();
} 