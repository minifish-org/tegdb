use std::fs;
use tegdb::log::{LENGTH_FIELD_BYTES, STORAGE_HEADER_SIZE};
use tegdb::{EngineConfig, Error, StorageEngine};
use tempfile::TempDir;

#[test]
fn test_disk_preallocation_basic() {
    let temp_dir = TempDir::new().unwrap();
    let db_path = temp_dir.path().join("test.teg");

    // Create engine with disk preallocation (10MB)
    let preallocate_size = 10 * 1024 * 1024; // 10MB
    let config = EngineConfig {
        preallocate_size: Some(preallocate_size),
        ..Default::default()
    };

    let mut engine = StorageEngine::with_config(db_path.clone(), config).unwrap();

    // Check file size is preallocated
    let metadata = fs::metadata(&db_path).unwrap();
    assert_eq!(metadata.len(), preallocate_size);

    // Insert some data
    for i in 0..10 {
        let key = format!("key_{}", i);
        let value = format!("value_{}", i);
        engine.set(key.as_bytes(), value.into_bytes()).unwrap();
    }

    // File size should still be preallocated size
    let metadata = fs::metadata(&db_path).unwrap();
    assert_eq!(metadata.len(), preallocate_size);

    // Verify data can be retrieved
    for i in 0..10 {
        let key = format!("key_{}", i);
        let value = engine.get(key.as_bytes()).unwrap();
        assert_eq!(value.as_ref(), format!("value_{}", i).as_bytes());
    }

    drop(engine);

    // Re-open database and verify data persists
    let config = EngineConfig::default();
    let engine = StorageEngine::with_config(db_path, config).unwrap();

    for i in 0..10 {
        let key = format!("key_{}", i);
        let value = engine.get(key.as_bytes()).unwrap();
        assert_eq!(value.as_ref(), format!("value_{}", i).as_bytes());
    }
}

#[test]
fn test_disk_preallocation_none() {
    let temp_dir = TempDir::new().unwrap();
    let db_path = temp_dir.path().join("test.teg");

    // Create engine without disk preallocation
    let config = EngineConfig {
        preallocate_size: None,
        ..Default::default()
    };

    let mut engine = StorageEngine::with_config(db_path.clone(), config).unwrap();

    // File size should be just the header
    let metadata = fs::metadata(&db_path).unwrap();
    assert_eq!(metadata.len(), 64); // STORAGE_HEADER_SIZE

    // Insert some data
    for i in 0..10 {
        let key = format!("key_{}", i);
        let value = format!("value_{}", i);
        engine.set(key.as_bytes(), value.into_bytes()).unwrap();
    }

    // File size should have grown
    let metadata = fs::metadata(&db_path).unwrap();
    assert!(metadata.len() > 64);
}

#[test]
fn test_disk_preallocation_small() {
    let temp_dir = TempDir::new().unwrap();
    let db_path = temp_dir.path().join("test.teg");

    // Preallocate smaller than header (should be ignored)
    let config = EngineConfig {
        preallocate_size: Some(32), // Smaller than STORAGE_HEADER_SIZE
        ..Default::default()
    };

    let result = StorageEngine::with_config(db_path.clone(), config);
    assert!(result.is_err());
    
    // Verify error message
    match result {
        Err(Error::Other(msg)) => assert!(msg.contains("must be at least 64 bytes")),
        _ => panic!("Expected Error::Other"),
    }
}

#[test]
fn test_valid_data_end_tracking() {
    let temp_dir = TempDir::new().unwrap();
    let db_path = temp_dir.path().join("test.teg");

    // Create engine with disk preallocation
    let preallocate_size = 1024 * 1024; // 1MB
    let config = EngineConfig {
        preallocate_size: Some(preallocate_size),
        ..Default::default()
    };

    let mut engine = StorageEngine::with_config(db_path.clone(), config).unwrap();

    // Insert data
    engine.set(b"test_key", b"test_value".to_vec()).unwrap();
    engine.flush().unwrap();
    drop(engine);

    // Verify header contains valid_data_end
    let file = fs::File::open(&db_path).unwrap();
    let metadata = file.metadata().unwrap();

    // File should still be preallocated size
    assert_eq!(metadata.len(), preallocate_size);

    // Re-open and verify data is intact
    let engine = StorageEngine::new(db_path).unwrap();
    let value = engine.get(b"test_key").unwrap();
    assert_eq!(value.as_ref(), b"test_value");
}

#[test]
fn test_disk_preallocation_limit_enforced() {
    let temp_dir = TempDir::new().unwrap();
    let db_path = temp_dir.path().join("disk_limit.teg");

    let key = b"k1";
    let value = vec![b'x'; 32];
    let entry_size = (2 * LENGTH_FIELD_BYTES + key.len() + value.len()) as u64;
    let limit = STORAGE_HEADER_SIZE as u64 + entry_size;

    let config = EngineConfig {
        preallocate_size: Some(limit),
        initial_capacity: Some(10),
        ..Default::default()
    };

    let mut engine = StorageEngine::with_config(db_path.clone(), config).unwrap();
    engine.set(key, value.clone()).unwrap();

    let err = engine.set(b"k2", value.clone()).unwrap_err();
    match err {
        Error::OutOfStorageQuota { bytes } => assert_eq!(bytes, limit),
        other => panic!("expected OutOfStorageQuota error, got {other:?}"),
    }

    // Updates to existing keys remain allowed.
    engine.set(key, value.clone()).unwrap();
    assert_eq!(std::fs::metadata(&db_path).unwrap().len(), limit);
}
