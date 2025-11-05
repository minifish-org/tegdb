use tegdb::{EngineConfig, StorageEngine};
use tempfile::TempDir;

#[test]
fn test_memory_preallocation_basic() {
    let temp_dir = TempDir::new().unwrap();
    let db_path = temp_dir.path().join("test.teg");

    // Create engine with memory preallocation
    let config = EngineConfig {
        initial_capacity: Some(1000),
        ..Default::default()
    };

    let mut engine = StorageEngine::with_config(db_path, config).unwrap();

    // Insert some data
    for i in 0..100 {
        let key = format!("key_{}", i);
        let value = format!("value_{}", i);
        engine.set(key.as_bytes(), value.into_bytes()).unwrap();
    }

    // Verify data can be retrieved
    for i in 0..100 {
        let key = format!("key_{}", i);
        let value = engine.get(key.as_bytes()).unwrap();
        assert_eq!(value.as_ref(), format!("value_{}", i).as_bytes());
    }

    assert_eq!(engine.len(), 100);
}

#[test]
fn test_memory_preallocation_none() {
    let temp_dir = TempDir::new().unwrap();
    let db_path = temp_dir.path().join("test.teg");

    // Create engine without memory preallocation
    let config = EngineConfig {
        initial_capacity: None,
        ..Default::default()
    };

    let mut engine = StorageEngine::with_config(db_path, config).unwrap();

    // Insert some data
    for i in 0..100 {
        let key = format!("key_{}", i);
        let value = format!("value_{}", i);
        engine.set(key.as_bytes(), value.into_bytes()).unwrap();
    }

    // Verify data can be retrieved
    for i in 0..100 {
        let key = format!("key_{}", i);
        let value = engine.get(key.as_bytes()).unwrap();
        assert_eq!(value.as_ref(), format!("value_{}", i).as_bytes());
    }

    assert_eq!(engine.len(), 100);
}

#[test]
fn test_memory_preallocation_large_capacity() {
    let temp_dir = TempDir::new().unwrap();
    let db_path = temp_dir.path().join("test.teg");

    // Create engine with large memory preallocation
    let config = EngineConfig {
        initial_capacity: Some(10000),
        ..Default::default()
    };

    let engine = StorageEngine::with_config(db_path, config).unwrap();

    // Just verify it can be created and is empty
    assert_eq!(engine.len(), 0);
}
