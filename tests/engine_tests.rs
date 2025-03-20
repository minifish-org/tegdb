use std::sync::Arc;
use std::path::PathBuf;
use std::fs;
use tegdb::Engine;

#[tokio::test]
async fn test_engine() {
    let path = PathBuf::from("test.db");
    let engine = Engine::new(path.clone());
    let key = b"key";
    let value = b"value";
    engine.set(key, value.to_vec()).await.unwrap();
    let get_value = engine.get(key).await.unwrap();
    assert_eq!(
        get_value,
        value,
        "Expected: {}, Got: {}",
        String::from_utf8_lossy(value),
        String::from_utf8_lossy(&get_value)
    );
    engine.del(key).await.unwrap();
    let get_value = engine.get(key).await;
    assert_eq!(
        get_value,
        None,
        "Expected: None, Got: Some value"
    );
    let start_key = b"a";
    let end_key = b"z";
    engine.set(start_key, b"start_value".to_vec()).await.unwrap();
    engine.set(end_key, b"end_value".to_vec()).await.unwrap();
    let mut end_key_extended = Vec::new();
    end_key_extended.extend_from_slice(end_key);
    end_key_extended.extend_from_slice(&[1u8]);
    let result = engine.scan(start_key.to_vec()..end_key_extended)
        .await
        .unwrap()
        .collect::<Vec<_>>();
    let expected = vec![
        (start_key.to_vec(), b"start_value".to_vec()),
        (end_key.to_vec(), b"end_value".to_vec()),
    ];
    let expected_strings: Vec<(String, String)> = expected.iter().map(|(k, v)| {
        (String::from_utf8_lossy(k).into_owned(), String::from_utf8_lossy(v).into_owned())
    }).collect();
    let result_strings: Vec<(String, String)> = result.iter().map(|(k, v)| {
        (String::from_utf8_lossy(k).into_owned(), String::from_utf8_lossy(v).into_owned())
    }).collect();
    assert_eq!(
        result_strings, expected_strings,
        "Expected: {:?}, Got: {:?}",
        expected_strings, result_strings
    );
    drop(engine);
    fs::remove_dir_all(path).unwrap();
}

#[tokio::test]
async fn test_concurrent_access() {
    use tokio::sync::Mutex;
    let path = PathBuf::from("concurrent.db");
    let engine = Arc::new(Mutex::new(Engine::new(path.clone())));
    let tasks: Vec<_> = (0..10)
        .map(|i| {
            let engine = engine.clone();
            tokio::spawn(async move {
                let key = format!("key_{}", i).into_bytes();
                let value = format!("value_{}", i).into_bytes();
                engine.lock().await.set(&key, value.clone()).await.unwrap();
                let got = engine.lock().await.get(&key).await.unwrap();
                assert_eq!(got, value);
            })
        })
        .collect();
    for t in tasks {
        t.await.unwrap();
    }
    drop(engine);
    fs::remove_dir_all(path).unwrap();
}

#[tokio::test]
async fn test_engine_durability() {
    let path = PathBuf::from("test_engine_durability.db");
    
    // Clean up any existing test directory
    fs::remove_dir_all(&path).ok();
    
    // First phase: Write some data and close the engine
    {
        let engine = Engine::new(path.clone());
        
        // Insert multiple test keys
        let test_data = vec![
            (b"key1".as_ref(), b"value1".as_ref()),
            (b"key2".as_ref(), b"value2".as_ref()),
            (b"key3".as_ref(), b"value3".as_ref()),
        ];
        
        for (key, value) in &test_data {
            engine.set(key, value.to_vec()).await.unwrap();
            // Verify data is immediately available
            let get_value = engine.get(key).await.unwrap();
            assert_eq!(get_value, value.to_vec(), 
                "Expected: {}, Got: {}", 
                String::from_utf8_lossy(value),
                String::from_utf8_lossy(&get_value));
        }
        
        // Force flush before dropping
        engine.flush().unwrap();
        // Drop the engine to ensure proper shutdown
        drop(engine);
        // Manually remove the lock file
        fs::remove_file(path.join("lock.lock")).ok();
    }
    
    // Second phase: Reopen engine and verify data persists
    {
        let engine = Engine::new(path.clone());
        
        // Verify all data is still there
        let test_data = vec![
            (b"key1".as_ref(), b"value1".as_ref()),
            (b"key2".as_ref(), b"value2".as_ref()),
            (b"key3".as_ref(), b"value3".as_ref()),
        ];
        
        for (key, value) in &test_data {
            let get_value = engine.get(key).await.unwrap();
            assert_eq!(get_value, value.to_vec(),
                "Data not found for key: {}, Expected: {}, Got: {}", 
                String::from_utf8_lossy(key),
                String::from_utf8_lossy(value),
                String::from_utf8_lossy(&get_value));
        }
        
        // Force flush before dropping
        engine.flush().unwrap();
        // Drop the engine to ensure proper shutdown
        drop(engine);
        // Manually remove the lock file
        fs::remove_file(path.join("lock.lock")).ok();
    }
    
    // Cleanup
    fs::remove_dir_all(&path).unwrap();
}

#[tokio::test]
async fn test_engine_compaction() {
    let path = PathBuf::from("test_engine_compaction.db");
    
    // Clean up any existing test directory
    fs::remove_dir_all(&path).ok();
    
    // First phase: Write some data and perform compaction
    {
        let mut engine = Engine::new(path.clone());
        
        // Insert multiple test keys
        let test_data = vec![
            (b"key1".as_ref(), b"value1".as_ref()),
            (b"key2".as_ref(), b"value2".as_ref()),
            (b"key3".as_ref(), b"value3".as_ref()),
        ];
        
        // Write initial data
        for (key, value) in &test_data {
            engine.set(key, value.to_vec()).await.unwrap();
            // Verify data is immediately available
            let get_value = engine.get(key).await.unwrap();
            assert_eq!(get_value, value.to_vec(), 
                "Expected: {}, Got: {}", 
                String::from_utf8_lossy(value),
                String::from_utf8_lossy(&get_value));
        }
        
        // Force flush before compaction
        engine.flush().unwrap();
        
        // Perform compaction
        engine.compact().unwrap();
        
        // Verify data is still accessible after compaction
        for (key, value) in &test_data {
            let get_value = engine.get(key).await.unwrap();
            assert_eq!(get_value, value.to_vec(),
                "Data not found after compaction for key: {}, Expected: {}, Got: {}", 
                String::from_utf8_lossy(key),
                String::from_utf8_lossy(value),
                String::from_utf8_lossy(&get_value));
        }
        
        // Force flush before dropping
        engine.flush().unwrap();
        // Drop the engine to ensure proper shutdown
        drop(engine);
        // Manually remove the lock file
        fs::remove_file(path.join("lock.lock")).ok();
    }
    
    // Second phase: Reopen engine and verify data persists after compaction
    {
        let engine = Engine::new(path.clone());
        
        // Verify all data is still there after restart
        let test_data = vec![
            (b"key1".as_ref(), b"value1".as_ref()),
            (b"key2".as_ref(), b"value2".as_ref()),
            (b"key3".as_ref(), b"value3".as_ref()),
        ];
        
        for (key, value) in &test_data {
            let get_value = engine.get(key).await.unwrap();
            assert_eq!(get_value, value.to_vec(),
                "Data not found after restart for key: {}, Expected: {}, Got: {}", 
                String::from_utf8_lossy(key),
                String::from_utf8_lossy(value),
                String::from_utf8_lossy(&get_value));
        }
        
        // Force flush before dropping
        engine.flush().unwrap();
        // Drop the engine to ensure proper shutdown
        drop(engine);
        // Manually remove the lock file
        fs::remove_file(path.join("lock.lock")).ok();
    }
    
    // Cleanup
    fs::remove_dir_all(&path).unwrap();
}
