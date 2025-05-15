use std::path::PathBuf;
use std::fs;
use tegdb::Engine;

#[tokio::test]
async fn test_engine() {
    let path = PathBuf::from("test.db");
    let mut engine = Engine::new(path.clone());

    // Test set and get
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

    // Test del
    engine.del(key).await.unwrap();
    let get_value = engine.get(key).await;
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
    engine.set(start_key, b"start_value".to_vec()).await.unwrap();
    engine.set(end_key, b"end_value".to_vec()).await.unwrap();
    let mut end_key_extended = Vec::new();
    end_key_extended.extend_from_slice(end_key);
    end_key_extended.extend_from_slice(&[1u8]);
    let result = engine
        .scan(start_key.to_vec()..end_key_extended)
        .await
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
