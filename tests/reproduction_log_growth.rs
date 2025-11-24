use tegdb::database::Database;

#[test]
fn test_log_growth_indefinitely() {
    let temp_dir = tempfile::tempdir().unwrap();
    let db_path = temp_dir.path().join("test_growth.teg");
    let db_path_str = format!("file://{}", db_path.to_string_lossy());

    let config = tegdb::storage_engine::EngineConfig {
        preallocate_size: None,
        compaction_threshold_bytes: 1024 * 5, // 5 KB threshold
        compaction_ratio: 1.5,
        ..Default::default()
    };
    let mut db = Database::open_with_config(&db_path_str, config).unwrap();

    // Initial write
    db.execute("CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT(32))")
        .unwrap();
    db.execute("INSERT INTO users (id, name) VALUES (1, 'Alice')")
        .unwrap();

    let initial_size = std::fs::metadata(&db_path).unwrap().len();
    println!("Initial size: {}", initial_size);

    // Update the same record many times
    // Each update adds about 50 bytes (key + value + overhead)
    // 2000 updates ~ 100KB, which should trigger compaction (threshold 50KB)
    for i in 0..2000 {
        db.execute(&format!(
            "UPDATE users SET name = 'Alice_{}' WHERE id = 1",
            i
        ))
        .unwrap();
    }

    let final_size = std::fs::metadata(&db_path).unwrap().len();
    println!("Final size: {}", final_size);

    // The size should be much smaller than if it grew indefinitely
    // Without compaction, it would be > 100KB
    // With compaction, it should be close to initial size (maybe slightly larger due to header overheads)
    assert!(
        final_size < initial_size * 10,
        "Log file grew too much, compaction didn't work"
    );

    // Verify data is still correct
    let result = db.query("SELECT name FROM users WHERE id = 1").unwrap();
    assert_eq!(result.len(), 1);
    // The last update was Alice_1999
    let name = result.rows()[0][0].as_text().unwrap();
    assert_eq!(name, "Alice_1999");
}
