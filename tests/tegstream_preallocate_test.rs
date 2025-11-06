use std::fs::File;
use tegdb::tegstream::parser::{find_last_commit_offset, RecordParser};
use tegdb::{EngineConfig, StorageEngine};
use tempfile::TempDir;

#[test]
fn test_parser_with_preallocated_file() {
    let temp_dir = TempDir::new().unwrap();
    let db_path = temp_dir.path().join("test.teg");

    // Create engine with disk preallocation
    let preallocate_size = 10 * 1024 * 1024; // 10MB
    let config = EngineConfig {
        preallocate_size: Some(preallocate_size),
        ..Default::default()
    };

    let mut engine = StorageEngine::with_config(db_path.clone(), config).unwrap();

    // Insert some data and commit
    engine.set(b"key1", b"value1".to_vec()).unwrap();
    engine.set(b"key2", b"value2".to_vec()).unwrap();

    // Simulate transaction commit by using the transaction API
    {
        let mut tx = engine.begin_transaction();
        tx.set(b"key3", b"value3".to_vec()).unwrap();
        tx.commit().unwrap();
    }

    engine.flush().unwrap();
    drop(engine);

    // Use parser to find last commit offset
    let mut file = File::open(&db_path).unwrap();
    let last_commit = find_last_commit_offset(&mut file).unwrap();

    // Verify commit offset is reasonable (should be much less than preallocated size)
    assert!(last_commit > 64, "Commit offset should be after header");
    assert!(
        last_commit < 10000,
        "Commit offset should be much smaller than preallocated size"
    );

    // File size should still be preallocated
    let metadata = file.metadata().unwrap();
    assert_eq!(metadata.len(), preallocate_size);
}

#[test]
fn test_parser_read_valid_data_end() {
    let temp_dir = TempDir::new().unwrap();
    let db_path = temp_dir.path().join("test.teg");

    // Create preallocated database
    let preallocate_size = 5 * 1024 * 1024; // 5MB
    let config = EngineConfig {
        preallocate_size: Some(preallocate_size),
        ..Default::default()
    };

    let mut engine = StorageEngine::with_config(db_path.clone(), config).unwrap();
    engine.set(b"test", b"data".to_vec()).unwrap();
    engine.flush().unwrap();
    drop(engine);

    // Read valid_data_end using parser
    let mut file = File::open(&db_path).unwrap();
    let valid_data_end = RecordParser::read_valid_data_end(&mut file).unwrap();

    // Should be greater than header but much less than preallocated size
    assert!(
        valid_data_end >= 64,
        "valid_data_end should be at least header size"
    );
    assert!(
        valid_data_end < 1024,
        "valid_data_end should be small for minimal data"
    );

    // File size should be preallocated
    let file_size = file.metadata().unwrap().len();
    assert_eq!(file_size, preallocate_size);
    assert!(
        valid_data_end < file_size,
        "valid_data_end should be less than file size"
    );
}

#[test]
fn test_parser_version_1_rejected() {
    let temp_dir = TempDir::new().unwrap();
    let db_path = temp_dir.path().join("test_v1.teg");

    // Create a version 1 file manually
    use std::fs::OpenOptions;
    use std::io::Write;

    let mut file = OpenOptions::new()
        .create(true)
        .truncate(true)
        .write(true)
        .read(true)
        .open(&db_path)
        .unwrap();

    let mut header = vec![0u8; 64];
    header[0..6].copy_from_slice(b"TEGDB\0");
    header[6..8].copy_from_slice(&1u16.to_be_bytes()); // Version 1
    header[8..12].copy_from_slice(&0u32.to_be_bytes());
    header[12..16].copy_from_slice(&1024u32.to_be_bytes());
    header[16..20].copy_from_slice(&(256 * 1024u32).to_be_bytes());
    header[20] = 1u8;
    file.write_all(&header).unwrap();

    // Write some data
    let key = b"old_key";
    let value = b"old_value";
    file.write_all(&(key.len() as u32).to_be_bytes()).unwrap();
    file.write_all(&(value.len() as u32).to_be_bytes()).unwrap();
    file.write_all(key).unwrap();
    file.write_all(value).unwrap();
    file.sync_all().unwrap();

    drop(file);

    // Read valid_data_end (should now fail for unsupported version)
    let mut file = File::open(&db_path).unwrap();
    let result = RecordParser::read_valid_data_end(&mut file);
    assert!(
        result.is_err(),
        "Version 1 files should be rejected by the parser"
    );
}

#[test]
fn test_parser_skips_preallocated_space() {
    let temp_dir = TempDir::new().unwrap();
    let db_path = temp_dir.path().join("test.teg");

    // Create with large preallocation
    let preallocate_size = 20 * 1024 * 1024; // 20MB
    let config = EngineConfig {
        preallocate_size: Some(preallocate_size),
        ..Default::default()
    };

    let mut engine = StorageEngine::with_config(db_path.clone(), config).unwrap();

    // Insert minimal data
    engine.set(b"tiny", b"data".to_vec()).unwrap();

    // Commit transaction
    {
        let mut tx = engine.begin_transaction();
        tx.set(b"txkey", b"txval".to_vec()).unwrap();
        tx.commit().unwrap();
    }

    engine.flush().unwrap();
    drop(engine);

    // Find last commit - should not scan the entire preallocated file
    let mut file = File::open(&db_path).unwrap();
    let start = std::time::Instant::now();
    let last_commit = find_last_commit_offset(&mut file).unwrap();
    let elapsed = start.elapsed();

    // Should complete very quickly (< 10ms) because it only scans valid data
    assert!(
        elapsed.as_millis() < 100,
        "Parsing should be fast, only scanning valid data"
    );

    // Verify commit offset is reasonable
    assert!(last_commit >= 64);
    assert!(last_commit < 10000, "Only scanned valid data, not 20MB");
}
