use std::fs::{File, OpenOptions};
use std::io::{Read, Write};
use tegdb::log::STORAGE_FORMAT_VERSION;
use tegdb::StorageEngine;
use tempfile::TempDir;

/// Test that current storage headers can be read correctly
#[test]
fn test_current_header_version() {
    let temp_dir = TempDir::new().unwrap();
    let db_path = temp_dir.path().join("test.teg");

    // Create a new database (should use the current storage version)
    let mut engine = StorageEngine::new(db_path.clone()).unwrap();
    engine.set(b"key1", b"value1".to_vec()).unwrap();
    engine.flush().unwrap();
    drop(engine);

    // Read header and verify the version matches the constant
    let mut file = File::open(&db_path).unwrap();
    let mut header = vec![0u8; 64];
    file.read_exact(&mut header).unwrap();

    // Check version bytes [6..8)
    let version = u16::from_be_bytes([header[6], header[7]]);
    assert_eq!(
        version, STORAGE_FORMAT_VERSION,
        "New databases should use the current storage version"
    );

    // Check valid_data_end is present [21..29)
    let valid_data_end = u64::from_be_bytes([
        header[21], header[22], header[23], header[24], header[25], header[26], header[27],
        header[28],
    ]);
    assert!(
        valid_data_end >= 64,
        "valid_data_end should be at least header size"
    );
    assert!(
        valid_data_end < 1024,
        "valid_data_end should be reasonable for small test"
    );

    // Re-open and verify data
    let engine = StorageEngine::new(db_path).unwrap();
    let value = engine.get(b"key1").unwrap();
    assert_eq!(value.as_ref(), b"value1");
}

/// Simulate a version 1 file and verify it is rejected
#[test]
fn test_version_1_rejected() {
    let temp_dir = TempDir::new().unwrap();
    let db_path = temp_dir.path().join("test_v1.teg");

    // Manually create a version 1 header
    let mut file = OpenOptions::new()
        .create(true)
        .truncate(true)
        .write(true)
        .read(true)
        .open(&db_path)
        .unwrap();

    let mut header = vec![0u8; 64];
    // magic [0..6)
    header[0..6].copy_from_slice(b"TEGDB\0");
    // version [6..8) = 1
    header[6..8].copy_from_slice(&1u16.to_be_bytes());
    // flags [8..12) = 0
    header[8..12].copy_from_slice(&0u32.to_be_bytes());
    // max_key [12..16)
    header[12..16].copy_from_slice(&1024u32.to_be_bytes());
    // max_val [16..20)
    header[16..20].copy_from_slice(&(256 * 1024u32).to_be_bytes());
    // endianness [20] = 1
    header[20] = 1u8;
    // [21..64) reserved = 0 (no valid_data_end for version 1)

    file.write_all(&header).unwrap();

    // Write a simple entry
    let key = b"test_key";
    let value = b"test_value";
    let key_len = key.len() as u32;
    let value_len = value.len() as u32;

    file.write_all(&key_len.to_be_bytes()).unwrap();
    file.write_all(&value_len.to_be_bytes()).unwrap();
    file.write_all(key).unwrap();
    file.write_all(value).unwrap();
    file.sync_all().unwrap();
    drop(file);

    // Opening should now fail
    let result = StorageEngine::new(db_path);
    assert!(result.is_err(), "Version 1 files should be rejected");
}

/// Test that header magic is validated
#[test]
fn test_invalid_magic_rejected() {
    let temp_dir = TempDir::new().unwrap();
    let db_path = temp_dir.path().join("test_bad.teg");

    // Create file with bad magic
    let mut file = File::create(&db_path).unwrap();
    let mut header = vec![0u8; 64];
    header[0..6].copy_from_slice(b"BADMAG"); // Wrong magic
    header[6..8].copy_from_slice(&2u16.to_be_bytes());
    header[20] = 1u8; // endianness
    file.write_all(&header).unwrap();
    drop(file);

    // Should fail to open
    let result = StorageEngine::new(db_path);
    assert!(result.is_err(), "Should reject invalid magic");
}

/// Test that unsupported version is rejected
#[test]
fn test_unsupported_version_rejected() {
    let temp_dir = TempDir::new().unwrap();
    let db_path = temp_dir.path().join("test_v99.teg");

    // Create file with future version
    let mut file = File::create(&db_path).unwrap();
    let mut header = vec![0u8; 64];
    header[0..6].copy_from_slice(b"TEGDB\0");
    header[6..8].copy_from_slice(&99u16.to_be_bytes()); // Future version
    header[20] = 1u8; // endianness
    file.write_all(&header).unwrap();
    drop(file);

    // Should fail to open
    let result = StorageEngine::new(db_path);
    assert!(result.is_err(), "Should reject unsupported version");
}

/// Test valid_data_end is updated correctly
#[test]
fn test_valid_data_end_updates() {
    let temp_dir = TempDir::new().unwrap();
    let db_path = temp_dir.path().join("test.teg");

    // Create engine and insert data
    let engine = StorageEngine::new(db_path.clone()).unwrap();

    // Initial valid_data_end (should be header size)
    drop(engine);
    let mut file = File::open(&db_path).unwrap();
    let mut header = vec![0u8; 64];
    file.read_exact(&mut header).unwrap();
    let initial_end = u64::from_be_bytes([
        header[21], header[22], header[23], header[24], header[25], header[26], header[27],
        header[28],
    ]);
    assert_eq!(initial_end, 64);
    drop(file);

    // Insert some data
    let mut engine = StorageEngine::new(db_path.clone()).unwrap();
    engine.set(b"key1", b"value1".to_vec()).unwrap();
    engine.flush().unwrap();
    drop(engine);

    // valid_data_end should have increased
    let mut file = File::open(&db_path).unwrap();
    let mut header = vec![0u8; 64];
    file.read_exact(&mut header).unwrap();
    let after_insert_end = u64::from_be_bytes([
        header[21], header[22], header[23], header[24], header[25], header[26], header[27],
        header[28],
    ]);
    assert!(
        after_insert_end > initial_end,
        "valid_data_end should increase after insert"
    );
}
