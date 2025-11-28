//! Comprehensive tests for tgstream (cloud sync) functionality
//!
//! Note: Many tests require MinIO/S3 setup and environment variables:
//! - TGSTREAM_IT=1
//! - AWS_ACCESS_KEY_ID
//! - AWS_SECRET_ACCESS_KEY
//! - AWS_ENDPOINT_URL
//! - TGSTREAM_BUCKET (optional, defaults to "tgstream-test")

use std::env;
use std::fs::OpenOptions;
use std::io::{Seek, SeekFrom, Write};

use tegdb::log::{STORAGE_FORMAT_VERSION, STORAGE_HEADER_SIZE, STORAGE_MAGIC, TX_COMMIT_MARKER};
use tegdb::tgstream::config::{Config, S3Config};
use tegdb::tgstream::parser::find_last_commit_offset;
use tegdb::tgstream::restore::Restore;
use tegdb::tgstream::tailer::Tailer;

fn write_header(file: &mut std::fs::File) {
    let mut header = vec![0u8; STORAGE_HEADER_SIZE];
    header[0..STORAGE_MAGIC.len()].copy_from_slice(STORAGE_MAGIC);
    header[6..8].copy_from_slice(&STORAGE_FORMAT_VERSION.to_be_bytes());
    header[8..12].copy_from_slice(&0u32.to_be_bytes());
    header[12..16].copy_from_slice(&(1024u32).to_be_bytes());
    header[16..20].copy_from_slice(&(256 * 1024u32).to_be_bytes());
    header[20] = 1u8;
    file.write_all(&header).unwrap();
}

fn write_record(file: &mut std::fs::File, key: &[u8], value: &[u8]) {
    let key_len = (key.len() as u32).to_be_bytes();
    let val_len = (value.len() as u32).to_be_bytes();
    file.write_all(&key_len).unwrap();
    file.write_all(&val_len).unwrap();
    file.write_all(key).unwrap();
    file.write_all(value).unwrap();
}

fn get_test_config(db_path: std::path::PathBuf) -> Option<Config> {
    if env::var("TGSTREAM_IT").unwrap_or_default() != "1" {
        return None;
    }
    if env::var("AWS_ACCESS_KEY_ID").is_err() {
        return None;
    }
    if env::var("AWS_SECRET_ACCESS_KEY").is_err() {
        return None;
    }
    if env::var("AWS_ENDPOINT_URL").is_err() {
        return None;
    }

    let bucket = env::var("TGSTREAM_BUCKET").unwrap_or_else(|_| "tgstream-test".to_string());
    let prefix = format!("tgstream/{:?}", std::process::id());
    let region = env::var("AWS_REGION").unwrap_or_else(|_| "us-east-1".to_string());

    Some(Config {
        database_path: db_path,
        s3: S3Config {
            bucket,
            prefix,
            region,
            endpoint: env::var("AWS_ENDPOINT_URL").ok(),
            access_key_id: env::var("AWS_ACCESS_KEY_ID").ok(),
            secret_access_key: env::var("AWS_SECRET_ACCESS_KEY").ok(),
        },
        retention: Default::default(),
        base: Default::default(),
        segment: Default::default(),
        gzip: true,
    })
}

#[tokio::test]
async fn test_backup_creation_and_verification() {
    let config = match get_test_config(tempfile::tempdir().unwrap().path().join("backup_test.teg"))
    {
        Some(cfg) => cfg,
        None => return,
    };

    let dir = tempfile::tempdir().unwrap();
    let db_path = dir.path().join("backup_test.teg");
    let mut f = OpenOptions::new()
        .create(true)
        .truncate(true)
        .read(true)
        .write(true)
        .open(&db_path)
        .unwrap();

    write_header(&mut f);
    write_record(&mut f, b"test_key", b"test_value");
    write_record(&mut f, TX_COMMIT_MARKER, b"");
    f.flush().unwrap();

    // Create backup
    let mut tailer = Tailer::new(config.clone()).await.unwrap();
    tailer.snapshot_once().await.unwrap();

    // Verify backup was created by checking restore works
    let out_path = dir.path().join("restored_backup.teg");
    let rest = Restore::new(&config.s3, &config.s3_prefix()).await.unwrap();
    rest.restore_to(&out_path, None).await.unwrap();

    // Verify restored database
    let mut rf = OpenOptions::new().read(true).open(&out_path).unwrap();
    let off_restored = find_last_commit_offset(&mut rf).unwrap();
    assert!(off_restored > STORAGE_HEADER_SIZE as u64);
}

#[tokio::test]
async fn test_restore_from_backup() {
    let config = match get_test_config(tempfile::tempdir().unwrap().path().join("restore_test.teg"))
    {
        Some(cfg) => cfg,
        None => return,
    };

    let dir = tempfile::tempdir().unwrap();
    let db_path = dir.path().join("restore_test.teg");
    let mut f = OpenOptions::new()
        .create(true)
        .truncate(true)
        .read(true)
        .write(true)
        .open(&db_path)
        .unwrap();

    write_header(&mut f);
    write_record(&mut f, b"key1", b"value1");
    write_record(&mut f, b"key2", b"value2");
    write_record(&mut f, TX_COMMIT_MARKER, b"");
    f.flush().unwrap();

    // Create backup
    let mut tailer = Tailer::new(config.clone()).await.unwrap();
    tailer.snapshot_once().await.unwrap();

    // Restore
    let out_path = dir.path().join("restored.teg");
    let rest = Restore::new(&config.s3, &config.s3_prefix()).await.unwrap();
    rest.restore_to(&out_path, None).await.unwrap();

    // Verify restore succeeded
    let mut rf = OpenOptions::new().read(true).open(&out_path).unwrap();
    let off_restored = find_last_commit_offset(&mut rf).unwrap();
    let mut of = OpenOptions::new().read(true).open(&db_path).unwrap();
    let off_src = find_last_commit_offset(&mut of).unwrap();
    assert!(off_restored >= off_src);
}

#[tokio::test]
async fn test_incremental_backups() {
    let config = match get_test_config(
        tempfile::tempdir()
            .unwrap()
            .path()
            .join("incremental_test.teg"),
    ) {
        Some(cfg) => cfg,
        None => return,
    };

    let dir = tempfile::tempdir().unwrap();
    let db_path = dir.path().join("incremental_test.teg");
    let mut f = OpenOptions::new()
        .create(true)
        .truncate(true)
        .read(true)
        .write(true)
        .open(&db_path)
        .unwrap();

    write_header(&mut f);
    write_record(&mut f, b"initial", b"data");
    write_record(&mut f, TX_COMMIT_MARKER, b"");
    f.flush().unwrap();

    // First backup
    let mut tailer = Tailer::new(config.clone()).await.unwrap();
    tailer.snapshot_once().await.unwrap();

    // Add more data
    f.seek(SeekFrom::End(0)).unwrap();
    write_record(&mut f, b"incremental", b"data");
    write_record(&mut f, TX_COMMIT_MARKER, b"");
    f.flush().unwrap();

    // Second backup (incremental)
    tailer.snapshot_once().await.unwrap();

    // Restore should include both commits
    let out_path = dir.path().join("restored_incremental.teg");
    let rest = Restore::new(&config.s3, &config.s3_prefix()).await.unwrap();
    rest.restore_to(&out_path, None).await.unwrap();

    // Verify both commits are in restored database
    let mut rf = OpenOptions::new().read(true).open(&out_path).unwrap();
    let off_restored = find_last_commit_offset(&mut rf).unwrap();
    let mut of = OpenOptions::new().read(true).open(&db_path).unwrap();
    let off_src = find_last_commit_offset(&mut of).unwrap();
    assert!(off_restored >= off_src);
}

#[tokio::test]
async fn test_backup_with_transactions() {
    let config = match get_test_config(
        tempfile::tempdir()
            .unwrap()
            .path()
            .join("tx_backup_test.teg"),
    ) {
        Some(cfg) => cfg,
        None => return,
    };

    let dir = tempfile::tempdir().unwrap();
    let db_path = dir.path().join("tx_backup_test.teg");
    let mut f = OpenOptions::new()
        .create(true)
        .truncate(true)
        .read(true)
        .write(true)
        .open(&db_path)
        .unwrap();

    write_header(&mut f);

    // Multiple transactions
    for i in 1..=5 {
        write_record(
            &mut f,
            format!("key{}", i).as_bytes(),
            format!("value{}", i).as_bytes(),
        );
        write_record(&mut f, TX_COMMIT_MARKER, b"");
    }
    f.flush().unwrap();

    // Backup
    let mut tailer = Tailer::new(config.clone()).await.unwrap();
    tailer.snapshot_once().await.unwrap();

    // Restore
    let out_path = dir.path().join("restored_tx.teg");
    let rest = Restore::new(&config.s3, &config.s3_prefix()).await.unwrap();
    rest.restore_to(&out_path, None).await.unwrap();

    // Verify all transactions are preserved
    let mut rf = OpenOptions::new().read(true).open(&out_path).unwrap();
    let off_restored = find_last_commit_offset(&mut rf).unwrap();
    assert!(off_restored > STORAGE_HEADER_SIZE as u64);
}

#[tokio::test]
async fn test_multiple_backup_restore_cycles() {
    let config = match get_test_config(tempfile::tempdir().unwrap().path().join("cycles_test.teg"))
    {
        Some(cfg) => cfg,
        None => return,
    };

    let dir = tempfile::tempdir().unwrap();
    let db_path = dir.path().join("cycles_test.teg");
    let mut f = OpenOptions::new()
        .create(true)
        .truncate(true)
        .read(true)
        .write(true)
        .open(&db_path)
        .unwrap();

    write_header(&mut f);
    let mut tailer = Tailer::new(config.clone()).await.unwrap();

    // Multiple backup/restore cycles
    for cycle in 1..=3 {
        // Add data
        f.seek(SeekFrom::End(0)).unwrap();
        write_record(
            &mut f,
            format!("cycle{}", cycle).as_bytes(),
            format!("data{}", cycle).as_bytes(),
        );
        write_record(&mut f, TX_COMMIT_MARKER, b"");
        f.flush().unwrap();

        // Backup
        tailer.snapshot_once().await.unwrap();

        // Restore
        let out_path = dir.path().join(format!("restored_cycle{}.teg", cycle));
        let rest = Restore::new(&config.s3, &config.s3_prefix()).await.unwrap();
        rest.restore_to(&out_path, None).await.unwrap();

        // Verify
        let mut rf = OpenOptions::new().read(true).open(&out_path).unwrap();
        let off_restored = find_last_commit_offset(&mut rf).unwrap();
        assert!(off_restored > STORAGE_HEADER_SIZE as u64);
    }
}
