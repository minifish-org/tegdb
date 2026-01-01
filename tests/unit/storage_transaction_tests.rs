use std::{env, fs, path::PathBuf};
use tegdb::storage_engine::{EngineConfig, StorageEngine};
use tegdb::Result;

/// Creates a unique temporary file path for tests
fn temp_db_path(prefix: &str) -> PathBuf {
    let mut path = env::temp_dir();
    path.push(format!("tegdb_test_{}_{}", prefix, std::process::id()));
    path.with_extension("teg")
}

#[test]
fn test_transaction_commit() -> Result<()> {
    let path = temp_db_path("transaction_commit");
    if path.exists() {
        fs::remove_file(&path)?;
    }
    let mut engine = StorageEngine::new(path.clone())?;
    // initial values
    engine.set(b"a", b"1".to_vec())?;
    engine.set(b"b", b"2".to_vec())?;

    // begin transaction and apply operations
    {
        let mut tx = engine.begin_transaction();
        tx.set(b"a", b"10".to_vec())?;
        tx.delete(b"b")?;
        tx.set(b"c", b"3".to_vec())?;
        tx.commit()?;
    }

    // verify committed state
    assert_eq!(
        engine.get(b"a").map(|a| a.as_ref().to_vec()),
        Some(b"10".to_vec())
    );
    assert_eq!(engine.get(b"b"), None);
    assert_eq!(
        engine.get(b"c").map(|a| a.as_ref().to_vec()),
        Some(b"3".to_vec())
    );

    fs::remove_file(path)?;
    Ok(())
}

#[test]
fn test_transaction_rollback() -> Result<()> {
    let path = temp_db_path("transaction_rollback");
    if path.exists() {
        fs::remove_file(&path)?;
    }
    let mut engine = StorageEngine::new(path.clone())?;
    engine.set(b"x", b"alpha".to_vec())?;

    // begin transaction and perform operations without commit
    {
        let mut tx = engine.begin_transaction();
        tx.set(b"x", b"beta".to_vec())?;
        tx.set(b"y", b"100".to_vec())?;
        tx.delete(b"x")?;
        tx.rollback()?;
    }

    // verify rollback restored original state
    assert_eq!(
        engine.get(b"x").map(|a| a.as_ref().to_vec()),
        Some(b"alpha".to_vec())
    );
    assert_eq!(engine.get(b"y"), None);

    fs::remove_file(path)?;
    Ok(())
}

#[test]
fn test_transaction_empty_commit() -> Result<()> {
    let path = temp_db_path("tx_empty_commit");
    if path.exists() {
        fs::remove_file(&path)?;
    }
    let mut engine = StorageEngine::new(path.clone())?;
    engine.set(b"a", b"1".to_vec())?;
    {
        let mut tx = engine.begin_transaction();
        // no operations
        tx.commit()?;
    }
    // state unchanged
    assert_eq!(
        engine.get(b"a").map(|a| a.as_ref().to_vec()),
        Some(b"1".to_vec())
    );
    fs::remove_file(path)?;
    Ok(())
}

#[test]
fn test_transaction_empty_rollback() -> Result<()> {
    let path = temp_db_path("tx_empty_rollback");
    if path.exists() {
        fs::remove_file(&path)?;
    }
    let mut engine = StorageEngine::new(path.clone())?;
    engine.set(b"b", b"2".to_vec())?;
    {
        let mut tx = engine.begin_transaction();
        // no operations
        tx.rollback()?;
    }
    // state unchanged
    assert_eq!(
        engine.get(b"b").map(|a| a.as_ref().to_vec()),
        Some(b"2".to_vec())
    );
    fs::remove_file(path)?;
    Ok(())
}

#[test]
fn test_transaction_snapshot_isolation() -> Result<()> {
    let path = temp_db_path("tx_snapshot");
    if path.exists() {
        fs::remove_file(&path)?;
    }
    let mut engine = StorageEngine::new(path.clone())?;
    engine.set(b"k", b"v1".to_vec())?;
    engine.set(b"k", b"v2".to_vec())?;
    {
        let mut tx = engine.begin_transaction();
        assert_eq!(tx.get(b"k").as_deref(), Some(b"v2" as &[u8]));
        tx.set(b"k", b"v3".to_vec())?;
        tx.commit()?;
    }
    assert_eq!(
        engine.get(b"k").map(|a| a.as_ref().to_vec()),
        Some(b"v3".to_vec())
    );
    fs::remove_file(path)?;
    Ok(())
}

#[test]
fn test_sequential_transactions() -> Result<()> {
    let path = temp_db_path("tx_sequential");
    if path.exists() {
        fs::remove_file(&path)?;
    }
    let mut engine = StorageEngine::new(path.clone())?;
    engine.set(b"x", b"1".to_vec())?;
    {
        let mut tx1 = engine.begin_transaction();
        tx1.set(b"x", b"10".to_vec())?;
        tx1.commit()?;
    }
    assert_eq!(
        engine.get(b"x").map(|a| a.as_ref().to_vec()),
        Some(b"10".to_vec())
    );
    {
        let mut tx2 = engine.begin_transaction();
        tx2.delete(b"x")?;
        tx2.commit()?;
    }
    assert_eq!(engine.get(b"x"), None);
    fs::remove_file(path)?;
    Ok(())
}

#[test]
fn test_double_commit_fails() -> Result<()> {
    let path = temp_db_path("tx_double_commit");
    if path.exists() {
        fs::remove_file(&path)?;
    }
    let mut engine = StorageEngine::new(path.clone())?;
    {
        let mut tx = engine.begin_transaction();
        tx.set(b"k", b"v".to_vec())?;
        tx.commit()?;
        assert!(tx.commit().is_err());
    }
    fs::remove_file(path)?;
    Ok(())
}

#[test]
fn test_commit_after_rollback_fails() -> Result<()> {
    let path = temp_db_path("tx_commit_after_rollback");
    if path.exists() {
        fs::remove_file(&path)?;
    }
    let mut engine = StorageEngine::new(path.clone())?;
    {
        let mut tx = engine.begin_transaction();
        tx.set(b"a", b"1".to_vec())?;
        tx.rollback()?;
        assert!(tx.commit().is_err());
    }
    fs::remove_file(path)?;
    Ok(())
}

#[test]
fn test_delete_then_set_in_transaction() -> Result<()> {
    let path = temp_db_path("tx_delete_then_set");
    if path.exists() {
        fs::remove_file(&path)?;
    }
    let mut engine = StorageEngine::new(path.clone())?;
    engine.set(b"x", b"old".to_vec())?;
    {
        let mut tx = engine.begin_transaction();
        tx.delete(b"x")?;
        tx.set(b"x", b"new".to_vec())?;
        tx.commit()?;
    }
    assert_eq!(
        engine.get(b"x").map(|a| a.as_ref().to_vec()),
        Some(b"new".to_vec())
    );
    fs::remove_file(path)?;
    Ok(())
}

#[test]
fn test_durability_after_commit() -> Result<()> {
    let path = temp_db_path("tx_durability_after_commit");
    if path.exists() {
        fs::remove_file(&path)?;
    }
    {
        let mut engine = StorageEngine::new(path.clone())?;
        let mut tx = engine.begin_transaction();
        tx.set(b"a", b"2".to_vec())?;
        tx.commit()?;
    }
    let engine2 = StorageEngine::new(path.clone())?;
    assert_eq!(
        engine2.get(b"a").map(|a| a.as_ref().to_vec()),
        Some(b"2".to_vec())
    );
    fs::remove_file(path)?;
    Ok(())
}

#[test]
fn test_large_transaction_memory_usage() -> Result<()> {
    let path = temp_db_path("tx_large");
    if path.exists() {
        fs::remove_file(&path)?;
    }
    let config = tegdb::storage_engine::EngineConfig {
        initial_capacity: None,
        ..Default::default()
    };
    let mut engine = StorageEngine::with_config(path.clone(), config)?;
    {
        let mut tx = engine.begin_transaction();
        for i in 0..5000 {
            let key = format!("key{i}").into_bytes();
            let value = format!("val{i}").into_bytes();
            tx.set(&key, value)?;
        }
        tx.commit()?;
    }
    assert_eq!(engine.len(), 5000);
    assert_eq!(
        engine.get(b"key0").map(|a| a.as_ref().to_vec()),
        Some(b"val0".to_vec())
    );
    assert_eq!(
        engine.get(b"key4999").map(|a| a.as_ref().to_vec()),
        Some(b"val4999".to_vec())
    );
    fs::remove_file(path)?;
    Ok(())
}

#[test]
fn test_transaction_get_behaviour() -> Result<()> {
    let path = temp_db_path("tx_get");
    if path.exists() {
        fs::remove_file(&path)?;
    }
    let mut engine = StorageEngine::new(path.clone())?;
    engine.set(b"k1", b"v1".to_vec())?;
    engine.set(b"k2", b"v2".to_vec())?;
    engine.set(b"k3", b"v3".to_vec())?;
    {
        let mut tx = engine.begin_transaction();
        assert_eq!(tx.get(b"k1").as_deref(), Some(b"v1" as &[u8]));
        assert_eq!(tx.get(b"k2").as_deref(), Some(b"v2" as &[u8]));
        tx.set(b"k1", b"v1_tx".to_vec())?;
        tx.delete(b"k2")?;
        tx.set(b"k4", b"v4".to_vec())?;
        assert_eq!(tx.get(b"k1").as_deref(), Some(b"v1_tx" as &[u8]));
        assert_eq!(tx.get(b"k2"), None);
        assert_eq!(tx.get(b"k3").as_deref(), Some(b"v3" as &[u8]));
        assert_eq!(tx.get(b"k4").as_deref(), Some(b"v4" as &[u8]));
        tx.commit()?;
    }
    assert_eq!(
        engine.get(b"k1").map(|a| a.as_ref().to_vec()),
        Some(b"v1_tx".to_vec())
    );
    assert_eq!(engine.get(b"k2"), None);
    assert_eq!(
        engine.get(b"k3").map(|a| a.as_ref().to_vec()),
        Some(b"v3".to_vec())
    );
    assert_eq!(
        engine.get(b"k4").map(|a| a.as_ref().to_vec()),
        Some(b"v4".to_vec())
    );
    fs::remove_file(path)?;
    Ok(())
}

#[test]
fn test_transaction_scan_behaviour() -> Result<()> {
    let path = temp_db_path("tx_scan");
    if path.exists() {
        fs::remove_file(&path)?;
    }
    let mut engine = StorageEngine::new(path.clone())?;
    engine.set(b"a", b"1".to_vec())?;
    engine.set(b"b", b"2".to_vec())?;
    engine.set(b"c", b"3".to_vec())?;
    let mut tx = engine.begin_transaction();
    tx.set(b"b", b"2_tx".to_vec())?;
    tx.delete(b"c")?;
    tx.set(b"d", b"4".to_vec())?;
    let result = tx.scan(b"a".to_vec()..b"z".to_vec())?;
    let result: Vec<_> = result.collect();
    drop(tx); // Drop the transaction to release the mutable borrow

    let expected = [
        (b"a".to_vec(), b"1".to_vec()),
        (b"b".to_vec(), b"2_tx".to_vec()),
        (b"d".to_vec(), b"4".to_vec()),
    ];
    assert_eq!(result.len(), expected.len());
    for (i, (actual, expected)) in result.iter().zip(expected.iter()).enumerate() {
        assert_eq!(actual.0, expected.0, "Key mismatch at index {i}");
        assert_eq!(
            actual.1.as_ref(),
            expected.1.as_slice(),
            "Value mismatch at index {i}"
        );
    }
    let base = engine
        .scan(b"a".to_vec()..b"z".to_vec())?
        .collect::<Vec<_>>();
    let base_expected = [
        (b"a".to_vec(), b"1".to_vec()),
        (b"b".to_vec(), b"2".to_vec()),
        (b"c".to_vec(), b"3".to_vec()),
    ];
    assert_eq!(base.len(), base_expected.len());
    for (i, (actual, expected)) in base.iter().zip(base_expected.iter()).enumerate() {
        assert_eq!(actual.0, expected.0, "Key mismatch at index {i}");
        assert_eq!(
            actual.1.as_ref(),
            expected.1.as_slice(),
            "Value mismatch at index {i}"
        );
    }
    fs::remove_file(path)?;
    Ok(())
}

#[test]
fn test_implicit_rollback_on_drop() -> Result<()> {
    let path = temp_db_path("tx_implicit_rollback");
    if path.exists() {
        fs::remove_file(&path)?;
    }
    let mut engine = StorageEngine::new(path.clone())?;
    engine.set(b"x", b"init".to_vec())?;
    {
        let _tx = engine.begin_transaction();
    }
    assert_eq!(
        engine.get(b"x").map(|a| a.as_ref().to_vec()),
        Some(b"init".to_vec())
    );
    fs::remove_file(path)?;
    Ok(())
}

#[test]
fn test_transaction_snapshot_after_rollback() -> Result<()> {
    let path = temp_db_path("tx_snapshot_rollback");
    if path.exists() {
        fs::remove_file(&path)?;
    }
    let mut engine = StorageEngine::new(path.clone())?;
    engine.set(b"k", b"orig".to_vec())?;
    let mut tx = engine.begin_transaction();
    tx.set(b"k", b"new".to_vec())?;
    tx.delete(b"k")?;
    tx.rollback()?;
    assert_eq!(tx.get(b"k").as_deref(), Some(b"orig" as &[u8]));
    let scan_res = tx.scan(b"k".to_vec()..vec![b'z'])?;
    let scan_res: Vec<_> = scan_res.collect();
    assert_eq!(scan_res.len(), 1);
    assert_eq!(scan_res[0].0, b"k".to_vec());
    assert_eq!(scan_res[0].1.as_ref(), b"orig");
    fs::remove_file(path)?;
    Ok(())
}

#[test]
fn test_transaction_key_size_limit() {
    let path = temp_db_path("tx_key_limit");
    if path.exists() {
        fs::remove_file(&path).unwrap();
    }
    let config = EngineConfig {
        max_key_size: 1,
        ..Default::default()
    };
    let mut engine = StorageEngine::with_config(path.clone(), config).unwrap();
    {
        let mut tx = engine.begin_transaction();
        let err = tx.set(&[0, 1], b"v".to_vec());
        assert!(err.is_err());
        tx.set(b"a", b"v".to_vec()).unwrap();
        tx.commit().unwrap();
    }
    assert_eq!(
        engine.get(b"a").map(|a| a.as_ref().to_vec()),
        Some(b"v".to_vec())
    );
    fs::remove_file(path).unwrap();
}

#[test]
fn test_transaction_value_size_limit() {
    let path = temp_db_path("tx_value_limit");
    if path.exists() {
        fs::remove_file(&path).unwrap();
    }
    let config = EngineConfig {
        max_value_size: 1,
        ..Default::default()
    };
    let mut engine = StorageEngine::with_config(path.clone(), config).unwrap();
    {
        let mut tx = engine.begin_transaction();
        let err = tx.set(b"k", vec![0, 1]);
        assert!(err.is_err());
        tx.set(b"k", vec![0]).unwrap();
        tx.commit().unwrap();
    }
    assert_eq!(engine.get(b"k").map(|a| a.as_ref().to_vec()), Some(vec![0]));
    fs::remove_file(path).unwrap();
}

#[test]
fn test_transaction_error_propagation_in_transaction() -> Result<()> {
    let path = temp_db_path("tx_error_prop");
    if path.exists() {
        fs::remove_file(&path)?;
    }
    let config = EngineConfig {
        max_key_size: 1,
        ..Default::default()
    };
    let mut engine = StorageEngine::with_config(path.clone(), config)?;
    {
        let mut tx = engine.begin_transaction();
        tx.set(b"a", b"1".to_vec())?;
        let err = tx.set(&[0, 1], b"2".to_vec());
        assert!(err.is_err());
        tx.set(b"b", b"3".to_vec())?;
        tx.commit()?;
    }
    assert_eq!(
        engine.get(b"a").map(|a| a.as_ref().to_vec()),
        Some(b"1".to_vec())
    );
    assert_eq!(
        engine.get(b"b").map(|a| a.as_ref().to_vec()),
        Some(b"3".to_vec())
    );
    assert_eq!(engine.get(&[0, 1]), None);
    fs::remove_file(path)?;
    Ok(())
}

#[test]
fn test_pure_transaction_crash_recovery() -> Result<()> {
    let path = temp_db_path("tx_pure_crash_recovery");
    if path.exists() {
        fs::remove_file(&path)?;
    }

    // Simulate a crash: start transaction, perform writes, but don't commit
    {
        let mut engine = StorageEngine::new(path.clone())?;
        let mut tx = engine.begin_transaction();
        tx.set(b"key1", b"value1".to_vec())?;
        tx.set(b"key2", b"value2".to_vec())?;
        // Transaction dropped without commit - simulates crash
    }

    // Reopen engine to trigger recovery
    let engine2 = StorageEngine::new(path.clone())?;

    // Uncommitted transaction should be rolled back
    assert_eq!(engine2.get(b"key1"), None);
    assert_eq!(engine2.get(b"key2"), None);

    fs::remove_file(path)?;
    Ok(())
}

#[test]
fn test_inline_vs_disk_metrics_and_cache_hits() -> Result<()> {
    let path = temp_db_path("tx_inline_vs_disk_metrics");
    if path.exists() {
        fs::remove_file(&path)?;
    }
    let config = EngineConfig {
        inline_value_threshold: 4,
        cache_size_bytes: 64,
        ..Default::default()
    };
    let mut engine = StorageEngine::with_config(path.clone(), config)?;
    let small = b"sm".to_vec(); // inline
    let large = b"12345678".to_vec(); // stored on disk, exceeds threshold
    engine.set(b"ks", small.clone())?;
    engine.set(b"kl", large.clone())?;

    // Inline read should not hit disk or cache accounting
    assert_eq!(
        engine.get(b"ks").map(|v| v.as_ref().to_vec()),
        Some(small.clone())
    );
    let m0 = engine.metrics();
    assert_eq!(m0.bytes_read, 0);
    assert_eq!(m0.cache_hits, 0);
    assert_eq!(m0.cache_misses, 0);

    // First large read should miss cache and read from disk
    assert_eq!(
        engine.get(b"kl").map(|v| v.as_ref().to_vec()),
        Some(large.clone())
    );
    let m1 = engine.metrics();
    assert_eq!(m1.bytes_read, large.len() as u64);
    assert_eq!(m1.cache_hits, 0);
    assert_eq!(m1.cache_misses, 1);

    // Second large read should hit cache, no extra bytes_read
    assert_eq!(
        engine.get(b"kl").map(|v| v.as_ref().to_vec()),
        Some(large.clone())
    );
    let m2 = engine.metrics();
    assert_eq!(m2.bytes_read, large.len() as u64);
    assert_eq!(m2.cache_hits, 1);
    assert_eq!(m2.cache_misses, 1);

    fs::remove_file(path)?;
    Ok(())
}

#[test]
fn test_cache_eviction_on_cap() -> Result<()> {
    let path = temp_db_path("tx_cache_eviction");
    if path.exists() {
        fs::remove_file(&path)?;
    }
    // Force non-inline values and a tiny cache so eviction is required
    let config = EngineConfig {
        inline_value_threshold: 0,
        cache_size_bytes: 12,
        ..Default::default()
    };
    let mut engine = StorageEngine::with_config(path.clone(), config)?;
    let v1 = b"abcdefgh".to_vec(); // 8 bytes
    let v2 = b"ijklmnop".to_vec(); // 8 bytes
    engine.set(b"k1", v1.clone())?;
    engine.set(b"k2", v2.clone())?;

    // First read of k1: miss
    assert_eq!(
        engine.get(b"k1").map(|v| v.as_ref().to_vec()),
        Some(v1.clone())
    );
    // First read of k2: miss, should evict k1 due to cap
    assert_eq!(
        engine.get(b"k2").map(|v| v.as_ref().to_vec()),
        Some(v2.clone())
    );
    // Second read of k1: should miss again because k1 was evicted
    assert_eq!(
        engine.get(b"k1").map(|v| v.as_ref().to_vec()),
        Some(v1.clone())
    );

    let m = engine.metrics();
    assert_eq!(m.cache_hits, 0);
    assert_eq!(m.cache_misses, 3);
    assert_eq!(m.bytes_read, (v1.len() as u64) * 2 + v2.len() as u64);

    fs::remove_file(path)?;
    Ok(())
}
