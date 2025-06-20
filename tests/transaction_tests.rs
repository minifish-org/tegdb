use std::{path::PathBuf, fs, env};
use tegdb::{Engine, EngineConfig, Result};

/// Creates a unique temporary file path for tests
fn temp_db_path(prefix: &str) -> PathBuf {
    let mut path = env::temp_dir();
    path.push(format!("tegdb_test_{}_{}", prefix, std::process::id()));
    path
}

#[test]
fn test_transaction_commit() -> Result<()> {
    let path = temp_db_path("transaction_commit");
    if path.exists() {
        fs::remove_file(&path)?;
    }
    let mut engine = Engine::new(path.clone())?;
    // initial values
    engine.set(b"a", b"1".to_vec())?;
    engine.set(b"b", b"2".to_vec())?;

    // begin transaction and apply operations
    {
        let mut tx = engine.begin_transaction();
        tx.set(b"a".to_vec(), b"10".to_vec())?;
        tx.delete(b"b".to_vec())?;
        tx.set(b"c".to_vec(), b"3".to_vec())?;
        tx.commit()?;
    }

    // verify committed state
    assert_eq!(engine.get(b"a").map(|a| a.as_ref().to_vec()), Some(b"10".to_vec()));
    assert_eq!(engine.get(b"b"), None);
    assert_eq!(engine.get(b"c").map(|a| a.as_ref().to_vec()), Some(b"3".to_vec()));

    fs::remove_file(path)?;
    Ok(())
}

#[test]
fn test_transaction_rollback() -> Result<()> {
    let path = temp_db_path("transaction_rollback");
    if path.exists() {
        fs::remove_file(&path)?;
    }
    let mut engine = Engine::new(path.clone())?;
    engine.set(b"x", b"alpha".to_vec())?;

    // begin transaction and perform operations without commit
    {
        let mut tx = engine.begin_transaction();
        tx.set(b"x".to_vec(), b"beta".to_vec())?;
        tx.set(b"y".to_vec(), b"100".to_vec())?;
        tx.delete(b"x".to_vec())?;
        tx.rollback();
    }

    // verify rollback restored original state
    assert_eq!(engine.get(b"x").map(|a| a.as_ref().to_vec()), Some(b"alpha".to_vec()));
    assert_eq!(engine.get(b"y"), None);

    fs::remove_file(path)?;
    Ok(())
}

#[test]
fn test_transaction_empty_commit() -> Result<()> {
    let path = temp_db_path("tx_empty_commit");
    if path.exists() { fs::remove_file(&path)?; }
    let mut engine = Engine::new(path.clone())?;
    engine.set(b"a", b"1".to_vec())?;
    {
        let mut tx = engine.begin_transaction();
        // no operations
        tx.commit()?;
    }
    // state unchanged
    assert_eq!(engine.get(b"a").map(|a| a.as_ref().to_vec()), Some(b"1".to_vec()));
    fs::remove_file(path)?;
    Ok(())
}

#[test]
fn test_transaction_empty_rollback() -> Result<()> {
    let path = temp_db_path("tx_empty_rollback");
    if path.exists() { fs::remove_file(&path)?; }
    let mut engine = Engine::new(path.clone())?;
    engine.set(b"b", b"2".to_vec())?;
    {
        let mut tx = engine.begin_transaction();
        // no operations
        tx.rollback();
    }
    // state unchanged
    assert_eq!(engine.get(b"b").map(|a| a.as_ref().to_vec()), Some(b"2".to_vec()));
    fs::remove_file(path)?;
    Ok(())
}

#[test]
fn test_transaction_snapshot_isolation() -> Result<()> {
    let path = temp_db_path("tx_snapshot");
    if path.exists() { fs::remove_file(&path)?; }
    let mut engine = Engine::new(path.clone())?;
    engine.set(b"k", b"v1".to_vec())?;
    engine.set(b"k", b"v2".to_vec())?;
    {
        let mut tx = engine.begin_transaction();
        assert_eq!(tx.get(b"k"), Some(b"v2".to_vec()));
        tx.set(b"k".to_vec(), b"v3".to_vec())?;
        tx.commit()?;
    }
    assert_eq!(engine.get(b"k").map(|a| a.as_ref().to_vec()), Some(b"v3".to_vec()));
    fs::remove_file(path)?;
    Ok(())
}

#[test]
fn test_sequential_transactions() -> Result<()> {
    let path = temp_db_path("tx_sequential");
    if path.exists() { fs::remove_file(&path)?; }
    let mut engine = Engine::new(path.clone())?;
    engine.set(b"x", b"1".to_vec())?;
    {
        let mut tx1 = engine.begin_transaction();
        tx1.set(b"x".to_vec(), b"10".to_vec())?;
        tx1.commit()?;
    }
    assert_eq!(engine.get(b"x").map(|a| a.as_ref().to_vec()), Some(b"10".to_vec()));
    {
        let mut tx2 = engine.begin_transaction();
        tx2.delete(b"x".to_vec())?;
        tx2.commit()?;
    }
    assert_eq!(engine.get(b"x"), None);
    fs::remove_file(path)?;
    Ok(())
}

#[test]
fn test_uncommitted_transaction_not_persisted() -> Result<()> {
    let path = temp_db_path("tx_uncommitted_shutdown");
    if path.exists() { fs::remove_file(&path)?; }
    {
        let mut engine = Engine::new(path.clone())?;
        engine.set(b"a", b"1".to_vec())?;
        let mut tx = engine.begin_transaction();
        tx.set(b"a".to_vec(), b"2".to_vec())?;
        tx.set(b"b".to_vec(), b"3".to_vec())?;
    }
    let engine2 = Engine::new(path.clone())?;
    assert_eq!(engine2.get(b"a").map(|a| a.as_ref().to_vec()), Some(b"1".to_vec()));
    assert_eq!(engine2.get(b"b"), None);
    fs::remove_file(path)?;
    Ok(())
}

#[test]
fn test_double_commit_fails() -> Result<()> {
    let path = temp_db_path("tx_double_commit");
    if path.exists() { fs::remove_file(&path)?; }
    let mut engine = Engine::new(path.clone())?;
    {
        let mut tx = engine.begin_transaction();
        tx.set(b"k".to_vec(), b"v".to_vec())?;
        tx.commit()?;
        assert!(tx.commit().is_err());
    }
    fs::remove_file(path)?;
    Ok(())
}

#[test]
fn test_commit_after_rollback_fails() -> Result<()> {
    let path = temp_db_path("tx_commit_after_rollback");
    if path.exists() { fs::remove_file(&path)?; }
    let mut engine = Engine::new(path.clone())?;
    {
        let mut tx = engine.begin_transaction();
        tx.set(b"a".to_vec(), b"1".to_vec())?;
        tx.rollback();
        assert!(tx.commit().is_err());
    }
    fs::remove_file(path)?;
    Ok(())
}

#[test]
fn test_delete_then_set_in_transaction() -> Result<()> {
    let path = temp_db_path("tx_delete_then_set");
    if path.exists() { fs::remove_file(&path)?; }
    let mut engine = Engine::new(path.clone())?;
    engine.set(b"x", b"old".to_vec())?;
    {
        let mut tx = engine.begin_transaction();
        tx.delete(b"x".to_vec())?;
        tx.set(b"x".to_vec(), b"new".to_vec())?;
        tx.commit()?;
    }
    assert_eq!(engine.get(b"x").map(|a| a.as_ref().to_vec()), Some(b"new".to_vec()));
    fs::remove_file(path)?;
    Ok(())
}

#[test]
fn test_durability_after_commit() -> Result<()> {
    let path = temp_db_path("tx_durability_after_commit");
    if path.exists() { fs::remove_file(&path)?; }
    {
        let mut engine = Engine::new(path.clone())?;
        engine.set(b"a", b"1".to_vec())?;
        let mut tx = engine.begin_transaction();
        tx.set(b"a".to_vec(), b"2".to_vec())?;
        tx.commit()?;
    }
    let engine2 = Engine::new(path.clone())?;
    assert_eq!(engine2.get(b"a").map(|a| a.as_ref().to_vec()), Some(b"2".to_vec()));
    fs::remove_file(path)?;
    Ok(())
}

#[test]
fn test_large_transaction_memory_usage() -> Result<()> {
    let path = temp_db_path("tx_large");
    if path.exists() { fs::remove_file(&path)?; }
    let mut engine = Engine::new(path.clone())?;
    {
        let mut tx = engine.begin_transaction();
        for i in 0..5000 {
            let key = format!("key{}", i).into_bytes();
            let value = format!("val{}", i).into_bytes();
            tx.set(key, value)?;
        }
        tx.commit()?;
    }
    assert_eq!(engine.len(), 5000);
    assert_eq!(engine.get(b"key0").map(|a| a.as_ref().to_vec()), Some(b"val0".to_vec()));
    assert_eq!(engine.get(b"key4999").map(|a| a.as_ref().to_vec()), Some(b"val4999".to_vec()));
    fs::remove_file(path)?;
    Ok(())
}

#[test]
fn test_mix_raw_and_transaction() -> Result<()> {
    let path = temp_db_path("mix_raw_tx");
    if path.exists() { fs::remove_file(&path)?; }
    let mut engine = Engine::new(path.clone())?;
    engine.set(b"k", b"raw1".to_vec())?;
    assert_eq!(engine.get(b"k").map(|a| a.as_ref().to_vec()), Some(b"raw1".to_vec()));
    let snap = {
        let tx = engine.begin_transaction();
        tx.get(b"k").unwrap()
    };
    assert_eq!(snap, b"raw1".to_vec());
    engine.set(b"k", b"raw2".to_vec())?;
    assert_eq!(engine.get(b"k").map(|a| a.as_ref().to_vec()), Some(b"raw2".to_vec()));
    assert_eq!(snap, b"raw1".to_vec());
    {
        let mut tx2 = engine.begin_transaction();
        tx2.set(b"k".to_vec(), b"tx1".to_vec())?;
        tx2.commit()?;
    }
    assert_eq!(engine.get(b"k").map(|a| a.as_ref().to_vec()), Some(b"tx1".to_vec()));
    engine.del(b"k")?;
    assert_eq!(engine.get(b"k"), None);
    {
        let tx3 = engine.begin_transaction();
        assert_eq!(tx3.get(b"k"), None);
    }
    fs::remove_file(path)?;
    Ok(())
}

#[test]
fn test_transaction_get_behaviour() -> Result<()> {
    let path = temp_db_path("tx_get");
    if path.exists() { fs::remove_file(&path)?; }
    let mut engine = Engine::new(path.clone())?;
    engine.set(b"k1", b"v1".to_vec())?;
    engine.set(b"k2", b"v2".to_vec())?;
    engine.set(b"k3", b"v3".to_vec())?;
    {
        let mut tx = engine.begin_transaction();
        assert_eq!(tx.get(b"k1"), Some(b"v1".to_vec()));
        assert_eq!(tx.get(b"k2"), Some(b"v2".to_vec()));
        tx.set(b"k1".to_vec(), b"v1_tx".to_vec())?;
        tx.delete(b"k2".to_vec())?;
        tx.set(b"k4".to_vec(), b"v4".to_vec())?;
        assert_eq!(tx.get(b"k1"), Some(b"v1_tx".to_vec()));
        assert_eq!(tx.get(b"k2"), None);
        assert_eq!(tx.get(b"k3"), Some(b"v3".to_vec()));
        assert_eq!(tx.get(b"k4"), Some(b"v4".to_vec()));
        tx.commit()?;
    }
    assert_eq!(engine.get(b"k1").map(|a| a.as_ref().to_vec()), Some(b"v1_tx".to_vec()));
    assert_eq!(engine.get(b"k2"), None);
    assert_eq!(engine.get(b"k3").map(|a| a.as_ref().to_vec()), Some(b"v3".to_vec()));
    assert_eq!(engine.get(b"k4").map(|a| a.as_ref().to_vec()), Some(b"v4".to_vec()));
    fs::remove_file(path)?;
    Ok(())
}

#[test]
fn test_transaction_scan_behaviour() -> Result<()> {
    let path = temp_db_path("tx_scan");
    if path.exists() { fs::remove_file(&path)?; }
    let mut engine = Engine::new(path.clone())?;
    engine.set(b"a", b"1".to_vec())?;
    engine.set(b"b", b"2".to_vec())?;
    engine.set(b"c", b"3".to_vec())?;
    let result = {
        let mut tx = engine.begin_transaction();
        tx.set(b"b".to_vec(), b"2_tx".to_vec())?;
        tx.delete(b"c".to_vec())?;
        tx.set(b"d".to_vec(), b"4".to_vec())?;
        tx.scan(b"a".to_vec()..b"z".to_vec())
    };
    let expected = vec![
        (b"a".to_vec(), b"1".to_vec()),
        (b"b".to_vec(), b"2_tx".to_vec()),
        (b"d".to_vec(), b"4".to_vec()),
    ];
    assert_eq!(result, expected);
    let base = engine.scan(b"a".to_vec()..b"z".to_vec())?.collect::<Vec<_>>();
    let base_expected = vec![
        (b"a".to_vec(), b"1".to_vec()),
        (b"b".to_vec(), b"2".to_vec()),
        (b"c".to_vec(), b"3".to_vec()),
    ];
    assert_eq!(base.len(), base_expected.len());
    for (i, (actual, expected)) in base.iter().zip(base_expected.iter()).enumerate() {
        assert_eq!(actual.0, expected.0, "Key mismatch at index {}", i);
        assert_eq!(actual.1.as_ref(), expected.1.as_slice(), "Value mismatch at index {}", i);
    }
    fs::remove_file(path)?;  
    Ok(())
}

#[test]
fn test_implicit_rollback_on_drop() -> Result<()> {
    let path = temp_db_path("tx_implicit_rollback");
    if path.exists() { fs::remove_file(&path)?; }
    let mut engine = Engine::new(path.clone())?;
    engine.set(b"x", b"init".to_vec())?;
    {
        let _tx = engine.begin_transaction();
    }
    assert_eq!(engine.get(b"x").map(|a| a.as_ref().to_vec()), Some(b"init".to_vec()));
    fs::remove_file(path)?;
    Ok(())
}

#[test]
fn test_transaction_snapshot_after_rollback() -> Result<()> {
    let path = temp_db_path("tx_snapshot_rollback");
    if path.exists() { fs::remove_file(&path)?; }
    let mut engine = Engine::new(path.clone())?;
    engine.set(b"k", b"orig".to_vec())?;
    let mut tx = engine.begin_transaction();
    tx.set(b"k".to_vec(), b"new".to_vec())?;
    tx.delete(b"k".to_vec())?;
    tx.rollback();
    assert_eq!(tx.get(b"k"), Some(b"orig".to_vec()));
    let scan_res = tx.scan(b"k".to_vec()..vec![b'z']);
    assert_eq!(scan_res, vec![(b"k".to_vec(), b"orig".to_vec())]);
    fs::remove_file(path)?;
    Ok(())
}

#[test]
fn test_transaction_key_size_limit() {
    let path = temp_db_path("tx_key_limit");
    if path.exists() { fs::remove_file(&path).unwrap(); }
    let mut config = EngineConfig::default();
    config.max_key_size = 1;
    let mut engine = Engine::with_config(path.clone(), config).unwrap();
    {
        let mut tx = engine.begin_transaction();
        let err = tx.set(vec![0, 1], b"v".to_vec());
        assert!(err.is_err());
        tx.set(b"a".to_vec(), b"v".to_vec()).unwrap();
        tx.commit().unwrap();
    }
    assert_eq!(engine.get(b"a").map(|a| a.as_ref().to_vec()), Some(b"v".to_vec()));
    fs::remove_file(path).unwrap();
}

#[test]
fn test_transaction_value_size_limit() {
    let path = temp_db_path("tx_value_limit");
    if path.exists() { fs::remove_file(&path).unwrap(); }
    let mut config = EngineConfig::default();
    config.max_value_size = 1;
    let mut engine = Engine::with_config(path.clone(), config).unwrap();
    {
        let mut tx = engine.begin_transaction();
        let err = tx.set(b"k".to_vec(), vec![0, 1]);
        assert!(err.is_err());
        tx.set(b"k".to_vec(), vec![0]).unwrap();
        tx.commit().unwrap();
    }
    assert_eq!(engine.get(b"k").map(|a| a.as_ref().to_vec()), Some(vec![0]));
    fs::remove_file(path).unwrap();
}

#[test]
fn test_transaction_error_propagation_in_transaction() -> Result<()> {
    let path = temp_db_path("tx_error_prop");
    if path.exists() { fs::remove_file(&path)?; }
    let mut config = EngineConfig::default();
    config.max_key_size = 1;
    let mut engine = Engine::with_config(path.clone(), config)?;
    {
        let mut tx = engine.begin_transaction();
        tx.set(b"a".to_vec(), b"1".to_vec())?;
        let err = tx.set(vec![0,1], b"2".to_vec());
        assert!(err.is_err());
        tx.set(b"b".to_vec(), b"3".to_vec())?;
        tx.commit()?;
    }
    assert_eq!(engine.get(b"a").map(|a| a.as_ref().to_vec()), Some(b"1".to_vec()));
    assert_eq!(engine.get(b"b").map(|a| a.as_ref().to_vec()), Some(b"3".to_vec()));
    assert_eq!(engine.get(&vec![0,1]), None);
    fs::remove_file(path)?;
    Ok(())
}
