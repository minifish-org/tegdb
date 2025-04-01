use std::fs;
use std::io::Error;
use std::path::PathBuf;
use tokio;

use tegdb::Database;

// Basic CRUD Operations Tests
#[tokio::test]
async fn test_insert_and_select() -> Result<(), Error> {
    let path = PathBuf::from("test_insert_and_select.db");
    let db = Database::new(path.clone()).await;
    let mut txn = db.new_transaction().await;
    let key = b"test_key";
    let value = b"test_value".to_vec();

    txn.insert(key, value.clone()).await?;
    assert_eq!(txn.select(key).await.unwrap().0, Some(value.clone()));
    txn.commit().await?;

    let mut txn = db.new_transaction().await;
    assert_eq!(txn.select(key).await.unwrap().0, Some(value));
    txn.rollback().await?;

    db.shutdown().await;
    fs::remove_dir_all(&path).unwrap();
    Ok(())
}

#[tokio::test]
async fn test_update() -> Result<(), Error> {
    let path = PathBuf::from("test_update.db");
    let db = Database::new(path.clone()).await;
    let mut txn = db.new_transaction().await;
    let key = b"test_key";
    let initial = b"initial".to_vec();
    let updated = b"updated".to_vec();

    txn.insert(key, initial.clone()).await?;
    assert_eq!(txn.select(key).await.unwrap().0, Some(initial));
    txn.update(key, updated.clone()).await?;
    assert_eq!(txn.select(key).await.unwrap().0, Some(updated.clone()));
    txn.commit().await?;

    let mut txn = db.new_transaction().await;
    assert_eq!(txn.select(key).await.unwrap().0, Some(updated));
    txn.rollback().await?;

    db.shutdown().await;
    fs::remove_dir_all(&path).unwrap();
    Ok(())
}

#[tokio::test]
async fn test_delete() -> Result<(), Error> {
    let path = PathBuf::from("test_delete.db");
    let db = Database::new(path.clone()).await;
    let mut txn = db.new_transaction().await;
    let key = b"test_key";
    let value = b"to_delete".to_vec();

    txn.insert(key, value).await?;
    assert!(txn.select(key).await.unwrap().0.is_some());
    txn.delete(key).await?;
    assert!(txn.select(key).await.unwrap().0.is_none());

    let mut txn = db.new_transaction().await;
    assert!(txn.select(key).await.unwrap().0.is_none());
    txn.rollback().await?;

    db.shutdown().await;
    fs::remove_dir_all(&path).unwrap();
    Ok(())
}

// Transaction Management Tests
#[tokio::test]
async fn test_rollback_effect() -> Result<(), Error> {
    let path = PathBuf::from("test_rollback_effect.db");
    let db = Database::new(path.clone()).await;
    {
        let mut txn = db.new_transaction().await;
        txn.insert(b"temp_key", b"temp_value".to_vec()).await?;
        assert_eq!(
            txn.select(b"temp_key").await.unwrap().0,
            Some(b"temp_value".to_vec())
        );
        txn.rollback().await?;
    }
    {
        let mut txn = db.new_transaction().await;
        assert_eq!(txn.select(b"temp_key").await.unwrap().0, None);
        txn.rollback().await?;
    }

    db.shutdown().await;
    fs::remove_dir_all(&path).unwrap();
    Ok(())
}

// ACID Properties Tests
#[tokio::test]
async fn test_atomicity() -> Result<(), Error> {
    let path = PathBuf::from("test_atomicity.db");
    let db = Database::new(path.clone()).await;

    // Test successful atomic transaction
    {
        let mut txn = db.new_transaction().await;
        txn.insert(b"key1", b"value1".to_vec()).await?;
        txn.insert(b"key2", b"value2".to_vec()).await?;
        txn.commit().await?;

        // Verify both operations were committed
        let mut txn2 = db.new_transaction().await;
        assert_eq!(
            txn2.select(b"key1").await.unwrap().0,
            Some(b"value1".to_vec())
        );
        assert_eq!(
            txn2.select(b"key2").await.unwrap().0,
            Some(b"value2".to_vec())
        );
        txn2.rollback().await?;
    }

    // Test rollback on error
    {
        let mut txn = db.new_transaction().await;
        txn.insert(b"key3", b"value3".to_vec()).await?;
        txn.insert(b"key4", b"value4".to_vec()).await?;
        txn.rollback().await?;

        // Verify both operations were rolled back
        let mut txn2 = db.new_transaction().await;
        assert_eq!(txn2.select(b"key3").await.unwrap().0, None);
        assert_eq!(txn2.select(b"key4").await.unwrap().0, None);
        txn2.rollback().await?;
    }

    db.shutdown().await;
    fs::remove_dir_all(&path).unwrap();
    Ok(())
}

#[tokio::test]
async fn test_consistency() -> Result<(), Error> {
    let path = PathBuf::from("test_consistency.db");
    let db = Database::new(path.clone()).await;

    // Test maintaining referential integrity
    {
        let mut txn = db.new_transaction().await;
        txn.insert(b"parent", b"parent_value".to_vec()).await?;
        txn.insert(b"child", b"child_value".to_vec()).await?;
        txn.commit().await?;

        // Verify both records exist
        let mut txn2 = db.new_transaction().await;
        assert_eq!(
            txn2.select(b"parent").await.unwrap().0,
            Some(b"parent_value".to_vec())
        );
        assert_eq!(
            txn2.select(b"child").await.unwrap().0,
            Some(b"child_value".to_vec())
        );
        txn2.rollback().await?;
    }

    // Test maintaining data constraints
    {
        let mut txn = db.new_transaction().await;
        // Test that we can't insert a value that's too large
        let large_value = vec![0u8; 256 * 1024 + 1]; // Exceeds MAX_VALUE_SIZE
        assert!(txn.insert(b"large_key", large_value).await.is_err());
        txn.rollback().await?;
    }

    db.shutdown().await;
    fs::remove_dir_all(&path).unwrap();
    Ok(())
}

#[tokio::test]
async fn test_isolation() -> Result<(), Error> {
    let path = PathBuf::from("test_isolation.db");
    let db = Database::new(path.clone()).await;

    // Test serializable isolation with interleaved transactions

    // First transaction: Insert initial value
    let mut txn1 = db.new_transaction().await;
    txn1.insert(b"shared_key", b"value1".to_vec()).await?;

    // Second transaction: Try to read and update while first transaction is active
    let mut txn2 = db.new_transaction().await;
    // This should see no value since txn1 hasn't committed
    assert_eq!(txn2.select(b"shared_key").await.unwrap().0, None);

    // First transaction commits
    txn1.commit().await?;

    // Second transaction should now see the committed value
    assert_eq!(
        txn2.select(b"shared_key").await.unwrap().0,
        Some(b"value1".to_vec())
    );
    // Update the value
    txn2.update(b"shared_key", b"value2".to_vec()).await?;
    txn2.commit().await?;

    // Verify final state
    let mut final_txn = db.new_transaction().await;
    let (final_value, _) = final_txn.select(b"shared_key").await.unwrap();
    assert_eq!(final_value, Some(b"value2".to_vec()));
    final_txn.rollback().await?;

    db.shutdown().await;
    fs::remove_dir_all(&path).unwrap();
    Ok(())
}

#[tokio::test]
async fn test_durability() -> Result<(), Error> {
    let path = PathBuf::from("test_durability.db");

    // First phase: Write data and commit
    {
        let db = Database::new(path.clone()).await;
        let mut txn = db.new_transaction().await;
        txn.insert(b"durable_key", b"durable_value".to_vec())
            .await?;
        txn.commit().await?;
        db.engine.flush()?;
        db.shutdown().await;
    }

    // Simulate system failure by removing lock file
    fs::remove_file(path.join("lock.lock")).ok();

    // Second phase: Reopen and verify data persists
    {
        let db = Database::new(path.clone()).await;
        let mut txn = db.new_transaction().await;
        let (value, _) = txn.select(b"durable_key").await.unwrap();
        assert_eq!(value, Some(b"durable_value".to_vec()));
        txn.rollback().await?;
        db.shutdown().await;
    }

    fs::remove_dir_all(&path).unwrap();
    Ok(())
}

// Concurrency Tests
#[tokio::test]
async fn test_deadlock_detection() -> Result<(), Error> {
    let path = PathBuf::from("test_deadlock.db");
    let db = Database::new(path.clone()).await;

    // First transaction acquires lock on key1
    let mut txn1 = db.new_transaction().await;
    txn1.insert(b"key1", b"value1".to_vec()).await?;

    // Second transaction acquires lock on key2
    let mut txn2 = db.new_transaction().await;
    txn2.insert(b"key2", b"value2".to_vec()).await?;

    // First transaction tries to lock key2 (should fail)
    assert!(txn1.insert(b"key2", b"value2".to_vec()).await.is_err());

    // Second transaction tries to lock key1 (should fail)
    assert!(txn2.insert(b"key1", b"value1".to_vec()).await.is_err());

    // Both transactions should rollback
    txn1.rollback().await?;
    txn2.rollback().await?;

    // Verify no data was committed
    let mut final_txn = db.new_transaction().await;
    assert_eq!(final_txn.select(b"key1").await.unwrap().0, None);
    assert_eq!(final_txn.select(b"key2").await.unwrap().0, None);
    final_txn.rollback().await?;

    db.shutdown().await;
    fs::remove_dir_all(&path).unwrap();
    Ok(())
}

#[tokio::test]
async fn test_write_conflicts() -> Result<(), Error> {
    let path = PathBuf::from("test_conflicts.db");
    let db = Database::new(path.clone()).await;

    // First transaction: Insert initial value
    let mut txn1 = db.new_transaction().await;
    txn1.insert(b"conflict_key", b"initial".to_vec()).await?;
    txn1.commit().await?;

    // Second transaction: Try to update the same key
    let mut txn2 = db.new_transaction().await;
    txn2.update(b"conflict_key", b"updated".to_vec()).await?;
    txn2.commit().await?;

    // Verify final state
    let mut final_txn = db.new_transaction().await;
    let (value, _) = final_txn.select(b"conflict_key").await.unwrap();
    assert_eq!(value, Some(b"updated".to_vec()));
    final_txn.rollback().await?;

    db.shutdown().await;
    fs::remove_dir_all(&path).unwrap();
    Ok(())
}
