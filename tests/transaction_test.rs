use std::io::Error;
use std::path::PathBuf;
use std::fs;
use tokio;

use tegdb::Database;

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

#[tokio::test]
async fn test_rollback_effect() -> Result<(), Error> {
    let path = PathBuf::from("test_rollback_effect.db");
    let db = Database::new(path.clone()).await;
    {
        let mut txn = db.new_transaction().await;
        txn.insert(b"temp_key", b"temp_value".to_vec()).await?;
        assert_eq!(txn.select(b"temp_key").await.unwrap().0, Some(b"temp_value".to_vec()));
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

#[tokio::test]
async fn test_data_durability() -> Result<(), Error> {
    let path = PathBuf::from("test_durability.db");
    
    // Clean up any existing test directory and lock file
    fs::remove_dir_all(&path).ok();
    fs::remove_file(path.join("lock.lock")).ok();
    
    // First phase: Write some data and close the database
    {
        let db = Database::new(path.clone()).await;
        let mut txn = db.new_transaction().await;
        
        // Insert multiple test keys
        let test_data = vec![
            (b"key1".as_ref(), b"value1".as_ref()),
            (b"key2".as_ref(), b"value2".as_ref()),
            (b"key3".as_ref(), b"value3".as_ref()),
        ];
        
        for (key, value) in &test_data {
            txn.insert(key, value.to_vec()).await?;
            // Verify data is immediately available
            assert_eq!(txn.select(key).await.unwrap().0, Some(value.to_vec()));
        }
        
        txn.commit().await?;
        // Force flush before shutdown
        db.engine.flush()?;
        // Stop GC and persist snapshot
        db.shutdown().await;
        // Manually remove the lock file
        fs::remove_file(path.join("lock.lock"))?;
    }
    
    // Second phase: Reopen database and verify data persists
    {
        let db = Database::new(path.clone()).await;
        let mut txn = db.new_transaction().await;
        
        // Verify all data is still there
        let test_data = vec![
            (b"key1".as_ref(), b"value1".as_ref()),
            (b"key2".as_ref(), b"value2".as_ref()),
            (b"key3".as_ref(), b"value3".as_ref()),
        ];
        
        for (key, value) in &test_data {
            let result = txn.select(key).await.unwrap();
            assert_eq!(result.0, Some(value.to_vec()), 
                "Data not found for key: {}", String::from_utf8_lossy(key));
        }
        
        txn.rollback().await?;
        // Force flush before shutdown
        db.engine.flush()?;
        // Stop GC and persist snapshot
        db.shutdown().await;
        // Manually remove the lock file
        fs::remove_file(path.join("lock.lock"))?;
    }
    
    // Cleanup
    fs::remove_dir_all(&path).unwrap();
    Ok(())
}
