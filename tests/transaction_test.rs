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
