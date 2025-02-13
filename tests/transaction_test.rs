use std::io::Error;
use std::path::PathBuf;
use std::fs;
use tokio;

use tegdb::Database;
use tegdb::Transaction;

#[tokio::test]
async fn test_insert_and_select() -> Result<(), Error> {
    let path = PathBuf::from("test_insert_and_select.db");
    let db = Database::new(path.clone());
    let mut txn = Transaction::begin(db.clone());
    let key = b"test_key";
    let value = b"test_value".to_vec();

    txn.insert(key, value.clone()).await?;
    assert_eq!(txn.select(key).await, Some(value.clone()));
    txn.commit().await?;

    let mut txn = Transaction::begin(db.clone());
    assert_eq!(txn.select(key).await, Some(value));
    txn.rollback().await?;

    fs::remove_file(&path).unwrap();
    Ok(())
}

#[tokio::test]
async fn test_update() -> Result<(), Error> {
    let path = PathBuf::from("test_update.db");
    let db = Database::new(path.clone());
    let mut txn = Transaction::begin(db.clone());
    let key = b"test_key";
    let initial = b"initial".to_vec();
    let updated = b"updated".to_vec();

    txn.insert(key, initial.clone()).await?;
    assert_eq!(txn.select(key).await, Some(initial));
    txn.update(key, updated.clone()).await?;
    assert_eq!(txn.select(key).await, Some(updated.clone()));
    txn.commit().await?;

    let mut txn = Transaction::begin(db.clone());
    assert_eq!(txn.select(key).await, Some(updated));
    txn.rollback().await?;

    fs::remove_file(&path).unwrap();
    Ok(())
}

#[tokio::test]
async fn test_delete() -> Result<(), Error> {
    let path = PathBuf::from("test_delete.db");
    let db = Database::new(path.clone());
    let mut txn = Transaction::begin(db.clone());
    let key = b"test_key";
    let value = b"to_delete".to_vec();

    txn.insert(key, value).await?;
    assert!(txn.select(key).await.is_some());
    txn.delete(key).await?;
    assert!(txn.select(key).await.is_none());

    let mut txn = Transaction::begin(db.clone());
    assert!(txn.select(key).await.is_none());
    txn.rollback().await?;

    fs::remove_file(&path).unwrap();
    Ok(())
}

#[tokio::test]
async fn test_rollback_effect() -> Result<(), Error> {
    let path = PathBuf::from("test_rollback_effect.db");
    let db = Database::new(path.clone());
    {
        let mut txn = Transaction::begin(db.clone());
        txn.insert(b"temp_key", b"temp_value".to_vec()).await?;
        assert_eq!(txn.select(b"temp_key").await, Some(b"temp_value".to_vec()));
        txn.rollback().await?;
    }
    {
        let mut txn = Transaction::begin(db.clone());
        assert_eq!(txn.select(b"temp_key").await, None);
        txn.rollback().await?;
    }
    fs::remove_file(&path).unwrap();
    Ok(())
}
