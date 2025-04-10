use std::path::PathBuf;
use tegdb::Database; // adjust module path as necessary

#[tokio::main]
async fn main() {
    // Initialize database with a file path.
    let db_path = PathBuf::from("data.db");
    let db = Database::new(db_path).await;

    // Start Transaction API.
    let mut tx = db.new_transaction().await; // updated

    // INSERT using Transaction API.
    tx.insert(b"key1", b"value1".to_vec()).await.unwrap();
    println!("Inserted key1 -> value1");

    // UPDATE using Transaction API.
    tx.update(b"key1", b"value2".to_vec()).await.unwrap();
    println!("Updated key1 -> value2");

    // SELECT using Transaction API.
    let val = tx.select(b"key1").await.unwrap();
    println!("Selected key1 -> {:?}", val);

    // DELETE using Transaction API.
    tx.delete(b"key1").await.unwrap();
    println!("Deleted key1");

    // Commit transaction.
    tx.commit().await.unwrap();

    //println!("Dropping database now.");
    db.shutdown().await;
}
