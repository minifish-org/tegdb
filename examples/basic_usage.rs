use std::{fs, path::PathBuf};
use tegdb::Engine;

/// Demonstrates basic CRUD operations using TegDB.
#[tokio::main]
async fn main() {
    // Initialize the database engine using a file-based store.
    let path = PathBuf::from("test.db");
    let engine = Engine::new(path.clone());

    // Store a key-value pair.
    let key = b"key";
    let value = b"value";
    if let Err(e) = engine.set(key, value.to_vec()).await {
        eprintln!("Error setting value: {}", e);
    }

    // Retrieve and print the value for the provided key.
    match engine.get(key).await {
        Some(get_value) => println!("Got value: {}", String::from_utf8_lossy(&get_value)),
        None => println!("Key not found"),
    }

    // Delete the key-value pair.
    if let Err(e) = engine.del(key).await {
        eprintln!("Error deleting value: {}", e);
    }

    // Scan and print a range of key-value pairs.
    match engine.scan(b"a".to_vec()..b"z".to_vec()).await {
        Ok(values) => {
            for (key, value) in values {
                println!(
                    "Key: {}, Value: {}",
                    String::from_utf8_lossy(&key),
                    String::from_utf8_lossy(&value)
                );
            }
        }
        Err(e) => eprintln!("Error scanning values: {}", e),
    }

    // Engine cleanup is handled automatically when it goes out of scope.
    drop(engine);
    fs::remove_dir_all(&path).unwrap();
}
