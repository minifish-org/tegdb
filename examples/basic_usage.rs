use std::path::PathBuf;
use tegdb::Engine;

#[tokio::main]
#[cfg(not(test))]
async fn main() {
    let path = PathBuf::from("test.db");
    let mut engine = Engine::new(path.clone());

    // Set a value
    let key = b"key";
    let value = b"value";
    if let Err(e) = engine.set(key, value.to_vec()).await {
        eprintln!("Error setting value: {}", e);
    }

    // Get a value
    match engine.get(key).await {
        Some(get_value) => println!("Got value: {}", String::from_utf8_lossy(&get_value)),
        None => println!("Key not found"),
    }

    // Delete a value
    if let Err(e) = engine.del(key).await {
        eprintln!("Error deleting value: {}", e);
    }

    // Scan for values
    match engine.scan(b"a".to_vec()..b"z".to_vec()).await {
        Ok(values) => {
            for (key, value) in values {
                println!(
                    "Got key: {}, value: {}",
                    String::from_utf8_lossy(&key),
                    String::from_utf8_lossy(&value)
                );
            }
        }
        Err(e) => eprintln!("Error scanning values: {}", e),
    }

    // Clean up
    drop(engine);
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_main() {
        main().await;
    }
}
