use tegdb::{Database, Result};
use tempfile::NamedTempFile;

fn main() -> Result<()> {
    let temp_file = NamedTempFile::new().expect("Failed to create temp file");
    let db_path = temp_file.path().to_path_buf();

    {
        let mut db = Database::open(format!("file://{}", db_path.display()))?;

        println!("Creating table...");
        db.execute(
            "CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT(32) NOT NULL, age INTEGER)",
        )?;
    } // Database is closed here

    // Now let's check the storage directly
    println!("Checking what's actually stored in the database...");

    // Use the engine directly to see what's stored
    use tegdb::storage_engine::StorageEngine;
    let engine = StorageEngine::new(db_path)?;

    let schema_prefix = "S:".as_bytes().to_vec();
    let schema_end = "S~".as_bytes().to_vec();

    let scan_results: Vec<_> = engine.scan(schema_prefix..schema_end)?.collect();

    for (key, value) in scan_results {
        let key_str = String::from_utf8_lossy(&key);
        let value_str = String::from_utf8_lossy(&value);
        println!("Key: {key_str}");
        println!("Value: {value_str}");
    }

    Ok(())
}
