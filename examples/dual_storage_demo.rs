use tegdb::{Database, Result};

fn main() -> Result<()> {
    println!("=== TegDB Storage Backend Demo ===\n");

    // Native file backend (default)
    println!("1. Testing file backend...");
    test_file_log_backend()?;

    // Unsupported protocols now return an error
    println!("\n2. Demonstrating unsupported protocol handling...");
    test_unsupported_protocol();

    println!("\n🎉 Storage backend demo completed successfully!");
    Ok(())
}

fn test_file_log_backend() -> Result<()> {
    println!("   Creating file-based database...");
    let mut db = Database::open("file:///tmp/demo_file_log_backend.teg")?;

    println!("   Creating table and inserting data...");
    db.execute("CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT(32), score REAL)")?;
    db.execute("INSERT INTO users (id, name, score) VALUES (1, 'Alice', 95.5)")?;
    db.execute("INSERT INTO users (id, name, score) VALUES (2, 'Bob', 87.2)")?;

    println!("   Querying data...");
    let results = db.query("SELECT name, score FROM users WHERE score > 90")?;
    for row_result in results {
        let row = row_result?;
        if let [name, score] = &row[..] {
            println!("     File: {name:?} - {score:?}");
        }
    }

    println!("   ✓ File backend test completed");
    Ok(())
}

fn test_unsupported_protocol() {
    println!("   Attempting to open localstorage://demo_browser_log_backend");
    match Database::open("localstorage://demo_browser_log_backend") {
        Ok(_) => println!("   ⚠️  Unexpected success - protocol should not be accepted"),
        Err(e) => println!("   ✓ Received expected error: {e}"),
    }
}
