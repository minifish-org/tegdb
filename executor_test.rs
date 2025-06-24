use tegdb::{Database, engine::EngineConfig};
use std::path::PathBuf;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Test the improved executor with validation
    let mut path = std::env::temp_dir();
    path.push("executor_test.db");
    
    // Remove existing file
    let _ = std::fs::remove_file(&path);
    
    // Create database with custom config
    let config = EngineConfig {
        sync_on_write: false, // For faster testing
        ..Default::default()
    };
    
    let mut db = Database::open_with_config(&path, config)?;
    
    println!("Testing improved executor with validation...");
    
    // Test 1: Create table
    db.execute("BEGIN")?;
    let result = db.execute("CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT NOT NULL, email TEXT UNIQUE)")?;
    println!("Create table result: {:?}", result);
    
    // Test 2: Insert with validation
    let result = db.execute("INSERT INTO users (id, name, email) VALUES (1, 'Alice', 'alice@example.com')")?;
    println!("Insert result: {:?}", result);
    
    // Test 3: Try to insert duplicate primary key (should fail)
    match db.execute("INSERT INTO users (id, name, email) VALUES (1, 'Bob', 'bob@example.com')") {
        Ok(_) => println!("ERROR: Duplicate primary key should have failed!"),
        Err(e) => println!("✓ Primary key constraint validation works: {}", e),
    }
    
    // Test 4: Try to insert missing NOT NULL field (should fail)
    match db.execute("INSERT INTO users (id, email) VALUES (2, 'charlie@example.com')") {
        Ok(_) => println!("ERROR: Missing NOT NULL field should have failed!"),
        Err(e) => println!("✓ NOT NULL constraint validation works: {}", e),
    }
    
    // Test 5: Try to insert duplicate UNIQUE field (should fail)
    match db.execute("INSERT INTO users (id, name, email) VALUES (3, 'David', 'alice@example.com')") {
        Ok(_) => println!("ERROR: Duplicate UNIQUE field should have failed!"),
        Err(e) => println!("✓ UNIQUE constraint validation works: {}", e),
    }
    
    // Test 6: Valid insert
    let result = db.execute("INSERT INTO users (id, name, email) VALUES (2, 'Bob', 'bob@example.com')")?;
    println!("Valid insert result: {:?}", result);
    
    // Test 7: Select with LIMIT (memory optimization)
    let result = db.execute("SELECT * FROM users LIMIT 1")?;
    println!("Select with LIMIT result: {:?}", result);
    
    // Test 8: Commit
    let result = db.execute("COMMIT")?;
    println!("Commit result: {:?}", result);
    
    println!("All tests completed successfully!");
    
    // Cleanup
    let _ = std::fs::remove_file(&path);
    
    Ok(())
}
