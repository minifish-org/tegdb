// examples/sqlite_like_usage.rs
use tegdb::{Database, Result, SqlValue};
use tempfile::NamedTempFile;

fn main() -> Result<()> {
    // Create a temporary database file that will be automatically cleaned up
    let temp_file = NamedTempFile::new().expect("Failed to create temp file");
    let db_path = temp_file.path();
    
    // Create/open database, similar to SQLite
    let mut db = Database::open(db_path)?;
    
    // Create table
    db.execute("CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT NOT NULL, age INTEGER)")?;
    println!("Table created successfully");
    
    // Insert data
    let affected = db.execute("INSERT INTO users (id, name, age) VALUES (1, 'Alice', 30)")?;
    println!("Inserted {} rows", affected);
    
    // Insert more data individually to test
    db.execute("INSERT INTO users (id, name, age) VALUES (2, 'Bob', 25)")?;
    db.execute("INSERT INTO users (id, name, age) VALUES (3, 'Carol', 35)")?;
    println!("Inserted additional users");
    
    // Query data
    let result = db.query("SELECT id, name, age FROM users")?;
    
    println!("Columns: {:?}", result.columns());
    println!("Found {} rows", result.rows().len());
    
    for row in result.rows().iter() {
        let name = match &row[result.columns().iter().position(|c| c == "name").unwrap()] {
            SqlValue::Text(s) => s.clone(),
            _ => "Unknown".to_string(),
        };
        let id = match &row[result.columns().iter().position(|c| c == "id").unwrap()] {
            SqlValue::Integer(i) => *i,
            _ => 0,
        };
        let age = match &row[result.columns().iter().position(|c| c == "age").unwrap()] {
            SqlValue::Integer(i) => *i,
            _ => 0,
        };
        
        println!("User: {} (ID: {}, Age: {})", name, id, age);
    }
    
    // Use transaction with simpler operations
    {
        let mut tx = db.begin_transaction()?;
        tx.execute("UPDATE users SET age = 31 WHERE name = 'Alice'")?;
        tx.execute("DELETE FROM users WHERE name = 'Bob'")?;
        tx.commit()?; // Commit transaction
        println!("Transaction completed");
    }
    
    // Query again to see changes
    let result2 = db.query("SELECT id, name, age FROM users")?;
    println!("After transaction - Found {} rows", result2.rows().len());
    
    // Database file is automatically cleaned up when temp_file goes out of scope
    Ok(())
}
