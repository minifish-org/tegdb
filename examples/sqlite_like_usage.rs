// examples/sqlite_like_usage.rs
use tegdb::{Database, Result, parser::SqlValue};

fn main() -> Result<()> {
    // Create/open database, similar to SQLite
    let mut db = Database::open("my_database.db")?;
    
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
    
    for row in result.iter() {
        let name = match row.get("name").unwrap() {
            SqlValue::Text(s) => s.clone(),
            _ => "Unknown".to_string(),
        };
        let id = match row.get("id").unwrap() {
            SqlValue::Integer(i) => *i,
            _ => 0,
        };
        let age = match row.get("age").unwrap() {
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
    
    Ok(())
}
