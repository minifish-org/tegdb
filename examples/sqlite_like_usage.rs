// examples/sqlite_like_usage.rs
use tegdb::{Database, Result, SqlValue};

fn main() -> Result<()> {
    // Use a temporary path with .teg extension
    let db_path = std::env::temp_dir().join("sqlite_like_usage.teg");
    let _ = std::fs::remove_file(&db_path);

    // Create/open database, similar to SQLite
    let mut db = Database::open(format!("file://{}", db_path.display()))?;

    // Create table
    db.execute("CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT(32) NOT NULL, age INTEGER)")?;
    println!("Table created successfully");

    // Insert data
    let affected = db.execute("INSERT INTO users (id, name, age) VALUES (1, 'Alice', 30)")?;
    println!("Inserted {affected} rows");

    // Insert more data individually to test
    db.execute("INSERT INTO users (id, name, age) VALUES (2, 'Bob', 25)")?;
    db.execute("INSERT INTO users (id, name, age) VALUES (3, 'Carol', 35)")?;
    println!("Inserted additional users");

    // Query data
    let result = db.query("SELECT id, name, age FROM users").unwrap();

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

        println!("User: {name} (ID: {id}, Age: {age})");
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
    let result2 = db.query("SELECT id, name, age FROM users").unwrap();
    println!("After transaction - Found {} rows", result2.rows().len());

    // Clean up
    let _ = std::fs::remove_file(&db_path);
    Ok(())
}
