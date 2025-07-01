use tegdb::{Database, Result};

fn main() -> Result<()> {
    let db_path = "debug_test.db";
    let _ = std::fs::remove_file(db_path);
    
    let mut db = Database::open(db_path)?;
    
    println!("1. Creating table...");
    let affected = db.execute("CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT NOT NULL, age INTEGER)")?;
    println!("   CREATE TABLE affected: {}", affected);
    
    println!("2. Inserting test data...");
    let affected = db.execute("INSERT INTO users (id, name, age) VALUES (1, 'Alice', 30)")?;
    println!("   INSERT affected: {}", affected);
    
    let affected = db.execute("INSERT INTO users (id, name, age) VALUES (2, 'Bob', 25)")?;
    println!("   INSERT affected: {}", affected);
    
    println!("3. Checking data exists...");
    let result = db.query("SELECT * FROM users")?.into_query_result()?;
    println!("   Found {} rows", result.rows().len());
    for (i, row) in result.rows().iter().enumerate() {
        println!("   Row {}: {:?}", i, row);
    }
    
    println!("4. Testing UPDATE...");
    let result = db.query("SELECT * FROM users WHERE name = 'Alice'")?.into_query_result()?;
    println!("   Rows matching WHERE name = 'Alice': {}", result.rows().len());
    
    let affected = db.execute("UPDATE users SET age = 31 WHERE name = 'Alice'")?;
    println!("   UPDATE affected: {}", affected);
    
    println!("5. Verifying UPDATE...");
    let result = db.query("SELECT * FROM users WHERE name = 'Alice'")?.into_query_result()?;
    if !result.rows().is_empty() {
        println!("   Alice's data after update: {:?}", result.rows()[0]);
    }
    
    println!("6. Testing DELETE...");
    let result = db.query("SELECT * FROM users WHERE age < 30")?.into_query_result()?;
    println!("   Rows matching WHERE age < 30: {}", result.rows().len());
    
    let affected = db.execute("DELETE FROM users WHERE age < 30")?;
    println!("   DELETE affected: {}", affected);
    
    println!("7. Final verification...");
    let result = db.query("SELECT * FROM users")?.into_query_result()?;
    println!("   Final row count: {}", result.rows().len());
    
    let _ = std::fs::remove_file(db_path);
    Ok(())
}
