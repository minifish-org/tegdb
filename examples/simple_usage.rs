//! Simple usage example demonstrating TegDB's streamlined API
//!
//! This example shows how easy it is to use TegDB with the simplified API:
//! - No configuration needed - just open and use
//! - Native binary format used automatically for optimal performance
//! - Clean, SQLite-like interface

use tegdb::Database;

fn main() -> tegdb::Result<()> {
    println!("=== TegDB Simple Usage Example ===\n");

    // Clean up any existing database
    let _ = std::fs::remove_file("simple_example.db");

    // 1. Open database (that's it - no configuration needed!)
    println!("1. Opening database...");
    let mut db = Database::open("simple_example.db")?;
    println!("   ✓ Database opened with native binary format");

    // 2. Create table
    println!("2. Creating table...");
    db.execute("CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT, score REAL)")?;
    println!("   ✓ Table created");

    // 3. Insert data
    println!("3. Inserting data...");
    db.execute("INSERT INTO users (id, name, score) VALUES (1, 'Alice', 95.5)")?;
    db.execute("INSERT INTO users (id, name, score) VALUES (2, 'Bob', 87.2)")?;
    db.execute("INSERT INTO users (id, name, score) VALUES (3, 'Carol', 92.8)")?;
    println!("   ✓ Data inserted");

    // 4. Query data
    println!("4. Querying data...");
    let results = db
        .query("SELECT name, score FROM users WHERE score > 90.0 ORDER BY score DESC")
        .unwrap()
        .into_query_result()
        .unwrap();

    println!("   Users with score > 90:");
    for row in results.rows() {
        if let (Some(name), Some(score)) = (row.get(0), row.get(1)) {
            println!("     {:?} - {:?}", name, score);
        }
    }

    // 5. Update data
    println!("5. Updating data...");
    let affected = db.execute("UPDATE users SET score = 89.2 WHERE name = 'Bob'")?;
    println!("   ✓ Updated {} row", affected);

    // 6. Final query
    println!("6. Final results...");
    let final_results = db
        .query("SELECT name, score FROM users ORDER BY name")
        .unwrap()
        .into_query_result()
        .unwrap();

    println!("   All users:");
    for row in final_results.rows() {
        if let (Some(name), Some(score)) = (row.get(0), row.get(1)) {
            println!("     {:?} - {:?}", name, score);
        }
    }

    println!("\n🎉 All operations completed successfully!");
    println!("💡 Notice how simple the API is:");
    println!("   - Just `Database::open()` - no configuration needed");
    println!("   - Native binary format used automatically");
    println!("   - SQLite-like interface for familiar usage");

    // Clean up
    let _ = std::fs::remove_file("simple_example.db");

    Ok(())
}
