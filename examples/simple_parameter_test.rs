//! Simple test to verify parameter indexing
//!
//! This example tests if the basic parameter indexing is working.

use tegdb::{Database, Result, SqlValue};
use tempfile::NamedTempFile;

fn main() -> Result<()> {
    println!("=== Simple Parameter Test ===\n");

    // Create a temporary file for the database
    let temp_file = NamedTempFile::new().expect("Failed to create temp file");
    let db_path = temp_file.path();

    // Open database
    let mut db = Database::open(format!("file://{}", db_path.display()))?;
    println!("✓ Database opened");

    // Create table
    db.execute("CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT(32), age INTEGER)")?;
    println!("✓ Table created");

    // Insert test data
    db.execute("INSERT INTO users (id, name, age) VALUES (1, 'Alice', 30)")?;
    db.execute("INSERT INTO users (id, name, age) VALUES (2, 'Bob', 25)")?;
    println!("✓ Test data inserted");

    println!("\n1. Testing simple SELECT with parameter...");

    // Test with a simple query
    let stmt = db.prepare("SELECT * FROM users WHERE id = ?")?;
    println!("   → Prepared statement: {}", stmt.sql());
    println!("   → Parameter count: {}", stmt.parameter_count());

    let params = vec![SqlValue::Integer(1)];
    let result = db.query_prepared(&stmt, &params)?;
    println!("   → Query result: {} rows", result.rows().len());
    for row in result.rows() {
        println!("     {row:?}");
    }

    println!("\n2. Testing simple INSERT with parameters...");

    let insert_stmt = db.prepare("INSERT INTO users (id, name, age) VALUES (?, ?, ?)")?;
    println!("   → Prepared statement: {}", insert_stmt.sql());
    println!("   → Parameter count: {}", insert_stmt.parameter_count());

    let insert_params = vec![
        SqlValue::Integer(3),
        SqlValue::Text("Charlie".to_string()),
        SqlValue::Integer(35),
    ];

    let affected = db.execute_prepared(&insert_stmt, &insert_params)?;
    println!("   → Insert affected: {affected} rows");

    println!("\n3. Final verification...");

    let final_result = db.query("SELECT * FROM users")?;
    println!("   → Final data:");
    for row in final_result.rows() {
        println!("     {row:?}");
    }

    Ok(())
}
