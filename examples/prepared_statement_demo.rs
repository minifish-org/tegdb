//! Example demonstrating TegDB's prepare/execute protocol
//!
//! This example shows how to use prepared statements with parameterized queries,
//! similar to SQLite's prepare/execute pattern.
//!
//! Run with: cargo run --example prepared_statement_demo

use tegdb::{Database, Result, SqlValue};
use tempfile::NamedTempFile;

fn main() -> Result<()> {
    println!("=== TegDB Prepared Statement Demo ===\n");

    // Create a temporary file for the database
    let temp_file = NamedTempFile::new().expect("Failed to create temp file");
    let db_path = temp_file.path();

    // Open database
    let mut db = Database::open(format!("file://{}", db_path.display()))?;
    println!("✓ Database opened");

    // Create table
    db.execute(
        "CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT(32), age INTEGER, city TEXT(32))",
    )?;
    println!("✓ Table created");

    // Insert some test data
    db.execute("INSERT INTO users (id, name, age, city) VALUES (1, 'Alice', 30, 'New York')")?;
    db.execute("INSERT INTO users (id, name, age, city) VALUES (2, 'Bob', 25, 'Los Angeles')")?;
    db.execute("INSERT INTO users (id, name, age, city) VALUES (3, 'Carol', 35, 'Chicago')")?;
    db.execute("INSERT INTO users (id, name, age, city) VALUES (4, 'David', 28, 'New York')")?;
    db.execute("INSERT INTO users (id, name, age, city) VALUES (5, 'Eve', 32, 'Boston')")?;
    println!("✓ Test data inserted");

    println!("\n1. Preparing SELECT statement with parameters...");

    // Prepare a SELECT statement with parameters
    let select_stmt = db.prepare("SELECT name, age FROM users WHERE age > ? AND city = ?")?;
    println!("   → Prepared statement: {}", select_stmt.sql());
    println!("   → Parameter count: {}", select_stmt.parameter_count());

    // Execute the prepared statement with different parameters
    println!("\n2. Executing prepared SELECT with parameters...");

    let params1 = vec![
        SqlValue::Integer(30),
        SqlValue::Text("New York".to_string()),
    ];
    let result1 = db.query_prepared(&select_stmt, &params1)?;
    println!("   → Query: age > 30 AND city = 'New York'");
    println!("   → Found {} rows", result1.rows().len());
    for row in result1.rows() {
        println!("     {row:?}");
    }

    let params2 = vec![
        SqlValue::Integer(25),
        SqlValue::Text("Los Angeles".to_string()),
    ];
    let result2 = db.query_prepared(&select_stmt, &params2)?;
    println!("   → Query: age > 25 AND city = 'Los Angeles'");
    println!("   → Found {} rows", result2.rows().len());
    for row in result2.rows() {
        println!("     {row:?}");
    }

    println!("\n3. Preparing INSERT statement with parameters...");

    // Prepare an INSERT statement with parameters
    let insert_stmt = db.prepare("INSERT INTO users (id, name, age, city) VALUES (?, ?, ?, ?)")?;
    println!("   → Prepared statement: {}", insert_stmt.sql());
    println!("   → Parameter count: {}", insert_stmt.parameter_count());

    // Execute the prepared INSERT statement
    let insert_params = vec![
        SqlValue::Integer(6),
        SqlValue::Text("Frank".to_string()),
        SqlValue::Integer(29),
        SqlValue::Text("Seattle".to_string()),
    ];
    let affected = db.execute_prepared(&insert_stmt, &insert_params)?;
    println!("   → Inserted {affected} rows");

    println!("\n4. Preparing UPDATE statement with parameters...");

    // Prepare an UPDATE statement with parameters
    let update_stmt = db.prepare("UPDATE users SET age = ? WHERE name = ?")?;
    println!("   → Prepared statement: {}", update_stmt.sql());
    println!("   → Parameter count: {}", update_stmt.parameter_count());

    // Execute the prepared UPDATE statement
    let update_params = vec![SqlValue::Integer(31), SqlValue::Text("Alice".to_string())];
    let affected = db.execute_prepared(&update_stmt, &update_params)?;
    println!("   → Updated {affected} rows");

    println!("\n5. Preparing DELETE statement with parameters...");

    // Prepare a DELETE statement with parameters
    let delete_stmt = db.prepare("DELETE FROM users WHERE age < ?")?;
    println!("   → Prepared statement: {}", delete_stmt.sql());
    println!("   → Parameter count: {}", delete_stmt.parameter_count());

    // Execute the prepared DELETE statement
    let delete_params = vec![SqlValue::Integer(30)];
    let affected = db.execute_prepared(&delete_stmt, &delete_params)?;
    println!("   → Deleted {affected} rows");

    println!("\n6. Final query to see remaining data...");

    // Query to see the final state
    let final_result = db.query("SELECT * FROM users")?;
    println!("   → Remaining users:");
    for row in final_result.rows() {
        println!("     {row:?}");
    }

    println!("\n✓ Prepared statement demo completed successfully!");
    Ok(())
}
