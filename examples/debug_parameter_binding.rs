//! Debug example to test parameter binding step by step
//!
//! This example helps debug the parameter binding issue by testing each step.

use tegdb::{Database, Result, SqlValue};
use tempfile::NamedTempFile;

fn main() -> Result<()> {
    println!("=== TegDB Parameter Binding Debug ===\n");

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
    db.execute("INSERT INTO users (id, name, age) VALUES (3, 'Carol', 35)")?;
    println!("✓ Test data inserted");

    println!("\n1. Testing simple parameter binding...");

    // Test with a simple query that should work
    let simple_stmt = db.prepare("SELECT * FROM users WHERE id = ?")?;
    println!("   → Prepared statement: {}", simple_stmt.sql());
    println!("   → Parameter count: {}", simple_stmt.parameter_count());

    let params = vec![SqlValue::Integer(1)];
    let result = db.query_prepared(&simple_stmt, &params)?;
    println!("   → Query result: {} rows", result.rows().len());
    for row in result.rows() {
        println!("     {row:?}");
    }

    println!("\n2. Testing direct SQL vs prepared statement...");

    // Compare with direct SQL
    let direct_result = db.query("SELECT * FROM users WHERE id = 1")?;
    println!(
        "   → Direct SQL result: {} rows",
        direct_result.rows().len()
    );
    for row in direct_result.rows() {
        println!("     {row:?}");
    }

    println!("\n3. Testing INSERT with parameters...");

    let insert_stmt = db.prepare("INSERT INTO users (id, name, age) VALUES (?, ?, ?)")?;
    println!("   → Prepared statement: {}", insert_stmt.sql());
    println!("   → Parameter count: {}", insert_stmt.parameter_count());

    // Debug: Let's see what the execution plan looks like
    #[cfg(feature = "dev")]
    println!(
        "   → Execution plan: {:?}",
        insert_stmt.debug_execution_plan()
    );

    let insert_params = vec![
        SqlValue::Integer(4),
        SqlValue::Text("Debug".to_string()),
        SqlValue::Integer(40),
    ];
    println!("   → Parameters: {insert_params:?}");

    let affected = db.execute_prepared(&insert_stmt, &insert_params)?;
    println!("   → Insert affected: {affected} rows");

    println!("\n4. Final verification...");

    let final_result = db.query("SELECT * FROM users ORDER BY id")?;
    println!("   → Final data:");
    for row in final_result.rows() {
        println!("     {row:?}");
    }

    Ok(())
}
