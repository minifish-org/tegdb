//! Example demonstrating TegDB's execution plan caching in prepared statements
//!
//! This example shows that the execution plan is generated once at prepare time
//! and reused for all executions, improving performance for repeated queries.
//!
//! Run with: cargo run --example prepared_statement_plan_caching_demo

use std::time::Instant;
use tegdb::{Database, Result, SqlValue};
use tempfile::NamedTempFile;

fn main() -> Result<()> {
    println!("=== TegDB Prepared Statement Plan Caching Demo ===\n");

    // Create a temporary file for the database
    let temp_file = NamedTempFile::new().expect("Failed to create temp file");
    let db_path = temp_file.path();

    // Open database
    let mut db = Database::open(format!("file://{}", db_path.display()))?;
    println!("✓ Database opened");

    // Create table with enough data to make planning meaningful
    db.execute(
        "CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT(32), age INTEGER, city TEXT(32))",
    )?;
    println!("✓ Table created");

    // Insert test data
    for i in 1..=1000 {
        let city = if i % 3 == 0 {
            "New York"
        } else if i % 3 == 1 {
            "Los Angeles"
        } else {
            "Chicago"
        };
        db.execute(&format!(
            "INSERT INTO users (id, name, age, city) VALUES ({}, 'User{}', {}, '{}')",
            i,
            i,
            20 + (i % 50),
            city
        ))?;
    }
    println!("✓ Inserted 1000 test rows");

    println!("\n1. Testing plan caching with SELECT statements...");

    // Prepare a SELECT statement with parameters
    let select_stmt = db.prepare("SELECT name, age FROM users WHERE age > ? AND city = ?")?;
    println!("   → Prepared statement: {}", select_stmt.sql());
    println!("   → Parameter count: {}", select_stmt.parameter_count());

    // Execute the same prepared statement multiple times with different parameters
    let test_cases = vec![
        (30, "New York"),
        (25, "Los Angeles"),
        (35, "Chicago"),
        (40, "New York"),
        (20, "Los Angeles"),
    ];

    for (age, city) in test_cases {
        let start = Instant::now();
        let params = vec![SqlValue::Integer(age), SqlValue::Text(city.to_string())];
        let result = db.query_prepared(&select_stmt, &params)?;
        let duration = start.elapsed();

        println!(
            "   → age > {} AND city = '{}': {} rows in {:?}",
            age,
            city,
            result.rows().len(),
            duration
        );
    }

    println!("\n2. Testing plan caching with INSERT statements...");

    // Prepare an INSERT statement
    let insert_stmt = db.prepare("INSERT INTO users (id, name, age, city) VALUES (?, ?, ?, ?)")?;
    println!("   → Prepared statement: {}", insert_stmt.sql());

    // Execute multiple inserts
    for i in 1001..=1010 {
        let start = Instant::now();
        let params = vec![
            SqlValue::Integer(i),
            SqlValue::Text(format!("CachedUser{i}")),
            SqlValue::Integer(25 + (i % 20)),
            SqlValue::Text("CachedCity".to_string()),
        ];
        let affected = db.execute_prepared(&insert_stmt, &params)?;
        let duration = start.elapsed();

        println!("   → Insert {i}: {affected} rows affected in {duration:?}");
    }

    println!("\n3. Testing plan caching with UPDATE statements...");

    // Prepare an UPDATE statement
    let update_stmt = db.prepare("UPDATE users SET age = ? WHERE name = ?")?;
    println!("   → Prepared statement: {}", update_stmt.sql());

    // Execute multiple updates
    for i in 1..=5 {
        let start = Instant::now();
        let params = vec![
            SqlValue::Integer(30 + i),
            SqlValue::Text(format!("User{}", i * 100)),
        ];
        let affected = db.execute_prepared(&update_stmt, &params)?;
        let duration = start.elapsed();

        println!("   → Update {i}: {affected} rows affected in {duration:?}");
    }

    println!("\n4. Testing plan caching with DELETE statements...");

    // Prepare a DELETE statement
    let delete_stmt = db.prepare("DELETE FROM users WHERE age < ?")?;
    println!("   → Prepared statement: {}", delete_stmt.sql());

    // Execute multiple deletes
    for age in [25, 30, 35] {
        let start = Instant::now();
        let params = vec![SqlValue::Integer(age)];
        let affected = db.execute_prepared(&delete_stmt, &params)?;
        let duration = start.elapsed();

        println!("   → DELETE age < {age}: {affected} rows affected in {duration:?}");
    }

    println!("\n5. Final verification...");

    // Query to see final state
    let final_result = db.query("SELECT COUNT(*) FROM users")?;
    let count = if let Some(SqlValue::Integer(count)) =
        final_result.rows().first().and_then(|row| row.first())
    {
        *count
    } else {
        0
    };
    println!("   → Final user count: {count}");

    println!("\n✓ Plan caching demo completed successfully!");
    println!("   → The execution plan was generated once per prepared statement");
    println!("   → All subsequent executions reused the cached plan");
    println!("   → This demonstrates the RBO (Rule-Based Optimizer) behavior");

    Ok(())
}
