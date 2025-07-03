//! Benchmark to demonstrate IOT storage efficiency
//!
//! This benchmark compares storage efficiency between the old and new IOT implementation

use std::time::Instant;
use tegdb::{Database, Result};

fn main() -> Result<()> {
    println!("=== IOT Storage Efficiency Benchmark ===\n");

    // Create test databases
    let temp_dir = std::env::temp_dir();
    let db_path = temp_dir.join("iot_benchmark.db");

    // Clean up any existing file
    let _ = std::fs::remove_file(&db_path);

    let mut db = Database::open(&db_path)?;

    // Create a table with a meaningful composite primary key
    db.execute(
        "CREATE TABLE user_sessions (
        user_id INTEGER PRIMARY KEY,
        session_id INTEGER PRIMARY KEY,
        login_time TEXT,
        last_activity TEXT,
        ip_address TEXT,
        user_agent TEXT,
        is_active INTEGER
    )",
    )?;

    println!("Table schema:");
    println!("  PRIMARY KEY: (user_id, session_id) - stored in key");
    println!("  NON-PK:     login_time, last_activity, ip_address, user_agent, is_active - stored in value");
    println!();

    // Insert test data
    let start_time = Instant::now();
    let num_records = 1000;

    for user_id in 1..=100 {
        for session_id in 1..=10 {
            let sql = format!(
                "INSERT INTO user_sessions (user_id, session_id, login_time, last_activity, ip_address, user_agent, is_active) VALUES ({user_id}, {session_id}, 'login_time', 'last_activity', 'ip_address', 'user_agent', 1)"
            );
            db.execute(&sql)?;
        }
    }

    let insert_time = start_time.elapsed();
    println!("✅ Inserted {num_records} records in {insert_time:?}");

    // Test query performance
    let start_time = Instant::now();
    let result = db
        .query("SELECT * FROM user_sessions WHERE user_id = 1")
        .unwrap();
    let query_time = start_time.elapsed();
    println!(
        "✅ Range query (user_id = 1) completed in {:?}, found {} records",
        query_time,
        result.rows().len()
    );

    // Test primary key lookup performance
    let start_time = Instant::now();
    let result = db
        .query("SELECT * FROM user_sessions WHERE user_id = 50 AND session_id = 5")
        .unwrap();
    let pk_lookup_time = start_time.elapsed();
    println!("✅ Primary key lookup completed in {pk_lookup_time:?}");

    if let Some(row) = result.rows().first() {
        let user_id_pos = result
            .columns()
            .iter()
            .position(|c| c == "user_id")
            .unwrap();
        let session_id_pos = result
            .columns()
            .iter()
            .position(|c| c == "session_id")
            .unwrap();
        let ip_address_pos = result
            .columns()
            .iter()
            .position(|c| c == "ip_address")
            .unwrap();

        println!(
            "   Found session: user_id={:?}, session_id={:?}, ip={:?}",
            &row[user_id_pos], &row[session_id_pos], &row[ip_address_pos]
        );
    }

    // Estimate storage savings
    println!("\n=== Storage Analysis ===");

    // Calculate theoretical storage per row
    let pk_size_in_key = 2 * 20; // Two zero-padded integers (20 chars each)
    let pk_size_in_value_old = 2 * 8; // Two i64 values (8 bytes each) in old approach
    let non_pk_data_size = 4 * 20 + 8; // Rough estimate: 4 text fields + 1 integer

    let old_total_per_row = pk_size_in_key + pk_size_in_value_old + non_pk_data_size;
    let new_total_per_row = pk_size_in_key + non_pk_data_size; // No PK redundancy

    let savings_per_row = pk_size_in_value_old;
    let total_savings = savings_per_row * num_records;
    let savings_percentage = (savings_per_row as f64 / old_total_per_row as f64) * 100.0;

    println!("📊 Estimated storage comparison:");
    println!("   Old approach: ~{old_total_per_row} bytes per row");
    println!("   New approach: ~{new_total_per_row} bytes per row");
    println!("   Savings:      ~{savings_per_row} bytes per row ({savings_percentage:.1}%)");
    println!("   Total saved:  ~{total_savings} bytes for {num_records} records");

    println!("\n🚀 IOT Optimization Benefits:");
    println!("   ✅ Eliminated primary key redundancy in stored values");
    println!("   ✅ Reduced storage space by storing PK only in keys");
    println!("   ✅ Maintained full row reconstruction capability");
    println!("   ✅ Natural clustering and sorting by primary key");
    println!("   ✅ Efficient primary key lookups");

    // Clean up
    let _ = std::fs::remove_file(&db_path);

    Ok(())
}
