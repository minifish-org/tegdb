use std::fs;
use std::time::Instant;
use tegdb::{Database, SqlValue};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Ensure a clean database file
    let _ = fs::remove_file("/tmp/native_key_benchmark.db");
    println!("=== Native Key Benchmark ===");

    // Create database with absolute path
    let mut db = Database::open("file:///tmp/native_key_benchmark.teg")?;

    // Clean up any existing data
    let _ = db.execute("DROP TABLE IF EXISTS benchmark_test");

    // Create table with INTEGER PRIMARY KEY
    db.execute("CREATE TABLE benchmark_test (id INTEGER PRIMARY KEY, name TEXT(32), value REAL)")?;

    // Prepare statements
    let insert_stmt =
        db.prepare("INSERT INTO benchmark_test (id, name, value) VALUES (?1, ?2, ?3)")?;
    let select_stmt = db.prepare("SELECT * FROM benchmark_test WHERE id = ?1")?;
    let range_stmt = db.prepare("SELECT * FROM benchmark_test WHERE id >= ?1 AND id <= ?2")?;

    println!("Running benchmark with 10,000 operations...");

    // Benchmark INSERT operations
    let start = Instant::now();
    for i in 1..=10000 {
        db.execute_prepared(
            &insert_stmt,
            &[
                SqlValue::Integer(i),
                SqlValue::Text(format!("item_{i}")),
                SqlValue::Real(i as f64 * 1.5),
            ],
        )?;
    }
    let insert_time = start.elapsed();
    println!("INSERT operations: {insert_time:.2?}");

    // Benchmark SELECT operations
    let start = Instant::now();
    for i in 1..=10000 {
        let result = db.query_prepared(&select_stmt, &[SqlValue::Integer(i)])?;
        assert_eq!(result.rows().len(), 1);
    }
    let select_time = start.elapsed();
    println!("SELECT operations: {select_time:.2?}");

    // Benchmark range scan operations
    let start = Instant::now();
    for i in 0..100 {
        let start_id = i * 100 + 1;
        let end_id = (i + 1) * 100;
        let result = db.query_prepared(
            &range_stmt,
            &[SqlValue::Integer(start_id), SqlValue::Integer(end_id)],
        )?;
        let expected_count = (end_id - start_id + 1) as usize; // Inclusive range
        assert_eq!(
            result.rows().len(),
            expected_count,
            "Range scan failed: expected {} rows for range {} to {}, got {}",
            expected_count,
            start_id,
            end_id,
            result.rows().len()
        );
    }
    let range_time = start.elapsed();
    println!("Range scan operations: {range_time:.2?}");

    // Calculate total time
    let total_time = insert_time + select_time + range_time;
    println!("Total benchmark time: {total_time:.2?}");

    // Show memory savings comparison
    println!("\n=== Memory Savings Analysis ===");

    // Compare string key vs native key sizes
    let string_key_size = "12345".len(); // String representation of key
    let native_key_size = 8; // 8 bytes for i64

    println!("String key size: {string_key_size} bytes");
    println!("Native key size: {native_key_size} bytes");
    println!(
        "   → Memory savings: {:.1}%",
        if string_key_size > native_key_size {
            ((string_key_size - native_key_size) as f64 / string_key_size as f64) * 100.0
        } else {
            0.0
        }
    );

    // Compare binary format sizes
    let string_bytes = "12345".as_bytes();
    let native_bytes = 12345i64.to_be_bytes();

    println!("String binary size: {} bytes", string_bytes.len());
    println!("Native binary size: {} bytes", native_bytes.len());
    println!(
        "   → Binary format savings: {:.1}%",
        if string_bytes.len() > native_bytes.len() {
            ((string_bytes.len() - native_bytes.len()) as f64 / string_bytes.len() as f64) * 100.0
        } else {
            0.0
        }
    );

    println!("\nBenchmark completed successfully!");

    Ok(())
}
