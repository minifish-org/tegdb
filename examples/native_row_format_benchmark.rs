//! Benchmark to demonstrate the native binary row format performance characteristics
//!
//! This benchmark showcases TegDB's native binary row format (SQLite-inspired) capabilities:
//! - Row serialization/deserialization speed
//! - Partial column access (major benefit of native format)
//! - Memory efficiency with compact binary encoding
//! - Table scan performance with LIMIT clauses
//! - Condition evaluation performance

use std::time::Instant;
use tegdb::Database;

fn main() -> tegdb::Result<()> {
    println!("=== TegDB Native Binary Row Format Performance Benchmark ===\n");

    // Test data preparation
    let test_data = generate_test_data(10000);

    println!("Testing with {} rows of sample data", test_data.len());
    println!(
        "Row structure: id (integer), name (text), email (text), score (real), active (integer)\n"
    );

    // Test native format performance
    println!("Testing Native Binary Format Performance...");
    let results = test_storage_format(&test_data)?;

    // Display results
    println!("\n=== PERFORMANCE RESULTS ===");
    print_performance_metric("Database Creation", results.creation_time);
    print_performance_metric("Full Table Insert", results.insert_time);
    print_performance_metric("Full Table Scan", results.full_scan_time);
    print_performance_metric("Selective Column Query", results.selective_scan_time);
    print_performance_metric("Primary Key Lookup", results.pk_lookup_time);
    print_performance_metric("Limited Query (LIMIT 100)", results.limited_scan_time);
    print_performance_metric("Condition-based Query", results.condition_query_time);

    println!("\n=== MEMORY USAGE ===");
    println!("Database size: ~{} bytes", results.db_size_estimate);
    let bytes_per_row = results.db_size_estimate as f64 / test_data.len() as f64;
    println!("Average bytes per row: ~{bytes_per_row:.1} bytes");

    println!("\n=== PERFORMANCE ANALYSIS ===");

    // Analyze query performance characteristics
    let full_scan_ms = results.full_scan_time as f64 / 1_000_000.0;
    let selective_scan_ms = results.selective_scan_time as f64 / 1_000_000.0;
    let selective_improvement = full_scan_ms / selective_scan_ms;

    println!("• Selective column queries: {selective_improvement:.1}x faster than full table scan");
    println!("  Benefit: Avoiding full row deserialization for unused columns");

    let limited_scan_ms = results.limited_scan_time as f64 / 1_000_000.0;
    let limited_improvement = full_scan_ms / limited_scan_ms;
    println!("• Limited queries (LIMIT): {limited_improvement:.1}x faster than full scan");
    println!("  Benefit: Early termination and efficient row filtering");

    let pk_lookup_ms = results.pk_lookup_time as f64 / 1_000_000.0;
    let pk_improvement = full_scan_ms / pk_lookup_ms;
    println!("• Primary key lookups: {pk_improvement:.1}x faster than full scan");
    println!("  Benefit: Direct row access without scanning");

    let condition_ms = results.condition_query_time as f64 / 1_000_000.0;
    let condition_improvement = full_scan_ms / condition_ms;
    println!("• Condition-based queries: {condition_improvement:.1}x faster than full scan");
    println!("  Benefit: Fast condition evaluation without full row reconstruction");

    println!("\n=== NATIVE FORMAT BENEFITS ===");
    println!("✓ Compact binary encoding reduces storage space");
    println!("✓ Direct column access without full deserialization");
    println!("✓ Efficient condition evaluation on binary data");
    println!("✓ SQLite-inspired design for proven performance");
    println!("✓ Variable-length encoding for space efficiency");

    println!("\n=== THROUGHPUT ANALYSIS ===");
    let insert_rate = test_data.len() as f64 / (results.insert_time as f64 / 1_000_000_000.0);
    let scan_rate = test_data.len() as f64 / (results.full_scan_time as f64 / 1_000_000_000.0);

    println!("Insert throughput: ~{insert_rate:.0} rows/second");
    println!("Full scan throughput: ~{scan_rate:.0} rows/second");

    Ok(())
}

#[derive(Debug)]
struct BenchmarkResults {
    creation_time: u128,        // nanoseconds
    insert_time: u128,          // nanoseconds
    full_scan_time: u128,       // nanoseconds
    selective_scan_time: u128,  // nanoseconds
    pk_lookup_time: u128,       // nanoseconds
    limited_scan_time: u128,    // nanoseconds
    condition_query_time: u128, // nanoseconds
    db_size_estimate: usize,    // bytes
}

fn test_storage_format(
    test_data: &[(i64, String, String, f64, i64)],
) -> tegdb::Result<BenchmarkResults> {
    let db_path = "benchmark_native.db";

    // Clean up any existing database
    let _ = std::fs::remove_file(db_path);

    let start = Instant::now();

    // Create database (now always uses native format)
    let mut db = Database::open(format!("file://{db_path}"))?;
    let creation_time = start.elapsed().as_nanos();

    // Create table
    db.execute("CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT(32), email TEXT(32), score REAL, active INTEGER)")?;

    // Insert test data
    let insert_start = Instant::now();

    for (id, name, email, score, active) in test_data.iter() {
        db.execute(&format!(
            "INSERT INTO users (id, name, email, score, active) VALUES ({id}, '{name}', '{email}', {score}, {active})"
        ))?;
    }

    let insert_time = insert_start.elapsed().as_nanos();

    // Test 1: Full table scan
    let full_scan_start = Instant::now();
    let _full_result = db.query("SELECT * FROM users").unwrap();
    let full_scan_time = full_scan_start.elapsed().as_nanos();

    // Test 2: Selective column scan (major optimization for native format)
    let selective_start = Instant::now();
    let _selective_result = db.query("SELECT name, score FROM users").unwrap();
    let selective_scan_time = selective_start.elapsed().as_nanos();

    // Test 3: Primary key lookup
    let pk_start = Instant::now();
    let _pk_result = db
        .query("SELECT name, email FROM users WHERE id = 5000")
        .unwrap();
    let pk_lookup_time = pk_start.elapsed().as_nanos();

    // Test 4: Limited scan (should benefit from early termination)
    let limited_start = Instant::now();
    let _limited_result = db.query("SELECT name, score FROM users LIMIT 100").unwrap();
    let limited_scan_time = limited_start.elapsed().as_nanos();

    // Test 5: Condition-based query
    let condition_start = Instant::now();
    let _condition_result = db
        .query("SELECT name, score FROM users WHERE score > 70.0")
        .unwrap();
    let condition_query_time = condition_start.elapsed().as_nanos();

    // Estimate database size
    let db_size_estimate = std::fs::metadata(db_path)
        .map(|m| m.len() as usize)
        .unwrap_or(0);

    // Clean up
    drop(db);
    let _ = std::fs::remove_file(db_path);

    Ok(BenchmarkResults {
        creation_time,
        insert_time,
        full_scan_time,
        selective_scan_time,
        pk_lookup_time,
        limited_scan_time,
        condition_query_time,
        db_size_estimate,
    })
}

fn generate_test_data(count: usize) -> Vec<(i64, String, String, f64, i64)> {
    let mut data = Vec::with_capacity(count);

    for i in 0..count {
        let id = i as i64;
        let name = format!("User{i:05}");
        let email = format!("user{i}@example.com");
        let score = 50.0 + (i % 50) as f64 + (i as f64 * 0.01) % 1.0; // Realistic score range
        let active = if i % 3 == 0 { 1 } else { 0 }; // Mix of active/inactive

        data.push((id, name, email, score, active));
    }

    data
}

fn print_performance_metric(operation: &str, time_ns: u128) {
    let time_ms = time_ns as f64 / 1_000_000.0;
    println!("{operation:<25} | {time_ms:>8.2}ms");
}
