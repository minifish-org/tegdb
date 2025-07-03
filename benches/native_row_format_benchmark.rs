//! Benchmark to demonstrate TegDB's native binary row format performance
//!
//! This benchmark showcases the performance characteristics of TegDB's
//! SQLite-inspired native binary row format, focusing on:
//! - Row serialization/deserialization speed
//! - Partial column access (major optimization)
//! - Memory efficiency with compact binary encoding
//! - Table scan performance with various query patterns

use std::time::Instant;
use tegdb::Database;

fn main() -> tegdb::Result<()> {
    println!("=== TegDB Native Binary Row Format Performance Benchmark ===\n");

    // Test data preparation
    let test_data = generate_test_data(15000);

    println!("Testing with {} rows of sample data", test_data.len());
    println!(
        "Row structure: id (integer), name (text), email (text), score (real), active (integer)\n"
    );

    // Test the native format performance
    println!("Testing TegDB with Native Binary Row Format...");
    let results = test_native_format(&test_data)?;

    // Display results
    println!("\n=== PERFORMANCE RESULTS ===");
    print_performance_metric("Database Creation", results.creation_time);
    print_performance_metric("Bulk Insert (15k rows)", results.insert_time);
    print_performance_metric("Full Table Scan (*)", results.full_scan_time);
    print_performance_metric(
        "Selective Columns (name, score)",
        results.selective_scan_time,
    );
    print_performance_metric("Primary Key Lookup", results.pk_lookup_time);
    print_performance_metric("LIMIT Query (100 rows)", results.limited_scan_time);
    print_performance_metric("Filtered Query (WHERE)", results.filtered_scan_time);

    println!("\n=== STORAGE EFFICIENCY ===");
    println!("Database size: {} bytes", results.db_size_estimate);
    println!(
        "Average bytes per row: {:.1}",
        results.db_size_estimate as f64 / test_data.len() as f64
    );
    println!(
        "Compression ratio: {:.1}:1",
        estimate_uncompressed_size(&test_data) as f64 / results.db_size_estimate as f64
    );

    println!("\n=== NATIVE FORMAT BENEFITS ===");
    println!("✓ Direct column access without full row deserialization");
    println!("✓ Variable-length integer encoding saves space");
    println!("✓ Compact binary representation (no JSON/HashMap overhead)");
    println!("✓ Fast condition evaluation on binary data");
    println!("✓ Efficient LIMIT query early termination");

    // Performance analysis
    println!("\n=== PERFORMANCE ANALYSIS ===");
    analyze_performance(&results, test_data.len());

    println!("\n🎉 Native binary row format benchmark completed successfully!");

    Ok(())
}

#[derive(Debug)]
struct BenchmarkResults {
    creation_time: u128,       // nanoseconds
    insert_time: u128,         // nanoseconds
    full_scan_time: u128,      // nanoseconds
    selective_scan_time: u128, // nanoseconds
    pk_lookup_time: u128,      // nanoseconds
    limited_scan_time: u128,   // nanoseconds
    filtered_scan_time: u128,  // nanoseconds
    db_size_estimate: usize,   // bytes
}

fn test_native_format(
    test_data: &[(i64, String, String, f64, i64)],
) -> tegdb::Result<BenchmarkResults> {
    let db_path = "benchmark_native.db";

    // Clean up any existing database
    let _ = std::fs::remove_file(db_path);

    let start = Instant::now();

    // Create database (now always uses native format)
    let mut db = Database::open(db_path)?;
    let creation_time = start.elapsed().as_nanos();

    // Create table
    db.execute("CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT, email TEXT, score REAL, active INTEGER)")?;

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
    let full_result = db.query("SELECT * FROM users")?;
    let _full_rows = full_result.rows().to_vec();
    let full_scan_time = full_scan_start.elapsed().as_nanos();

    // Test 2: Selective column scan (major optimization for native format)
    let selective_start = Instant::now();
    let selective_result = db.query("SELECT name, score FROM users")?;
    let _selective_rows = selective_result.rows().to_vec();
    let selective_scan_time = selective_start.elapsed().as_nanos();

    // Test 3: Primary key lookup
    let pk_start = Instant::now();
    let pk_result = db.query("SELECT name, email FROM users WHERE id = 7500")?;
    let _pk_rows = pk_result.rows().to_vec();
    let pk_lookup_time = pk_start.elapsed().as_nanos();

    // Test 4: Limited scan (should benefit from early termination)
    let limited_start = Instant::now();
    let limited_result = db.query("SELECT name, score FROM users LIMIT 100")?;
    let _limited_rows = limited_result.rows().to_vec();
    let limited_scan_time = limited_start.elapsed().as_nanos();

    // Test 5: Filtered scan
    let filtered_start = Instant::now();
    let filtered_result = db.query("SELECT name FROM users WHERE active = 1")?;
    let _filtered_rows = filtered_result.rows().to_vec();
    let filtered_scan_time = filtered_start.elapsed().as_nanos();

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
        filtered_scan_time,
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
    println!("{operation:<30} | {time_ms:>10.2} ms");
}

fn estimate_uncompressed_size(test_data: &[(i64, String, String, f64, i64)]) -> usize {
    // Rough estimate of uncompressed data size
    let mut total_size = 0;
    for (_, name, email, _, _) in test_data {
        total_size += 8; // id (i64)
        total_size += name.len();
        total_size += email.len();
        total_size += 8; // score (f64)
        total_size += 8; // active (i64)
        total_size += 32; // overhead estimate
    }
    total_size
}

fn analyze_performance(results: &BenchmarkResults, row_count: usize) {
    let rows_per_second_insert =
        (row_count as f64) / (results.insert_time as f64 / 1_000_000_000.0);
    let rows_per_second_scan =
        (row_count as f64) / (results.full_scan_time as f64 / 1_000_000_000.0);

    println!("• Insert throughput: {rows_per_second_insert:.0} rows/second");
    println!("• Scan throughput: {rows_per_second_scan:.0} rows/second");

    // Analyze selective vs full scan efficiency
    let selective_efficiency = results.full_scan_time as f64 / results.selective_scan_time as f64;
    println!("• Selective column scan is {selective_efficiency:.1}x faster than full scan");

    // Analyze memory efficiency
    let bytes_per_row = results.db_size_estimate as f64 / row_count as f64;
    println!("• Storage efficiency: {bytes_per_row:.1} bytes per row");
}
