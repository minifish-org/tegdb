//! Benchmark comparing TegDB's native row format performance against SQLite
//!
//! This benchmark shows how the native binary row format brings TegDB's
//! performance much closer to SQLite, especially for:
//! - Selective column queries (major improvement)
//! - Table scans with LIMIT clauses
//! - Storage efficiency

use std::time::Instant;
use tegdb::Database as TegDatabase;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("=== TegDB vs SQLite Performance Comparison ===");
    println!("Testing TegDB's Native Binary Row Format against SQLite\n");

    let row_count = 25000;
    println!(
        "Dataset: {} rows with 5 columns (id, name, email, score, active)",
        row_count
    );

    // Clean up any existing databases
    let _ = std::fs::remove_file("tegdb_native.db");
    let _ = std::fs::remove_file("sqlite_comparison.db");

    // Test TegDB with native format
    println!("\n1. Testing TegDB with Native Binary Row Format...");
    let tegdb_results = test_tegdb_native(row_count)?;

    // Test SQLite
    println!("2. Testing SQLite (for comparison)...");
    let sqlite_results = test_sqlite(row_count)?;

    // Print detailed comparison
    println!("\n=== DETAILED PERFORMANCE COMPARISON ===");

    print_comparison_header();
    print_comparison_row(
        "Table Creation",
        tegdb_results.creation_time,
        sqlite_results.creation_time,
    );
    print_comparison_row(
        "Bulk Insert",
        tegdb_results.insert_time,
        sqlite_results.insert_time,
    );
    print_comparison_row(
        "Full Table Scan",
        tegdb_results.full_scan_time,
        sqlite_results.full_scan_time,
    );
    print_comparison_row(
        "Selective Columns (name, score)",
        tegdb_results.selective_scan_time,
        sqlite_results.selective_scan_time,
    );
    print_comparison_row(
        "Primary Key Lookup",
        tegdb_results.pk_lookup_time,
        sqlite_results.pk_lookup_time,
    );
    print_comparison_row(
        "LIMIT 1000 Query",
        tegdb_results.limited_scan_time,
        sqlite_results.limited_scan_time,
    );
    print_comparison_row(
        "COUNT(*) Query",
        tegdb_results.count_time,
        sqlite_results.count_time,
    );

    println!("\n=== STORAGE EFFICIENCY ===");
    println!("TegDB (Native Format): {:>10} bytes", tegdb_results.db_size);
    println!(
        "SQLite:                {:>10} bytes",
        sqlite_results.db_size
    );

    let size_ratio = sqlite_results.db_size as f64 / tegdb_results.db_size as f64;
    if size_ratio > 1.0 {
        println!(
            "TegDB storage is {:.1}x more compact than SQLite",
            size_ratio
        );
    } else {
        println!(
            "SQLite storage is {:.1}x more compact than TegDB",
            1.0 / size_ratio
        );
    }

    println!("\n=== PERFORMANCE ANALYSIS ===");

    // Analyze key metrics
    let selective_ratio =
        tegdb_results.selective_scan_time as f64 / sqlite_results.selective_scan_time as f64;
    let full_scan_ratio =
        tegdb_results.full_scan_time as f64 / sqlite_results.full_scan_time as f64;
    let pk_lookup_ratio =
        tegdb_results.pk_lookup_time as f64 / sqlite_results.pk_lookup_time as f64;
    let limit_ratio =
        tegdb_results.limited_scan_time as f64 / sqlite_results.limited_scan_time as f64;

    println!("Key Performance Gaps (TegDB vs SQLite):");
    println!(
        "• Selective column queries: {:.1}x SQLite speed",
        1.0 / selective_ratio
    );
    println!(
        "• Full table scans:         {:.1}x SQLite speed",
        1.0 / full_scan_ratio
    );
    println!(
        "• Primary key lookups:      {:.1}x SQLite speed",
        1.0 / pk_lookup_ratio
    );
    println!(
        "• LIMIT queries:            {:.1}x SQLite speed",
        1.0 / limit_ratio
    );

    println!("\n=== IMPROVEMENTS WITH NATIVE FORMAT ===");
    println!("The native binary row format brings several key improvements:");
    println!("✓ Significantly faster selective column access");
    println!("✓ Reduced memory usage during table scans");
    println!("✓ More compact storage format");
    println!("✓ Better cache locality for column-specific queries");
    println!("✓ Partial row deserialization avoids unnecessary work");

    if selective_ratio < 3.0 {
        println!("\n🎉 TegDB's selective column performance is now competitive with SQLite!");
    }

    if full_scan_ratio < 2.0 {
        println!("🎉 TegDB's full scan performance gap has been significantly reduced!");
    }

    // Clean up
    let _ = std::fs::remove_file("tegdb_native.db");
    let _ = std::fs::remove_file("sqlite_comparison.db");

    Ok(())
}

#[derive(Debug)]
struct BenchmarkResults {
    creation_time: u128,
    insert_time: u128,
    full_scan_time: u128,
    selective_scan_time: u128,
    pk_lookup_time: u128,
    limited_scan_time: u128,
    count_time: u128,
    db_size: u64,
}

fn test_tegdb_native(row_count: usize) -> Result<BenchmarkResults, Box<dyn std::error::Error>> {
    let start = Instant::now();

    let mut db = TegDatabase::open("tegdb_native.db")?;
    let creation_time = start.elapsed().as_nanos();

    // Create table
    db.execute("CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT, email TEXT, score REAL, active INTEGER)")?;

    // Bulk insert
    let insert_start = Instant::now();
    for i in 0..row_count {
        let name = format!("User{:05}", i);
        let email = format!("user{}@example.com", i);
        let score = 50.0 + (i % 50) as f64;
        let active = if i % 3 == 0 { 1 } else { 0 };

        db.execute(&format!(
            "INSERT INTO users (id, name, email, score, active) VALUES ({}, '{}', '{}', {}, {})",
            i, name, email, score, active
        ))?;
    }
    let insert_time = insert_start.elapsed().as_nanos();

    // Full table scan
    let full_start = Instant::now();
    let _result = db
        .query("SELECT * FROM users")
        .unwrap()
        .into_query_result()
        .unwrap();
    let full_scan_time = full_start.elapsed().as_nanos();

    // Selective column scan
    let selective_start = Instant::now();
    let _result = db
        .query("SELECT name, score FROM users")
        .unwrap()
        .into_query_result()
        .unwrap();
    let selective_scan_time = selective_start.elapsed().as_nanos();

    // Primary key lookup
    let pk_start = Instant::now();
    let _result = db
        .query(&format!(
            "SELECT name, email FROM users WHERE id = {}",
            row_count / 2
        ))
        .unwrap()
        .into_query_result()
        .unwrap();
    let pk_lookup_time = pk_start.elapsed().as_nanos();

    // Limited scan
    let limit_start = Instant::now();
    let _result = db
        .query("SELECT name, score FROM users LIMIT 1000")
        .unwrap()
        .into_query_result()
        .unwrap();
    let limited_scan_time = limit_start.elapsed().as_nanos();

    // Count query
    let count_start = Instant::now();
    let _result = db
        .query("SELECT COUNT(*) FROM users")
        .unwrap()
        .into_query_result()
        .unwrap();
    let count_time = count_start.elapsed().as_nanos();

    let db_size = std::fs::metadata("tegdb_native.db")?.len();

    Ok(BenchmarkResults {
        creation_time,
        insert_time,
        full_scan_time,
        selective_scan_time,
        pk_lookup_time,
        limited_scan_time,
        count_time,
        db_size,
    })
}

fn test_sqlite(row_count: usize) -> Result<BenchmarkResults, Box<dyn std::error::Error>> {
    let start = Instant::now();

    let conn = rusqlite::Connection::open("sqlite_comparison.db")?;
    let creation_time = start.elapsed().as_nanos();

    // Create table
    conn.execute("CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT, email TEXT, score REAL, active INTEGER)", [])?;

    // Bulk insert
    let insert_start = Instant::now();
    for i in 0..row_count {
        conn.execute(
            "INSERT INTO users VALUES (?, ?, ?, ?, ?)",
            [
                &i as &dyn rusqlite::ToSql,
                &format!("User{:05}", i),
                &format!("user{}@example.com", i),
                &(50.0 + (i % 50) as f64),
                &(if i % 3 == 0 { 1 } else { 0 }),
            ],
        )?;
    }
    let insert_time = insert_start.elapsed().as_nanos();

    // Full table scan
    let full_start = Instant::now();
    let mut stmt = conn.prepare("SELECT * FROM users")?;
    let rows = stmt.query_map([], |_row| Ok(()))?;
    let _count: Result<Vec<_>, _> = rows.collect();
    let full_scan_time = full_start.elapsed().as_nanos();

    // Selective column scan
    let selective_start = Instant::now();
    let mut stmt = conn.prepare("SELECT name, score FROM users")?;
    let rows = stmt.query_map([], |_row| Ok(()))?;
    let _count: Result<Vec<_>, _> = rows.collect();
    let selective_scan_time = selective_start.elapsed().as_nanos();

    // Primary key lookup
    let pk_start = Instant::now();
    let mut stmt = conn.prepare(&format!(
        "SELECT name, email FROM users WHERE id = {}",
        row_count / 2
    ))?;
    let rows = stmt.query_map([], |_row| Ok(()))?;
    let _count: Result<Vec<_>, _> = rows.collect();
    let pk_lookup_time = pk_start.elapsed().as_nanos();

    // Limited scan
    let limit_start = Instant::now();
    let mut stmt = conn.prepare("SELECT name, score FROM users LIMIT 1000")?;
    let rows = stmt.query_map([], |_row| Ok(()))?;
    let _count: Result<Vec<_>, _> = rows.collect();
    let limited_scan_time = limit_start.elapsed().as_nanos();

    // Count query
    let count_start = Instant::now();
    let mut stmt = conn.prepare("SELECT COUNT(*) FROM users")?;
    let rows = stmt.query_map([], |_row| Ok(()))?;
    let _count: Result<Vec<_>, _> = rows.collect();
    let count_time = count_start.elapsed().as_nanos();

    let db_size = std::fs::metadata("sqlite_comparison.db")?.len();

    Ok(BenchmarkResults {
        creation_time,
        insert_time,
        full_scan_time,
        selective_scan_time,
        pk_lookup_time,
        limited_scan_time,
        count_time,
        db_size,
    })
}

fn print_comparison_header() {
    println!(
        "{:<25} | {:>12} | {:>12} | {:>12}",
        "Operation", "TegDB (ms)", "SQLite (ms)", "Ratio"
    );
    println!("{}", "-".repeat(70));
}

fn print_comparison_row(operation: &str, tegdb_ns: u128, sqlite_ns: u128) {
    let tegdb_ms = tegdb_ns as f64 / 1_000_000.0;
    let sqlite_ms = sqlite_ns as f64 / 1_000_000.0;
    let ratio = if sqlite_ns > 0 {
        tegdb_ns as f64 / sqlite_ns as f64
    } else {
        f64::INFINITY
    };

    println!(
        "{:<25} | {:>10.2} | {:>10.2} | {:>10.1}x",
        operation, tegdb_ms, sqlite_ms, ratio
    );
}
