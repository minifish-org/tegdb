use criterion::{black_box, criterion_group, criterion_main, Criterion, BenchmarkId};
use rusqlite::{params, Connection};
use std::path::PathBuf;
use std::env;
use std::fs;

/// Creates a unique temporary file path for benchmarks
fn temp_db_path(prefix: &str) -> PathBuf {
    let mut path = env::temp_dir();
    path.push(format!("tegdb_focused_{}_{}", prefix, std::process::id()));
    path
}

/// Focused benchmark demonstrating TegDB's query planner benefits
fn focused_planner_benchmark(c: &mut Criterion) {
    let tegdb_path = temp_db_path("tegdb");
    let sqlite_path = temp_db_path("sqlite");
    
    // Clean up existing files
    let _ = fs::remove_file(&tegdb_path);
    let _ = fs::remove_file(&sqlite_path);
    
    // Setup TegDB
    let mut tegdb = tegdb::Database::open(&tegdb_path).expect("Failed to create TegDB database");
    
    // Setup SQLite
    let sqlite = Connection::open(&sqlite_path).unwrap();
    sqlite.pragma_update(None, "synchronous", "FULL").unwrap();
    
    // Create tables with same schema
    let create_table_sql = "CREATE TABLE products (
        id INTEGER PRIMARY KEY, 
        name TEXT NOT NULL, 
        category TEXT NOT NULL,
        price REAL NOT NULL
    )";
    
    tegdb.execute(create_table_sql).expect("Failed to create TegDB table");
    sqlite.execute(create_table_sql, []).unwrap();
    
    // Insert test data
    println!("Setting up test data...");
    
    for i in 1..=500 {
        let insert_sql = format!(
            "INSERT INTO products (id, name, category, price) VALUES ({}, 'Product {}', '{}', {:.2})",
            i,
            i,
            if i % 3 == 0 { "Electronics" } else if i % 3 == 1 { "Books" } else { "Clothing" },
            (i as f64 * 9.99) % 1000.0 + 10.0
        );
        tegdb.execute(&insert_sql).unwrap();
        
        sqlite.execute(
            "INSERT INTO products (id, name, category, price) VALUES (?, ?, ?, ?)",
            params![
                i,
                format!("Product {}", i),
                if i % 3 == 0 { "Electronics" } else if i % 3 == 1 { "Books" } else { "Clothing" },
                (i as f64 * 9.99) % 1000.0 + 10.0
            ],
        ).unwrap();
    }
    
    println!("Test data setup complete. Running focused benchmarks...");
    
    // PRIMARY KEY LOOKUP - TegDB's IOT advantage should show here
    let mut group = c.benchmark_group("Primary Key Lookup");
    
    group.bench_function(BenchmarkId::new("TegDB", "single_pk"), |b| {
        b.iter(|| {
            let id = black_box(250);
            let result = tegdb.query(&format!("SELECT name, price FROM products WHERE id = {}", id)).unwrap();
            black_box(result);
        });
    });
    
    group.bench_function(BenchmarkId::new("SQLite", "single_pk"), |b| {
        b.iter(|| {
            let id = black_box(250);
            let mut stmt = sqlite.prepare("SELECT name, price FROM products WHERE id = ?").unwrap();
            let mut rows = stmt.query([id]).unwrap();
            let mut results = Vec::new();
            while let Some(row) = rows.next().unwrap() {
                results.push((
                    row.get::<_, String>(0).unwrap(),
                    row.get::<_, f64>(1).unwrap(),
                ));
            }
            black_box(results);
        });
    });
    
    group.finish();
    
    // RANGE QUERIES - Planner should optimize these
    let mut group = c.benchmark_group("Range Query");
    
    group.bench_function(BenchmarkId::new("TegDB", "pk_range"), |b| {
        b.iter(|| {
            let start = black_box(100);
            let end = black_box(110);
            let result = tegdb.query(&format!("SELECT id, name FROM products WHERE id >= {} AND id <= {}", start, end)).unwrap();
            black_box(result);
        });
    });
    
    group.bench_function(BenchmarkId::new("SQLite", "pk_range"), |b| {
        b.iter(|| {
            let start = black_box(100);
            let end = black_box(110);
            let mut stmt = sqlite.prepare("SELECT id, name FROM products WHERE id >= ? AND id <= ?").unwrap();
            let mut rows = stmt.query([start, end]).unwrap();
            let mut results = Vec::new();
            while let Some(row) = rows.next().unwrap() {
                results.push((
                    row.get::<_, i64>(0).unwrap(),
                    row.get::<_, String>(1).unwrap(),
                ));
            }
            black_box(results);
        });
    });
    
    group.finish();
    
    // SCAN COMPARISON - Full scan vs optimized access
    let mut group = c.benchmark_group("Scan Comparison");
    
    // Non-indexed category search (full scan)
    group.bench_function(BenchmarkId::new("TegDB", "full_scan"), |b| {
        b.iter(|| {
            let category = black_box("Electronics");
            let result = tegdb.query(&format!("SELECT id, name FROM products WHERE category = '{}'", category)).unwrap();
            black_box(result);
        });
    });
    
    group.bench_function(BenchmarkId::new("SQLite", "full_scan"), |b| {
        b.iter(|| {
            let category = black_box("Electronics");
            let mut stmt = sqlite.prepare("SELECT id, name FROM products WHERE category = ?").unwrap();
            let mut rows = stmt.query([category]).unwrap();
            let mut results = Vec::new();
            while let Some(row) = rows.next().unwrap() {
                results.push((
                    row.get::<_, i64>(0).unwrap(),
                    row.get::<_, String>(1).unwrap(),
                ));
            }
            black_box(results);
        });
    });
    
    // Primary key access (optimized)
    group.bench_function(BenchmarkId::new("TegDB", "optimized_pk"), |b| {
        b.iter(|| {
            let id = black_box(300);
            let result = tegdb.query(&format!("SELECT name, category FROM products WHERE id = {}", id)).unwrap();
            black_box(result);
        });
    });
    
    group.bench_function(BenchmarkId::new("SQLite", "optimized_pk"), |b| {
        b.iter(|| {
            let id = black_box(300);
            let mut stmt = sqlite.prepare("SELECT name, category FROM products WHERE id = ?").unwrap();
            let mut rows = stmt.query([id]).unwrap();
            let mut results = Vec::new();
            while let Some(row) = rows.next().unwrap() {
                results.push((
                    row.get::<_, String>(0).unwrap(),
                    row.get::<_, String>(1).unwrap(),
                ));
            }
            black_box(results);
        });
    });
    
    group.finish();
    
    // Clean up
    drop(tegdb);
    drop(sqlite);
    let _ = fs::remove_file(&tegdb_path);
    let _ = fs::remove_file(&sqlite_path);
    
    println!("Focused planner benchmarks completed!");
}

criterion_group!(focused_benches, focused_planner_benchmark);
criterion_main!(focused_benches);
