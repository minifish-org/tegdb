use criterion::{black_box, criterion_group, criterion_main, BenchmarkId, Criterion};
use rusqlite::{params, Connection};
use std::env;
use std::fs;
use std::path::PathBuf;

/// Creates a unique temporary file path for benchmarks
fn temp_db_path(prefix: &str) -> PathBuf {
    let mut path = env::temp_dir();
    path.push(format!(
        "tegdb_planner_bench_{}_{}",
        prefix,
        std::process::id()
    ));
    path
}

/// Benchmark demonstrating TegDB's query planner optimizations vs SQLite
fn planner_optimization_benchmark(c: &mut Criterion) {
    let tegdb_path = temp_db_path("tegdb_planner");
    let sqlite_path = temp_db_path("sqlite_planner");

    // Clean up existing files
    let _ = fs::remove_file(&tegdb_path);
    let _ = fs::remove_file(&sqlite_path);

    // Setup TegDB
    let mut tegdb = tegdb::Database::open(&format!("file://{}", tegdb_path.display())).expect("Failed to create TegDB database");

    // Setup SQLite with similar configuration
    let sqlite = Connection::open(&sqlite_path).unwrap();
    sqlite.pragma_update(None, "synchronous", "FULL").unwrap();
    sqlite.pragma_update(None, "journal_mode", "WAL").unwrap();

    // Create tables with same schema
    let create_table_sql = "CREATE TABLE products (
        id INTEGER PRIMARY KEY, 
        name TEXT NOT NULL, 
        category TEXT NOT NULL,
        price REAL NOT NULL,
        stock INTEGER NOT NULL
    )";

    tegdb
        .execute(create_table_sql)
        .expect("Failed to create TegDB table");
    sqlite.execute(create_table_sql, []).unwrap();

    // Insert test data - enough to make full table scans expensive
    println!("Setting up test data with 1,000 records...");

    // TegDB data insertion
    for i in 1..=1000 {
        let insert_sql = format!(
            "INSERT INTO products (id, name, category, price, stock) VALUES ({}, 'Product {}', '{}', {:.2}, {})",
            i,
            i,
            if i % 3 == 0 { "Electronics" } else if i % 3 == 1 { "Books" } else { "Clothing" },
            (i as f64 * 9.99) % 1000.0 + 10.0,
            (i * 7) % 100 + 1
        );
        tegdb.execute(&insert_sql).unwrap();
    }

    // SQLite data insertion
    for i in 1..=1000 {
        sqlite
            .execute(
                "INSERT INTO products (id, name, category, price, stock) VALUES (?, ?, ?, ?, ?)",
                params![
                    i,
                    format!("Product {}", i),
                    if i % 3 == 0 {
                        "Electronics"
                    } else if i % 3 == 1 {
                        "Books"
                    } else {
                        "Clothing"
                    },
                    (i as f64 * 9.99) % 1000.0 + 10.0,
                    (i * 7) % 100 + 1
                ],
            )
            .unwrap();
    }

    println!("Test data setup complete. Running benchmarks...");

    // === PRIMARY KEY LOOKUP OPTIMIZATION ===
    // This should show TegDB's IOT (Index-Organized Table) advantage

    let mut group = c.benchmark_group("Primary Key Lookup");

    // Test single primary key lookup
    group.bench_function(BenchmarkId::new("TegDB", "single_pk_lookup"), |b| {
        b.iter(|| {
            let id = black_box(5000); // Middle of dataset
            let result = tegdb
                .query(&format!("SELECT * FROM products WHERE id = {id}"))
                .unwrap();
            black_box(result);
        });
    });

    group.bench_function(BenchmarkId::new("SQLite", "single_pk_lookup"), |b| {
        b.iter(|| {
            let id = black_box(5000);
            let mut stmt = sqlite
                .prepare("SELECT * FROM products WHERE id = ?")
                .unwrap();
            let mut rows = stmt.query([id]).unwrap();
            let mut results = Vec::new();
            while let Some(row) = rows.next().unwrap() {
                results.push((
                    row.get::<_, i64>(0).unwrap(),
                    row.get::<_, String>(1).unwrap(),
                    row.get::<_, String>(2).unwrap(),
                    row.get::<_, f64>(3).unwrap(),
                    row.get::<_, i64>(4).unwrap(),
                ));
            }
            black_box(results);
        });
    });

    group.finish();

    // === RANGE QUERIES ON PRIMARY KEY ===
    // TegDB's planner should optimize range queries on primary keys

    let mut group = c.benchmark_group("Primary Key Range Query");

    group.bench_function(BenchmarkId::new("TegDB", "pk_range_small"), |b| {
        b.iter(|| {
            let start = black_box(1000);
            let end = black_box(1010); // Small range - 10 records
            let result = tegdb
                .query(&format!(
                    "SELECT id, name, price FROM products WHERE id >= {start} AND id <= {end}"
                ))
                .unwrap();
            black_box(result);
        });
    });

    group.bench_function(BenchmarkId::new("SQLite", "pk_range_small"), |b| {
        b.iter(|| {
            let start = black_box(1000);
            let end = black_box(1010);
            let mut stmt = sqlite
                .prepare("SELECT id, name, price FROM products WHERE id >= ? AND id <= ?")
                .unwrap();
            let mut rows = stmt.query([start, end]).unwrap();
            let mut results = Vec::new();
            while let Some(row) = rows.next().unwrap() {
                results.push((
                    row.get::<_, i64>(0).unwrap(),
                    row.get::<_, String>(1).unwrap(),
                    row.get::<_, f64>(2).unwrap(),
                ));
            }
            black_box(results);
        });
    });

    group.finish();

    // === COMPLEX WHERE CLAUSES ===
    // Test planner's ability to optimize complex conditions

    let mut group = c.benchmark_group("Complex WHERE Optimization");

    // Query that benefits from primary key optimization
    group.bench_function(
        BenchmarkId::new("TegDB", "pk_with_additional_filter"),
        |b| {
            b.iter(|| {
                let id = black_box(7500);
                let min_price = black_box(50.0);
                let result = tegdb
                    .query(&format!(
                        "SELECT name, price FROM products WHERE id = {id} AND price > {min_price}"
                    ))
                    .unwrap();
                black_box(result);
            });
        },
    );

    group.bench_function(
        BenchmarkId::new("SQLite", "pk_with_additional_filter"),
        |b| {
            b.iter(|| {
                let id = black_box(7500);
                let min_price = black_box(50.0);
                let mut stmt = sqlite
                    .prepare("SELECT name, price FROM products WHERE id = ? AND price > ?")
                    .unwrap();
                let mut rows = stmt.query([id.to_string(), min_price.to_string()]).unwrap();
                let mut results = Vec::new();
                while let Some(row) = rows.next().unwrap() {
                    results.push((
                        row.get::<_, String>(0).unwrap(),
                        row.get::<_, f64>(1).unwrap(),
                    ));
                }
                black_box(results);
            });
        },
    );

    group.finish();

    // === FULL TABLE SCAN VS OPTIMIZED ACCESS ===
    // Compare scenarios where TegDB can avoid full table scans

    let mut group = c.benchmark_group("Scan Avoidance");

    // Non-indexed column search (should be slower for both)
    group.bench_function(BenchmarkId::new("TegDB", "full_scan_category"), |b| {
        b.iter(|| {
            let category = black_box("Electronics");
            let result = tegdb
                .query(&format!(
                    "SELECT id, name FROM products WHERE category = '{category}'"
                ))
                .unwrap();
            black_box(result);
        });
    });

    group.bench_function(BenchmarkId::new("SQLite", "full_scan_category"), |b| {
        b.iter(|| {
            let category = black_box("Electronics");
            let mut stmt = sqlite
                .prepare("SELECT id, name FROM products WHERE category = ?")
                .unwrap();
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

    // Primary key access (should be much faster for TegDB due to IOT)
    group.bench_function(BenchmarkId::new("TegDB", "optimized_pk_access"), |b| {
        b.iter(|| {
            let ids = [
                black_box(100),
                black_box(2000),
                black_box(5000),
                black_box(8000),
            ];
            for id in ids {
                let result = tegdb
                    .query(&format!("SELECT name, price FROM products WHERE id = {id}"))
                    .unwrap();
                black_box(result);
            }
        });
    });

    group.bench_function(BenchmarkId::new("SQLite", "optimized_pk_access"), |b| {
        b.iter(|| {
            let ids = [
                black_box(100),
                black_box(2000),
                black_box(5000),
                black_box(8000),
            ];
            for id in ids {
                let mut stmt = sqlite
                    .prepare("SELECT name, price FROM products WHERE id = ?")
                    .unwrap();
                let mut rows = stmt.query([id]).unwrap();
                let mut results = Vec::new();
                while let Some(row) = rows.next().unwrap() {
                    results.push((
                        row.get::<_, String>(0).unwrap(),
                        row.get::<_, f64>(1).unwrap(),
                    ));
                }
                black_box(results);
            }
        });
    });

    group.finish();

    // === AGGREGATE OPERATIONS ===
    // Test planner optimizations for aggregate queries

    let mut group = c.benchmark_group("Aggregate Optimization");

    // Count with WHERE clause on primary key
    group.bench_function(BenchmarkId::new("TegDB", "count_with_pk_range"), |b| {
        b.iter(|| {
            let start = black_box(1000);
            let end = black_box(2000);
            let result = tegdb
                .query(&format!(
                    "SELECT id FROM products WHERE id >= {start} AND id <= {end}"
                ))
                .unwrap();
            // Simulate count by getting length
            black_box(result.rows().len());
        });
    });

    group.bench_function(BenchmarkId::new("SQLite", "count_with_pk_range"), |b| {
        b.iter(|| {
            let start = black_box(1000);
            let end = black_box(2000);
            let mut stmt = sqlite
                .prepare("SELECT COUNT(*) FROM products WHERE id >= ? AND id <= ?")
                .unwrap();
            let mut rows = stmt.query([start, end]).unwrap();
            if let Some(row) = rows.next().unwrap() {
                black_box(row.get::<_, i64>(0).unwrap());
            }
        });
    });

    group.finish();

    // === PLANNER STRESS TEST ===
    // Complex queries that really test the planner's capabilities

    let mut group = c.benchmark_group("Planner Stress Test");

    // Multi-condition query with primary key optimization opportunity
    group.bench_function(BenchmarkId::new("TegDB", "multi_condition_with_pk"), |b| {
        b.iter(|| {
            let id = black_box(3333);
            let min_price = black_box(100.0);
            let min_stock = black_box(10);
            let result = tegdb.query(&format!(
                "SELECT name, price, stock FROM products WHERE id = {id} AND price > {min_price} AND stock >= {min_stock}"
            )).unwrap();
            black_box(result);
        });
    });

    group.bench_function(BenchmarkId::new("SQLite", "multi_condition_with_pk"), |b| {
        b.iter(|| {
            let id = black_box(3333);
            let min_price = black_box(100.0);
            let min_stock = black_box(10);
            let mut stmt = sqlite.prepare(
                "SELECT name, price, stock FROM products WHERE id = ? AND price > ? AND stock >= ?"
            ).unwrap();
            let mut rows = stmt
                .query([id.to_string(), min_price.to_string(), min_stock.to_string()])
                .unwrap();
            let mut results = Vec::new();
            while let Some(row) = rows.next().unwrap() {
                results.push((
                    row.get::<_, String>(0).unwrap(),
                    row.get::<_, f64>(1).unwrap(),
                    row.get::<_, i64>(2).unwrap(),
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

    println!("Planner optimization benchmarks completed!");
}

criterion_group!(planner_benches, planner_optimization_benchmark);
criterion_main!(planner_benches);
