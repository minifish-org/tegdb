use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion};
use rusqlite::{params, Connection};
use std::env;
use std::fs;
use std::hint::black_box;
use std::path::PathBuf;

/// Creates a unique temporary file path for benchmarks
fn temp_db_path(prefix: &str) -> PathBuf {
    let mut path = env::temp_dir();
    path.push(format!("tegdb_limit_{}_{}", prefix, std::process::id()));
    path
}

/// Benchmark demonstrating TegDB's LIMIT clause optimization vs SQLite
fn limit_optimization_benchmark(c: &mut Criterion) {
    let tegdb_path = temp_db_path("tegdb");
    let sqlite_path = temp_db_path("sqlite");

    // Clean up existing files
    let _ = fs::remove_file(&tegdb_path);
    let _ = fs::remove_file(&sqlite_path);

    // Setup TegDB
    let mut tegdb = tegdb::Database::open(format!("file://{}", tegdb_path.display()))
        .expect("Failed to create TegDB database");

    // Setup SQLite
    let sqlite = Connection::open(&sqlite_path).unwrap();
    sqlite.pragma_update(None, "synchronous", "FULL").unwrap();
    sqlite.pragma_update(None, "journal_mode", "WAL").unwrap();

    // Create simple tables
    let create_table_sql = "CREATE TABLE items (
        id INTEGER PRIMARY KEY, 
        name TEXT NOT NULL, 
        category TEXT NOT NULL,
        value INTEGER NOT NULL
    )";

    tegdb
        .execute(create_table_sql)
        .expect("Failed to create TegDB table");
    sqlite.execute(create_table_sql, []).unwrap();

    // Insert test data
    println!("Setting up test data with 1,000 records...");

    for i in 1..=1000 {
        let insert_sql = format!(
            "INSERT INTO items (id, name, category, value) VALUES ({}, 'Item {}', '{}', {})",
            i,
            i,
            if i % 3 == 0 {
                "A"
            } else if i % 3 == 1 {
                "B"
            } else {
                "C"
            },
            (i * 7) % 100
        );
        tegdb.execute(&insert_sql).unwrap();

        sqlite
            .execute(
                "INSERT INTO items (id, name, category, value) VALUES (?, ?, ?, ?)",
                params![
                    i,
                    format!("Item {}", i),
                    if i % 3 == 0 {
                        "A"
                    } else if i % 3 == 1 {
                        "B"
                    } else {
                        "C"
                    },
                    (i * 7) % 100
                ],
            )
            .unwrap();
    }

    println!("Test data setup complete. Running LIMIT benchmarks...");

    // === LIMIT ON FULL TABLE SCAN ===
    // Test if planner can optimize LIMIT to stop early

    let mut group = c.benchmark_group("LIMIT Full Scan");

    group.bench_function(BenchmarkId::new("TegDB", "limit_5"), |b| {
        b.iter(|| {
            let result = tegdb.query("SELECT id, name FROM items LIMIT 5").unwrap();
            black_box(result);
        });
    });

    group.bench_function(BenchmarkId::new("SQLite", "limit_5"), |b| {
        b.iter(|| {
            let mut stmt = sqlite
                .prepare("SELECT id, name FROM items LIMIT 5")
                .unwrap();
            let mut rows = stmt.query([]).unwrap();
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

    group.bench_function(BenchmarkId::new("TegDB", "limit_50"), |b| {
        b.iter(|| {
            let result = tegdb.query("SELECT id, name FROM items LIMIT 50").unwrap();
            black_box(result);
        });
    });

    group.bench_function(BenchmarkId::new("SQLite", "limit_50"), |b| {
        b.iter(|| {
            let mut stmt = sqlite
                .prepare("SELECT id, name FROM items LIMIT 50")
                .unwrap();
            let mut rows = stmt.query([]).unwrap();
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

    // === LIMIT WITH WHERE CLAUSE ===
    // Test LIMIT with primary key filtering

    let mut group = c.benchmark_group("LIMIT with WHERE");

    group.bench_function(BenchmarkId::new("TegDB", "limit_pk_filter"), |b| {
        b.iter(|| {
            let result = tegdb
                .query("SELECT id, name FROM items WHERE id >= 100 LIMIT 10")
                .unwrap();
            black_box(result);
        });
    });

    group.bench_function(BenchmarkId::new("SQLite", "limit_pk_filter"), |b| {
        b.iter(|| {
            let mut stmt = sqlite
                .prepare("SELECT id, name FROM items WHERE id >= 100 LIMIT 10")
                .unwrap();
            let mut rows = stmt.query([]).unwrap();
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

    // === LIMIT WITH NON-INDEXED COLUMN ===
    // Test LIMIT with full table scan requirement

    let mut group = c.benchmark_group("LIMIT with Non-Indexed");

    group.bench_function(BenchmarkId::new("TegDB", "limit_category_filter"), |b| {
        b.iter(|| {
            let result = tegdb
                .query("SELECT id, name FROM items WHERE category = 'A' LIMIT 5")
                .unwrap();
            black_box(result);
        });
    });

    group.bench_function(BenchmarkId::new("SQLite", "limit_category_filter"), |b| {
        b.iter(|| {
            let mut stmt = sqlite
                .prepare("SELECT id, name FROM items WHERE category = 'A' LIMIT 5")
                .unwrap();
            let mut rows = stmt.query([]).unwrap();
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

    // === COMPARISON: FULL SCAN vs LIMIT ===
    // Show the difference between full scan and limited scan

    let mut group = c.benchmark_group("Full vs Limited Scan");

    group.bench_function(BenchmarkId::new("TegDB", "full_scan"), |b| {
        b.iter(|| {
            let result = tegdb.query("SELECT id, name FROM items").unwrap();
            black_box(result);
        });
    });

    group.bench_function(BenchmarkId::new("TegDB", "limited_scan"), |b| {
        b.iter(|| {
            let result = tegdb.query("SELECT id, name FROM items LIMIT 10").unwrap();
            black_box(result);
        });
    });

    group.bench_function(BenchmarkId::new("SQLite", "full_scan"), |b| {
        b.iter(|| {
            let mut stmt = sqlite.prepare("SELECT id, name FROM items").unwrap();
            let mut rows = stmt.query([]).unwrap();
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

    group.bench_function(BenchmarkId::new("SQLite", "limited_scan"), |b| {
        b.iter(|| {
            let mut stmt = sqlite
                .prepare("SELECT id, name FROM items LIMIT 10")
                .unwrap();
            let mut rows = stmt.query([]).unwrap();
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

    // Clean up
    drop(tegdb);
    drop(sqlite);
    let _ = fs::remove_file(&tegdb_path);
    let _ = fs::remove_file(&sqlite_path);

    println!("LIMIT optimization benchmarks completed!");
}

criterion_group!(limit_benches, limit_optimization_benchmark);
criterion_main!(limit_benches);
