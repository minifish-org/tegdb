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
        name TEXT(32) NOT NULL, 
        category TEXT(32) NOT NULL,
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

    // === PREPARE ALL STATEMENTS ONCE ===
    // Prepare all statements at the beginning to avoid borrow checker issues

    // Basic LIMIT statements
    let tegdb_limit_5 = tegdb.prepare("SELECT id, name FROM items LIMIT 5").unwrap();
    let tegdb_limit_50 = tegdb
        .prepare("SELECT id, name FROM items LIMIT 50")
        .unwrap();
    let mut sqlite_limit_5 = sqlite
        .prepare("SELECT id, name FROM items LIMIT 5")
        .unwrap();
    let mut sqlite_limit_50 = sqlite
        .prepare("SELECT id, name FROM items LIMIT 50")
        .unwrap();

    // LIMIT with WHERE statements
    let tegdb_limit_where = tegdb
        .prepare("SELECT id, name FROM items WHERE id >= 100 LIMIT 10")
        .unwrap();
    let mut sqlite_limit_where = sqlite
        .prepare("SELECT id, name FROM items WHERE id >= 100 LIMIT 10")
        .unwrap();

    // LIMIT with non-indexed filter statements
    let tegdb_limit_category = tegdb
        .prepare("SELECT id, name FROM items WHERE category = 'A' LIMIT 5")
        .unwrap();
    let mut sqlite_limit_category = sqlite
        .prepare("SELECT id, name FROM items WHERE category = 'A' LIMIT 5")
        .unwrap();

    // Full scan vs limited scan statements
    let tegdb_full_scan = tegdb.prepare("SELECT id, name FROM items").unwrap();
    let tegdb_limited_scan = tegdb
        .prepare("SELECT id, name FROM items LIMIT 10")
        .unwrap();
    let mut sqlite_full_scan = sqlite.prepare("SELECT id, name FROM items").unwrap();
    let mut sqlite_limited_scan = sqlite
        .prepare("SELECT id, name FROM items LIMIT 10")
        .unwrap();

    // === 1. BASIC LIMIT QUERIES ===
    let mut group = c.benchmark_group("Basic LIMIT Queries");

    // LIMIT 5
    group.bench_function(BenchmarkId::new("TegDB", "unprepared_limit_5"), |b| {
        b.iter(|| {
            let result = tegdb.query("SELECT id, name FROM items LIMIT 5").unwrap();
            black_box(result);
        });
    });

    group.bench_function(BenchmarkId::new("TegDB", "prepared_limit_5"), |b| {
        b.iter(|| {
            let result = tegdb.query_prepared(&tegdb_limit_5, &[]).unwrap();
            black_box(result);
        });
    });

    group.bench_function(BenchmarkId::new("SQLite", "prepared_limit_5"), |b| {
        b.iter(|| {
            let mut rows = sqlite_limit_5.query([]).unwrap();
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

    // LIMIT 50
    group.bench_function(BenchmarkId::new("TegDB", "unprepared_limit_50"), |b| {
        b.iter(|| {
            let result = tegdb.query("SELECT id, name FROM items LIMIT 50").unwrap();
            black_box(result);
        });
    });

    group.bench_function(BenchmarkId::new("TegDB", "prepared_limit_50"), |b| {
        b.iter(|| {
            let result = tegdb.query_prepared(&tegdb_limit_50, &[]).unwrap();
            black_box(result);
        });
    });

    group.bench_function(BenchmarkId::new("SQLite", "prepared_limit_50"), |b| {
        b.iter(|| {
            let mut rows = sqlite_limit_50.query([]).unwrap();
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

    // === 2. LIMIT WITH WHERE CLAUSES ===
    let mut group = c.benchmark_group("LIMIT with WHERE Clauses");

    group.bench_function(BenchmarkId::new("TegDB", "unprepared_limit_where"), |b| {
        b.iter(|| {
            let result = tegdb
                .query("SELECT id, name FROM items WHERE id >= 100 LIMIT 10")
                .unwrap();
            black_box(result);
        });
    });

    group.bench_function(BenchmarkId::new("TegDB", "prepared_limit_where"), |b| {
        b.iter(|| {
            let result = tegdb.query_prepared(&tegdb_limit_where, &[]).unwrap();
            black_box(result);
        });
    });

    group.bench_function(BenchmarkId::new("SQLite", "prepared_limit_where"), |b| {
        b.iter(|| {
            let mut rows = sqlite_limit_where.query([]).unwrap();
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

    // === 3. LIMIT WITH NON-INDEXED FILTERS ===
    let mut group = c.benchmark_group("LIMIT with Non-Indexed Filters");

    group.bench_function(
        BenchmarkId::new("TegDB", "unprepared_limit_category"),
        |b| {
            b.iter(|| {
                let result = tegdb
                    .query("SELECT id, name FROM items WHERE category = 'A' LIMIT 5")
                    .unwrap();
                black_box(result);
            });
        },
    );

    group.bench_function(BenchmarkId::new("TegDB", "prepared_limit_category"), |b| {
        b.iter(|| {
            let result = tegdb.query_prepared(&tegdb_limit_category, &[]).unwrap();
            black_box(result);
        });
    });

    group.bench_function(BenchmarkId::new("SQLite", "prepared_limit_category"), |b| {
        b.iter(|| {
            let mut rows = sqlite_limit_category.query([]).unwrap();
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

    // === 4. FULL SCAN vs LIMITED SCAN ===
    let mut group = c.benchmark_group("Full Scan vs Limited Scan");

    // Full scan
    group.bench_function(BenchmarkId::new("TegDB", "unprepared_full_scan"), |b| {
        b.iter(|| {
            let result = tegdb.query("SELECT id, name FROM items").unwrap();
            black_box(result);
        });
    });

    group.bench_function(BenchmarkId::new("TegDB", "prepared_full_scan"), |b| {
        b.iter(|| {
            let result = tegdb.query_prepared(&tegdb_full_scan, &[]).unwrap();
            black_box(result);
        });
    });

    group.bench_function(BenchmarkId::new("SQLite", "prepared_full_scan"), |b| {
        b.iter(|| {
            let mut rows = sqlite_full_scan.query([]).unwrap();
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

    // Limited scan
    group.bench_function(BenchmarkId::new("TegDB", "unprepared_limited_scan"), |b| {
        b.iter(|| {
            let result = tegdb.query("SELECT id, name FROM items LIMIT 10").unwrap();
            black_box(result);
        });
    });

    group.bench_function(BenchmarkId::new("TegDB", "prepared_limited_scan"), |b| {
        b.iter(|| {
            let result = tegdb.query_prepared(&tegdb_limited_scan, &[]).unwrap();
            black_box(result);
        });
    });

    group.bench_function(BenchmarkId::new("SQLite", "prepared_limited_scan"), |b| {
        b.iter(|| {
            let mut rows = sqlite_limited_scan.query([]).unwrap();
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

    // === CLEANUP ===
    // Drop all prepared statements before cleanup
    drop(tegdb_limit_5);
    drop(tegdb_limit_50);
    drop(tegdb_limit_where);
    drop(tegdb_limit_category);
    drop(tegdb_full_scan);
    drop(tegdb_limited_scan);
    drop(sqlite_limit_5);
    drop(sqlite_limit_50);
    drop(sqlite_limit_where);
    drop(sqlite_limit_category);
    drop(sqlite_full_scan);
    drop(sqlite_limited_scan);

    // Clean up
    drop(tegdb);
    drop(sqlite);
    let _ = fs::remove_file(&tegdb_path);
    let _ = fs::remove_file(&sqlite_path);

    println!("LIMIT optimization benchmarks completed!");
}

criterion_group!(limit_benches, limit_optimization_benchmark);
criterion_main!(limit_benches);
