use criterion::{black_box, criterion_group, criterion_main, Criterion};
use rusqlite::{params, Connection};
use std::path::PathBuf;
use std::env;
use std::fs;

/// Creates a unique temporary file path for benchmarks
fn temp_db_path(prefix: &str) -> PathBuf {
    let mut path = env::temp_dir();
    path.push(format!("tegdb_bench_{}_{}", prefix, std::process::id()));
    path
}

fn database_benchmark(c: &mut Criterion) {
    let path = temp_db_path("database");
    if path.exists() {
        fs::remove_file(&path).expect("Failed to remove existing test file");
    }
    
    let mut db = tegdb::Database::open(&path).expect("Failed to create database");
    
    // Setup table for benchmarking
    db.execute("CREATE TABLE benchmark_test (id INTEGER PRIMARY KEY, name TEXT, value INTEGER)")
        .expect("Failed to create table");
    
    // Insert some initial data for SELECT operations
    db.execute("INSERT INTO benchmark_test (id, name, value) VALUES (1, 'test', 100)")
        .expect("Failed to insert initial data");

    // Benchmark INSERT operations
    c.bench_function("database insert", |b| {
        b.iter(|| {
            // Use timestamp-based ID to ensure uniqueness across all benchmarks
            let id = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos() as u64;
            let sql = format!(
                "INSERT INTO benchmark_test (id, name, value) VALUES ({}, 'test_{}', {})",
                black_box(id),
                black_box(id),
                black_box((id % 1000) * 10)
            );
            db.execute(&sql).unwrap();
        })
    });

    // Benchmark SELECT operations
    c.bench_function("database select", |b| {
        b.iter(|| {
            let result = db.query("SELECT * FROM benchmark_test WHERE id = 1").unwrap();
            black_box(result);
        })
    });

    // Benchmark SELECT with WHERE clause
    c.bench_function("database select where", |b| {
        b.iter(|| {
            let result = db.query("SELECT name, value FROM benchmark_test WHERE value > 50").unwrap();
            black_box(result);
        })
    });

    // Benchmark UPDATE operations
    c.bench_function("database update", |b| {
        b.iter(|| {
            let affected = db.execute("UPDATE benchmark_test SET value = 999 WHERE id = 1").unwrap();
            black_box(affected);
        })
    });

    // Benchmark transaction operations
    c.bench_function("database transaction", |b| {
        b.iter(|| {
            // Use timestamp-based ID to ensure uniqueness across all benchmarks
            let id = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos() as u64;
            let mut tx = db.begin_transaction().unwrap();
            let sql = format!(
                "INSERT INTO benchmark_test (id, name, value) VALUES ({}, 'tx_test_{}', {})",
                black_box(id),
                black_box(id),
                black_box((id % 1000) * 5)
            );
            tx.execute(&sql).unwrap();
            tx.commit().unwrap();
        })
    });

    // Benchmark DELETE operations
    c.bench_function("database delete", |b| {
        b.iter(|| {
            let affected = db.execute("DELETE FROM benchmark_test WHERE value = 999").unwrap();
            black_box(affected);
        })
    });

    // Clean up
    drop(db);
    let _ = fs::remove_file(&path);
}

fn sqlite_sql_benchmark(c: &mut Criterion) {
    let path = temp_db_path("sqlite");
    if path.exists() {
        fs::remove_file(&path).expect("Failed to remove existing test file");
    }
    
    let conn = Connection::open(&path).unwrap();
    
    // Configure SQLite for durability (similar to TegDB's sync_on_write: true)
    conn.pragma_update(None, "synchronous", "FULL").unwrap(); // Ensure full fsync on every write
    conn.pragma_update(None, "journal_mode", "WAL").unwrap(); // Use WAL mode for better performance
    
    conn.execute(
        "CREATE TABLE benchmark_test (id INTEGER PRIMARY KEY, name TEXT, value INTEGER)",
        [],
    ).unwrap();
    
    // Insert some initial data for SELECT operations
    conn.execute("INSERT INTO benchmark_test (id, name, value) VALUES (1, 'test', 100)", []).unwrap();

    // Benchmark INSERT operations
    c.bench_function("sqlite sql insert", |b| {
        b.iter(|| {
            // Use timestamp-based ID to ensure uniqueness across all benchmarks
            let id = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos() as u64;
            conn.execute(
                "INSERT INTO benchmark_test (id, name, value) VALUES (?, ?, ?)",
                params![black_box(id), format!("test_{}", black_box(id)), black_box((id % 1000) * 10)],
            ).unwrap();
        })
    });

    // Benchmark SELECT operations
    c.bench_function("sqlite sql select", |b| {
        b.iter(|| {
            let mut stmt = conn.prepare("SELECT * FROM benchmark_test WHERE id = ?").unwrap();
            let mut rows = stmt.query([black_box(1)]).unwrap();
            while let Some(row) = rows.next().unwrap() {
                black_box((
                    row.get::<_, i64>(0).unwrap(),
                    row.get::<_, String>(1).unwrap(),
                    row.get::<_, i64>(2).unwrap(),
                ));
            }
        })
    });

    // Benchmark SELECT with WHERE clause
    c.bench_function("sqlite sql select where", |b| {
        b.iter(|| {
            let mut stmt = conn.prepare("SELECT name, value FROM benchmark_test WHERE value > ?").unwrap();
            let mut rows = stmt.query([black_box(50)]).unwrap();
            let mut results = Vec::new();
            while let Some(row) = rows.next().unwrap() {
                results.push((
                    row.get::<_, String>(0).unwrap(),
                    row.get::<_, i64>(1).unwrap(),
                ));
            }
            black_box(results);
        })
    });

    // Benchmark UPDATE operations
    c.bench_function("sqlite sql update", |b| {
        b.iter(|| {
            let affected = conn.execute(
                "UPDATE benchmark_test SET value = ? WHERE id = ?",
                params![black_box(999), black_box(1)],
            ).unwrap();
            black_box(affected);
        })
    });

    // Benchmark transaction operations
    c.bench_function("sqlite sql transaction", |b| {
        b.iter(|| {
            // Use timestamp-based ID to ensure uniqueness across all benchmarks
            let id = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos() as u64;
            let tx = conn.unchecked_transaction().unwrap();
            tx.execute(
                "INSERT INTO benchmark_test (id, name, value) VALUES (?, ?, ?)",
                params![black_box(id), format!("tx_test_{}", black_box(id)), black_box((id % 1000) * 5)],
            ).unwrap();
            tx.commit().unwrap();
        })
    });

    // Benchmark DELETE operations
    c.bench_function("sqlite sql delete", |b| {
        b.iter(|| {
            let affected = conn.execute(
                "DELETE FROM benchmark_test WHERE value = ?",
                params![black_box(999)],
            ).unwrap();
            black_box(affected);
        })
    });
    
    // Clean up
    drop(conn);
    let _ = fs::remove_file(&path);
}

criterion_group!(
    database_benches,
    database_benchmark,
    sqlite_sql_benchmark
);
criterion_main!(database_benches);
