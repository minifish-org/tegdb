use criterion::{black_box, criterion_group, criterion_main, Criterion};
use rusqlite::{params, Connection};
use std::path::PathBuf;
use std::env;
use std::fs;
use tegdb::Engine;

/// Creates a unique temporary file path for benchmarks
fn temp_db_path(prefix: &str) -> PathBuf {
    let mut path = env::temp_dir();
    path.push(format!("tegdb_bench_{}_{}", prefix, std::process::id()));
    path
}

fn engine_benchmark(c: &mut Criterion) {
    let path = temp_db_path("engine");
    if path.exists() {
        fs::remove_file(&path).expect("Failed to remove existing test file");
    }
    let mut engine = Engine::new(path.clone()).expect("Failed to create engine");
    let key = b"key";
    let value = b"value";

    c.bench_function("engine set", |b| {
        b.iter(|| {
            engine.set(black_box(key), black_box(value.to_vec())).unwrap();
        })
    });

    c.bench_function("engine get", |b| {
        b.iter(|| {
            engine.get(black_box(key)).unwrap();
        })
    });

    c.bench_function("engine scan", |b| {
        let start_key = b"a";
        let end_key = b"z";
        b.iter(|| {
            black_box(
                engine
                    .scan(black_box(start_key.to_vec())..black_box(end_key.to_vec()))
                    .unwrap()
                    .collect::<Vec<_>>()
            );
        })
    });

    c.bench_function("engine del", |b| {
        b.iter(|| {
            engine.del(black_box(key)).unwrap();
        })
    });
    
    // Clean up test file at the end
    drop(engine); // Ensure the file is closed
    let _ = fs::remove_file(&path);
}

fn sled_benchmark(c: &mut Criterion) {
    let path = temp_db_path("sled_basic");
    let path_str = path.to_str().expect("Invalid path");
    if path.exists() {
        std::fs::remove_dir_all(path_str).unwrap_or_default();
    }
    let db = sled::open(path_str).unwrap();
    let key = b"key";
    let value = b"value";

    c.bench_function("sled insert", |b| {
        b.iter(|| {
            db.insert(black_box(key), black_box(value)).unwrap();
        })
    });

    c.bench_function("sled get", |b| {
        b.iter(|| {
            db.get(black_box(key)).unwrap();
        })
    });

    c.bench_function("sled scan", |b| {
        b.iter(|| {
            black_box(
                db.range::<&[u8], _>(black_box(b"a".as_ref())..black_box(b"z".as_ref()))
                    .map(|x| x.unwrap())
                    .collect::<Vec<_>>()
            );
        })
    });

    c.bench_function("sled remove", |b| {
        b.iter(|| {
            db.remove(black_box(key)).unwrap();
        })
    });

    // Clean up
    drop(db); // Ensure the database is closed
    std::fs::remove_dir_all(path_str).unwrap_or_default();
}



fn sqlite_benchmark(c: &mut Criterion) {
    let conn = Connection::open_in_memory().unwrap();
    conn.execute(
        "CREATE TABLE test (key BLOB PRIMARY KEY, value BLOB)",
        [],
    )
    .unwrap();
    let key = b"key";
    let value = b"value";

    c.bench_function("sqlite insert", |b| {
        b.iter(|| {
            conn.execute(
                "INSERT OR REPLACE INTO test VALUES (?, ?)",
                params![black_box(key), black_box(value)],
            )
            .unwrap();
        })
    });

    c.bench_function("sqlite get", |b| {
        b.iter(|| {
            let mut stmt = conn
                .prepare("SELECT value FROM test WHERE key = ?")
                .unwrap();
            let mut rows = stmt.query([black_box(key)]).unwrap();
            rows.next().unwrap();
        })
    });

    c.bench_function("sqlite scan", |b| {
        b.iter(|| {
            let mut stmt = conn
                .prepare("SELECT key, value FROM test WHERE key >= ? AND key <= ? ORDER BY key")
                .unwrap();
            let rows = stmt
                .query_map([black_box(b"a".as_slice()), black_box(b"z".as_slice())], |row| {
                    Ok((row.get::<_, Vec<u8>>(0).unwrap(), row.get::<_, Vec<u8>>(1).unwrap()))
                })
                .unwrap()
                .collect::<Result<Vec<_>, _>>()
                .unwrap();
            black_box(rows);
        })
    });

    c.bench_function("sqlite delete", |b| {
        b.iter(|| {
            conn.execute("DELETE FROM test WHERE key = ?", params![black_box(key)])
                .unwrap();
        })
    });
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

    // Benchmark INSERT operations
    c.bench_function("database insert", |b| {
        b.iter(|| {
            // Use a timestamp-based ID to ensure uniqueness
            let id = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos() as u64;
            db.execute(&format!(
                "INSERT INTO benchmark_test (id, name, value) VALUES ({}, 'test_{}', {})",
                black_box(id),
                black_box(id),
                black_box((id % 1000) * 10)
            )).unwrap();
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
            // Use a timestamp-based ID to ensure uniqueness  
            let id = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos() as u64;
            let mut tx = db.begin_transaction().unwrap();
            tx.execute(&format!(
                "INSERT INTO benchmark_test (id, name, value) VALUES ({}, 'tx_test_{}', {})",
                black_box(id),
                black_box(id),
                black_box((id % 1000) * 5)
            )).unwrap();
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
    let conn = Connection::open_in_memory().unwrap();
    conn.execute(
        "CREATE TABLE benchmark_test (id INTEGER PRIMARY KEY, name TEXT, value INTEGER)",
        [],
    ).unwrap();

    // Benchmark INSERT operations
    c.bench_function("sqlite sql insert", |b| {
        b.iter(|| {
            // Use a timestamp-based ID to ensure uniqueness
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
            // Use a timestamp-based ID to ensure uniqueness
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
}

criterion_group!(
    benches,
    engine_benchmark,
    sled_benchmark,
    sqlite_benchmark,
    database_benchmark,
    sqlite_sql_benchmark
);
criterion_main!(benches);
