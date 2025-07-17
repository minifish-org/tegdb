use criterion::{criterion_group, criterion_main, Criterion};
use rusqlite::{params, Connection};
use std::env;
use std::fs;
use std::hint::black_box;
use std::path::PathBuf;
use tegdb::StorageEngine;

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
    let mut engine = StorageEngine::new(path.clone()).expect("Failed to create engine");
    let key = b"key";
    let value = b"value";

    c.bench_function("engine set", |b| {
        b.iter(|| {
            engine
                .set(black_box(key), black_box(value.to_vec()))
                .unwrap();
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
                    .collect::<Vec<_>>(),
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
                    .collect::<Vec<_>>(),
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
        "CREATE TABLE test (key TEXT(32) PRIMARY KEY, value TEXT(32))",
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
                .prepare("SELECT value FROM test WHERE key = ?1")
                .unwrap();
            let mut rows = stmt.query([black_box(key)]).unwrap();
            rows.next().unwrap();
        })
    });

    c.bench_function("sqlite scan", |b| {
        b.iter(|| {
            let mut stmt = conn
                .prepare("SELECT key, value FROM test WHERE key >= ?1 AND key <= ?2")
                .unwrap();
            let rows = stmt
                .query_map(
                    [black_box(b"a".as_slice()), black_box(b"z".as_slice())],
                    |row| {
                        Ok((
                            row.get::<_, Vec<u8>>(0).unwrap(),
                            row.get::<_, Vec<u8>>(1).unwrap(),
                        ))
                    },
                )
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

criterion_group!(benches, engine_benchmark, sled_benchmark, sqlite_benchmark);
criterion_main!(benches);
