use criterion::{criterion_group, criterion_main, Criterion};
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

fn engine_benchmark(c: &mut Criterion, value_size: usize) {
    let path = temp_db_path(&format!("seq_{value_size}"));
    if path.exists() {
        fs::remove_file(&path).expect("Failed to remove existing test file");
    }
    let mut engine = StorageEngine::new(path.clone()).expect("Failed to create engine");
    let value = vec![0; value_size];

    // Insert some test data before running get benchmarks
    for i in 0..1000 {
        let key_str = format!("key{i}");
        let key = key_str.as_bytes();
        engine.set(key, value.to_vec()).unwrap();
    }

    c.bench_function(&format!("engine seq set {value_size}"), |b| {
        let mut i = 0;
        b.iter(|| {
            let key_str = format!("key{i}");
            let key = key_str.as_bytes();
            engine
                .set(black_box(key), black_box(value.to_vec()))
                .unwrap();
            i += 1;
        })
    });

    c.bench_function(&format!("engine seq get {value_size}"), |b| {
        let mut i = 0;
        b.iter(|| {
            let key_str = format!("key{}", i % 1000); // Cycle through the 1000 keys we added
            let key = key_str.as_bytes();
            let _ = black_box(engine.get(black_box(key)));
            i += 1;
        })
    });

    // Add sequential scan benchmark for engine
    c.bench_function(&format!("engine seq scan {value_size}"), |b| {
        b.iter(|| {
            let start = format!("key{}", 0).into_bytes();
            let end = format!("key{}", 1000).into_bytes();
            let iter = engine.scan(black_box(start..end)).unwrap();
            let result: Vec<_> = iter.collect();
            black_box(result);
        })
    });

    c.bench_function(&format!("engine seq del {value_size}"), |b| {
        let mut i = 0;
        b.iter(|| {
            let key_str = format!("key{}", i % 1000); // Cycle through the 1000 keys we added
            let key = key_str.as_bytes();
            let _ = engine.del(black_box(key)); // Just benchmark the operation, don't unwrap
            i += 1;
        })
    });

    // Clean up the test file after benchmarks
    drop(engine); // Ensure the file is closed
    let _ = fs::remove_file(&path);
}

fn sled_benchmark(c: &mut Criterion, value_size: usize) {
    let path = temp_db_path(&format!("sled_seq_{value_size}"));
    let path_str = path.to_str().expect("Invalid path");
    if path.exists() {
        std::fs::remove_dir_all(path_str).unwrap_or_default();
    }
    let db = sled::open(path_str).unwrap();
    let value = vec![0; value_size];

    // Insert some test data before running get benchmarks
    for i in 0..1000 {
        let key_str = format!("key{i}");
        let key = key_str.as_bytes();
        db.insert(key, value.as_slice()).unwrap();
    }

    c.bench_function(&format!("sled seq insert {value_size}"), |b| {
        let mut i = 0;
        b.iter(|| {
            let key_str = format!("key{i}");
            let key = key_str.as_bytes();
            db.insert(black_box(key), black_box(value.as_slice()))
                .unwrap();
            i += 1;
        })
    });

    c.bench_function(&format!("sled seq get {value_size}"), |b| {
        let mut i = 0;
        b.iter(|| {
            let key_str = format!("key{}", i % 1000); // Cycle through the 1000 keys we added
            let key = key_str.as_bytes();
            let _ = black_box(db.get(black_box(key)));
            i += 1;
        })
    });

    // Add sequential scan benchmark for sled
    c.bench_function(&format!("sled seq scan {value_size}"), |b| {
        b.iter(|| {
            let start = format!("key{}", 0).into_bytes();
            let end = format!("key{}", 1000).into_bytes();
            let result: Vec<_> = db
                .range(black_box(start..end))
                .map(|r| r.unwrap())
                .collect();
            black_box(result);
        })
    });

    c.bench_function(&format!("sled seq remove {value_size}"), |b| {
        let mut i = 0;
        b.iter(|| {
            let key_str = format!("key{}", i % 1000); // Cycle through the 1000 keys we added
            let key = key_str.as_bytes();
            let _ = db.remove(black_box(key));
            i += 1;
        })
    });

    drop(db); // Ensure the database is closed
    std::fs::remove_dir_all(path_str).unwrap_or_default();
}

fn benchmark_small(c: &mut Criterion) {
    engine_benchmark(c, 1024);
    sled_benchmark(c, 1024);
}

fn benchmark_large(c: &mut Criterion) {
    engine_benchmark(c, 255_000);
    sled_benchmark(c, 255_000);
}

criterion_group!(benches, benchmark_small, benchmark_large);
criterion_main!(benches);
