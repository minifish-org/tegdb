use criterion::{criterion_group, criterion_main, Criterion};
use std::hint::black_box;
use std::env;
use std::fs;
use std::path::PathBuf;
use tegdb::StorageEngine;

/// Creates a unique temporary file path for benchmarks
fn temp_db_path(prefix: &str) -> PathBuf {
    let mut path = env::temp_dir();
    path.push(format!("tegdb_bench_{}_{}", prefix, std::process::id()));
    path
}

fn transaction_begin_scaling(c: &mut Criterion) {
    let sizes = [0, 10, 100, 1000, 10000];

    for &size in &sizes {
        let path = temp_db_path(&format!("begin_scaling_{size}"));
        if path.exists() {
            fs::remove_file(&path).expect("Failed to remove existing test file");
        }
        let mut engine = StorageEngine::new(path.clone()).expect("Failed to create engine");

        // Pre-populate with specified number of entries
        for i in 0..size {
            let key = format!("key{i}");
            let value = format!("value{i}");
            engine.set(key.as_bytes(), value.into_bytes()).unwrap();
        }

        c.bench_function(&format!("transaction begin with {size} entries"), |b| {
            b.iter(|| {
                let tx = black_box(engine.begin_transaction());
                drop(tx); // Rollback automatically on drop
            })
        });

        // Clean up test file
        drop(engine);
        let _ = fs::remove_file(&path);
    }
}

criterion_group!(scaling_benches, transaction_begin_scaling);
criterion_main!(scaling_benches);
