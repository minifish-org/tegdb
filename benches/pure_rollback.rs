use criterion::{black_box, criterion_group, criterion_main, Criterion};
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

fn pure_rollback_benchmark(c: &mut Criterion) {
    let path = temp_db_path("pure_rollback");
    if path.exists() {
        fs::remove_file(&path).expect("Failed to remove existing test file");
    }

    // Simple rollback with minimal setup
    c.bench_function("optimized rollback", |b| {
        b.iter(|| {
            let mut engine = StorageEngine::new(path.clone()).expect("Failed to create engine");
            let mut tx = engine.begin_transaction();
            // Add minimal operation to have something to rollback
            tx.set(b"k", b"v".to_vec()).unwrap();
            // Benchmark just the rollback
            let _ = black_box(tx.rollback());
        })
    });

    // Clean up test file
    let _ = fs::remove_file(&path);
}

criterion_group!(pure_rollback_benches, pure_rollback_benchmark);
criterion_main!(pure_rollback_benches);
