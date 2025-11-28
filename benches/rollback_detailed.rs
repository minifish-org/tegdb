use criterion::{criterion_group, criterion_main, Criterion};
use std::env;
use std::fs;
use std::hint::black_box;
use std::path::PathBuf;
use tegdb::storage_engine::StorageEngine;

/// Creates a unique temporary file path for benchmarks
fn temp_db_path(prefix: &str) -> PathBuf {
    let mut path = env::temp_dir();
    path.push(format!("tegdb_bench_{}_{}", prefix, std::process::id()));
    path
}

fn rollback_benchmark_detailed(c: &mut Criterion) {
    let path = temp_db_path("rollback_detailed");
    if path.exists() {
        fs::remove_file(&path).expect("Failed to remove existing test file");
    }
    let mut engine = StorageEngine::new(path.clone()).expect("Failed to create engine");

    // Benchmark rollback with different numbers of pending operations
    let sizes = [0, 1, 10, 100, 1000];

    for &size in &sizes {
        c.bench_function(&format!("rollback with {size} pending ops"), |b| {
            b.iter(|| {
                let mut tx = engine.begin_transaction();

                // Add some pending operations
                for i in 0..size {
                    let key = format!("pending_key{i}");
                    let value = format!("pending_value{i}");
                    tx.set(black_box(key.as_bytes()), black_box(value.into_bytes()))
                        .unwrap();
                }

                // Now benchmark the rollback
                let _ = black_box(tx.rollback());
            })
        });
    }

    // Benchmark just state change (minimal rollback)
    c.bench_function("rollback state change only", |b| {
        b.iter(|| {
            let mut tx = engine.begin_transaction();
            // Don't add any operations, just rollback
            let _ = black_box(tx.rollback());
        })
    });

    // Benchmark automatic rollback on drop
    c.bench_function("automatic rollback on drop", |b| {
        b.iter(|| {
            let mut tx = engine.begin_transaction();
            // Add one operation
            tx.set(b"key", b"value".to_vec()).unwrap();
            // Drop without explicit rollback - should auto-rollback
            drop(tx);
            black_box(());
        })
    });

    // Clean up test file
    drop(engine);
    let _ = fs::remove_file(&path);
}

criterion_group!(rollback_benches, rollback_benchmark_detailed);
criterion_main!(rollback_benches);
