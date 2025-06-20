use criterion::{black_box, criterion_group, criterion_main, Criterion};
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

fn engine_vs_transaction_comparison(c: &mut Criterion) {
    let path = temp_db_path("engine_vs_tx");
    if path.exists() {
        fs::remove_file(&path).expect("Failed to remove existing test file");
    }
    let mut engine = Engine::new(path.clone()).expect("Failed to create engine");
    
    // Pre-populate with some data for get/scan operations
    for i in 0..100 {
        let key = format!("key{}", i);
        let value = format!("value{}", i);
        engine.set(key.as_bytes(), value.into_bytes()).unwrap();
    }

    let key = b"benchmark_key";
    let value = b"benchmark_value";

    // ===== SET Operation Comparison =====
    c.bench_function("engine set", |b| {
        b.iter(|| {
            engine.set(black_box(key), black_box(value.to_vec())).unwrap();
        })
    });

    c.bench_function("transaction set (no commit)", |b| {
        b.iter(|| {
            let mut tx = engine.begin_transaction();
            tx.set(black_box(key.to_vec()), black_box(value.to_vec())).unwrap();
            // Note: not committing to isolate just the set operation
            tx.rollback();
        })
    });

    c.bench_function("transaction set + commit", |b| {
        b.iter(|| {
            let mut tx = engine.begin_transaction();
            tx.set(black_box(key.to_vec()), black_box(value.to_vec())).unwrap();
            tx.commit().unwrap();
        })
    });

    // ===== GET Operation Comparison =====
    c.bench_function("engine get", |b| {
        b.iter(|| {
            let _ = black_box(engine.get(black_box(key)));
        })
    });

    c.bench_function("transaction get", |b| {
        let tx = engine.begin_transaction();
        b.iter(|| {
            let _ = black_box(tx.get(black_box(key)));
        })
    });

    // ===== DELETE Operation Comparison =====
    c.bench_function("engine delete", |b| {
        b.iter(|| {
            engine.del(black_box(key)).unwrap();
        })
    });

    c.bench_function("transaction delete (no commit)", |b| {
        b.iter(|| {
            let mut tx = engine.begin_transaction();
            tx.delete(black_box(key.to_vec())).unwrap();
            // Note: not committing to isolate just the delete operation
            tx.rollback();
        })
    });

    c.bench_function("transaction delete + commit", |b| {
        b.iter(|| {
            let mut tx = engine.begin_transaction();
            tx.delete(black_box(key.to_vec())).unwrap();
            tx.commit().unwrap();
        })
    });

    // ===== SCAN Operation Comparison =====
    c.bench_function("engine scan", |b| {
        let start_key = b"key0".to_vec();
        let end_key = b"key9".to_vec();
        b.iter(|| {
            let iter = engine.scan(black_box(start_key.clone()..end_key.clone())).unwrap();
            let results: Vec<_> = iter.collect();
            black_box(results);
        })
    });

    c.bench_function("transaction scan", |b| {
        let tx = engine.begin_transaction();
        b.iter(|| {
            let start_key = b"key0".to_vec();
            let end_key = b"key9".to_vec();
            let results = black_box(tx.scan(black_box(start_key..end_key)));
            black_box(results);
        })
    });

    // Clean up test file
    drop(engine);
    let _ = fs::remove_file(&path);
}

fn batch_operations_comparison(c: &mut Criterion) {
    let path = temp_db_path("batch_comparison");
    if path.exists() {
        fs::remove_file(&path).expect("Failed to remove existing test file");
    }
    let mut engine = Engine::new(path.clone()).expect("Failed to create engine");

    let batch_sizes = [1, 10, 100];

    for &size in &batch_sizes {
        // ===== Engine Individual Operations vs Transaction Batch =====
        c.bench_function(&format!("engine individual ops ({})", size), |b| {
            b.iter(|| {
                for i in 0..size {
                    let key = format!("batch_key{}", i);
                    let value = format!("batch_value{}", i);
                    engine.set(black_box(key.as_bytes()), black_box(value.into_bytes())).unwrap();
                }
            })
        });

        c.bench_function(&format!("transaction batch commit ({})", size), |b| {
            b.iter(|| {
                let mut tx = engine.begin_transaction();
                for i in 0..size {
                    let key = format!("batch_key{}", i);
                    let value = format!("batch_value{}", i);
                    tx.set(black_box(key.into_bytes()), black_box(value.into_bytes())).unwrap();
                }
                tx.commit().unwrap();
            })
        });
    }

    // Clean up test file
    drop(engine);
    let _ = fs::remove_file(&path);
}

fn transaction_overhead_analysis(c: &mut Criterion) {
    let path = temp_db_path("overhead_analysis");
    if path.exists() {
        fs::remove_file(&path).expect("Failed to remove existing test file");
    }
    let mut engine = Engine::new(path.clone()).expect("Failed to create engine");

    // ===== Transaction overhead analysis =====
    c.bench_function("transaction overhead: begin + rollback", |b| {
        b.iter(|| {
            let tx = black_box(engine.begin_transaction());
            drop(tx); // Automatic rollback
        })
    });

    c.bench_function("transaction overhead: begin + empty commit", |b| {
        b.iter(|| {
            let mut tx = engine.begin_transaction();
            tx.commit().unwrap();
        })
    });

    // ===== Compare transaction operations in isolation =====
    c.bench_function("isolated: engine set", |b| {
        b.iter(|| {
            engine.set(b"isolated_key", b"isolated_value".to_vec()).unwrap();
        })
    });

    c.bench_function("isolated: transaction set only", |b| {
        b.iter(|| {
            let mut tx = engine.begin_transaction();
            tx.set(b"isolated_key".to_vec(), b"isolated_value".to_vec()).unwrap();
            // Don't commit, just measure the set operation in transaction context
            tx.rollback();
        })
    });

    // Clean up test file
    drop(engine);
    let _ = fs::remove_file(&path);
}

criterion_group!(
    engine_vs_transaction_benches,
    engine_vs_transaction_comparison,
    batch_operations_comparison,
    transaction_overhead_analysis
);
criterion_main!(engine_vs_transaction_benches);
