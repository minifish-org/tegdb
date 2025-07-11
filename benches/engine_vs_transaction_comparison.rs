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

/// Benchmark comparing direct engine operations vs transaction-wrapped operations
///
/// Fairness improvements made:
/// 1. Transaction creation/cleanup moved inside benchmark loops for GET operations
/// 2. Consistent data preparation for DELETE operations
/// 3. Consistent result collection for SCAN operations  
/// 4. Pre-population of test data for realistic scenarios
/// 5. Explicit transaction cleanup to prevent resource leaks
/// 6. Separate engines for raw KV operations vs transactional operations to prevent cross-contamination
/// 7. API consistency: Always use low-level API for raw engine, always use high-level transactional API for tx engine
///    This applies to both data setup and benchmarks to reflect real-world usage patterns
/// 8. Optimized tx engine setup to use batched transactions for efficiency while maintaining API consistency
fn engine_vs_transaction_comparison(c: &mut Criterion) {
    // ===== SET Operation Comparison =====
    {
        let raw_path = temp_db_path("engine_raw_kv_set");
        let tx_path = temp_db_path("engine_tx_set");
        let mut raw_engine = create_and_populate_raw_engine(&raw_path);
        let mut tx_engine = create_and_populate_tx_engine(&tx_path);

        let key = b"benchmark_key";
        let value = b"benchmark_value";

        c.bench_function("engine set", |b| {
            b.iter(|| {
                raw_engine
                    .set(black_box(key), black_box(value.to_vec()))
                    .unwrap();
            })
        });

        c.bench_function("transaction set (no commit)", |b| {
            b.iter(|| {
                let mut tx = tx_engine.begin_transaction();
                tx.set(black_box(key), black_box(value.to_vec())).unwrap();
                // Note: not committing to isolate just the set operation
                let _ = tx.rollback();
            })
        });

        c.bench_function("transaction set + commit", |b| {
            b.iter(|| {
                let mut tx = tx_engine.begin_transaction();
                tx.set(black_box(key), black_box(value.to_vec())).unwrap();
                tx.commit().unwrap();
            })
        });

        // Clean up
        drop(raw_engine);
        drop(tx_engine);
        let _ = fs::remove_file(&raw_path);
        let _ = fs::remove_file(&tx_path);
    }

    // ===== GET Operation Comparison =====
    {
        let raw_path = temp_db_path("engine_raw_kv_get");
        let tx_path = temp_db_path("engine_tx_get");
        let raw_engine = create_and_populate_raw_engine(&raw_path);
        let mut tx_engine = create_and_populate_tx_engine(&tx_path);

        let key = b"benchmark_key";

        c.bench_function("engine get", |b| {
            b.iter(|| {
                let _ = black_box(raw_engine.get(black_box(key)));
            })
        });

        c.bench_function("transaction get", |b| {
            b.iter(|| {
                let tx = tx_engine.begin_transaction();
                let _ = black_box(tx.get(black_box(key)));
                drop(tx); // Explicit cleanup
            })
        });

        // Clean up
        drop(raw_engine);
        drop(tx_engine);
        let _ = fs::remove_file(&raw_path);
        let _ = fs::remove_file(&tx_path);
    }

    // ===== DELETE Operation Comparison =====
    {
        let raw_path = temp_db_path("engine_raw_kv_del");
        let tx_path = temp_db_path("engine_tx_del");
        let mut raw_engine = create_and_populate_raw_engine(&raw_path);
        let mut tx_engine = create_and_populate_tx_engine(&tx_path);

        let key = b"benchmark_key";

        c.bench_function("engine delete", |b| {
            b.iter(|| {
                // Ensure key exists before deleting
                raw_engine.del(black_box(key)).unwrap();
            })
        });

        c.bench_function("transaction delete (no commit)", |b| {
            b.iter(|| {
                let mut tx = tx_engine.begin_transaction();
                tx.delete(black_box(key)).unwrap();
                // Note: not committing to isolate just the delete operation
                let _ = tx.rollback();
            })
        });

        c.bench_function("transaction delete + commit", |b| {
            b.iter(|| {
                let mut tx = tx_engine.begin_transaction();
                tx.delete(black_box(key)).unwrap();
                tx.commit().unwrap();
            })
        });

        // Clean up
        drop(raw_engine);
        drop(tx_engine);
        let _ = fs::remove_file(&raw_path);
        let _ = fs::remove_file(&tx_path);
    }

    // ===== SCAN Operation Comparison =====
    {
        let raw_path = temp_db_path("engine_raw_kv_scan");
        let tx_path = temp_db_path("engine_tx_scan");
        let raw_engine = create_and_populate_raw_engine(&raw_path);
        let mut tx_engine = create_and_populate_tx_engine(&tx_path);

        c.bench_function("engine scan", |b| {
            let start_key = b"key0".to_vec();
            let end_key = b"key9".to_vec();
            b.iter(|| {
                let iter = raw_engine
                    .scan(black_box(start_key.clone()..end_key.clone()))
                    .unwrap();
                let results: Vec<_> = iter.collect();
                black_box(results);
            })
        });

        c.bench_function("transaction scan", |b| {
            b.iter(|| {
                let tx = tx_engine.begin_transaction();
                let start_key = b"key0".to_vec();
                let end_key = b"key9".to_vec();
                let iter = tx.scan(black_box(start_key..end_key)).unwrap();
                let results: Vec<_> = iter.collect();
                black_box(results);
                drop(tx); // Explicit cleanup
            })
        });

        // Clean up
        drop(raw_engine);
        drop(tx_engine);
        let _ = fs::remove_file(&raw_path);
        let _ = fs::remove_file(&tx_path);
    }
}

/// Helper function to create and populate a raw engine with test data
fn create_and_populate_raw_engine(path: &PathBuf) -> StorageEngine {
    // Clean up any existing file
    if path.exists() {
        fs::remove_file(path).expect("Failed to remove existing raw test file");
    }

    let mut engine = StorageEngine::new(path.clone()).expect("Failed to create raw engine");

    // Pre-populate with test data using low-level API
    for i in 0..100 {
        let key = format!("key{i}");
        let value = format!("value{i}");
        engine
            .set(key.as_bytes(), value.as_bytes().to_vec())
            .unwrap();
    }

    let key = b"benchmark_key";
    let value = b"benchmark_value";
    engine.set(key, value.to_vec()).unwrap();

    engine
}

/// Helper function to create and populate a tx engine with test data using transactions
fn create_and_populate_tx_engine(path: &PathBuf) -> StorageEngine {
    // Clean up any existing file
    if path.exists() {
        fs::remove_file(path).expect("Failed to remove existing tx test file");
    }

    let mut engine = StorageEngine::new(path.clone()).expect("Failed to create tx engine");

    // Pre-populate with test data using high-level transactional API
    // Batch all operations in a single transaction for efficiency
    {
        let mut tx = engine.begin_transaction();

        // Add 100 test keys
        for i in 0..100 {
            let key = format!("key{i}");
            let value = format!("value{i}");
            tx.set(key.as_bytes(), value.as_bytes().to_vec()).unwrap();
        }

        // Add the benchmark key
        let key = b"benchmark_key";
        let value = b"benchmark_value";
        tx.set(key, value.to_vec()).unwrap();

        tx.commit().unwrap();
    } // tx is dropped here, releasing the borrow

    engine
}

fn batch_operations_comparison(c: &mut Criterion) {
    // Create separate engines for batch operations comparison
    let raw_batch_path = temp_db_path("batch_raw");
    let tx_batch_path = temp_db_path("batch_tx");

    // Clean up any existing files
    if raw_batch_path.exists() {
        fs::remove_file(&raw_batch_path).expect("Failed to remove existing raw batch test file");
    }
    if tx_batch_path.exists() {
        fs::remove_file(&tx_batch_path).expect("Failed to remove existing tx batch test file");
    }

    let mut raw_engine =
        StorageEngine::new(raw_batch_path.clone()).expect("Failed to create raw batch engine");
    let mut tx_engine =
        StorageEngine::new(tx_batch_path.clone()).expect("Failed to create tx batch engine");

    let batch_sizes = [1, 10, 100];

    for &size in &batch_sizes {
        // ===== Engine Individual Operations vs Transaction Batch =====
        c.bench_function(&format!("engine individual ops ({size})"), |b| {
            b.iter(|| {
                for i in 0..size {
                    let key = format!("batch_key{i}");
                    let value = format!("batch_value{i}");
                    raw_engine
                        .set(black_box(key.as_bytes()), black_box(value.into_bytes()))
                        .unwrap();
                }
            })
        });

        c.bench_function(&format!("transaction batch commit ({size})"), |b| {
            b.iter(|| {
                let mut tx = tx_engine.begin_transaction();
                for i in 0..size {
                    let key = format!("batch_key{i}");
                    let value = format!("batch_value{i}");
                    tx.set(black_box(key.as_bytes()), black_box(value.into_bytes()))
                        .unwrap();
                }
                tx.commit().unwrap();
            })
        });

        // ===== Mixed operations comparison =====
        c.bench_function(&format!("engine mixed ops ({size})"), |b| {
            b.iter(|| {
                for i in 0..size {
                    let key = format!("mixed_key{i}");
                    let value = format!("mixed_value{i}");
                    raw_engine
                        .set(black_box(key.as_bytes()), black_box(value.into_bytes()))
                        .unwrap();
                    let _ = black_box(raw_engine.get(black_box(key.as_bytes())));
                    if i % 2 == 0 {
                        raw_engine.del(black_box(key.as_bytes())).unwrap();
                    }
                }
            })
        });

        c.bench_function(&format!("transaction mixed ops ({size})"), |b| {
            b.iter(|| {
                let mut tx = tx_engine.begin_transaction();
                for i in 0..size {
                    let key = format!("mixed_key{i}");
                    let value = format!("mixed_value{i}");
                    tx.set(black_box(key.as_bytes()), black_box(value.into_bytes()))
                        .unwrap();
                    let _ = black_box(tx.get(black_box(key.as_bytes())));
                    if i % 2 == 0 {
                        tx.delete(black_box(key.as_bytes())).unwrap();
                    }
                }
                tx.commit().unwrap();
            })
        });
    }

    // Clean up test files
    drop(raw_engine);
    drop(tx_engine);
    let _ = fs::remove_file(&raw_batch_path);
    let _ = fs::remove_file(&tx_batch_path);
}

fn transaction_overhead_analysis(c: &mut Criterion) {
    // Create separate engines for overhead analysis
    let raw_overhead_path = temp_db_path("overhead_raw");
    let tx_overhead_path = temp_db_path("overhead_tx");

    // Clean up any existing files
    if raw_overhead_path.exists() {
        fs::remove_file(&raw_overhead_path)
            .expect("Failed to remove existing raw overhead test file");
    }
    if tx_overhead_path.exists() {
        fs::remove_file(&tx_overhead_path)
            .expect("Failed to remove existing tx overhead test file");
    }

    let mut raw_engine = StorageEngine::new(raw_overhead_path.clone())
        .expect("Failed to create raw overhead engine");
    let mut tx_engine =
        StorageEngine::new(tx_overhead_path.clone()).expect("Failed to create tx overhead engine");

    // Pre-populate both engines with test data
    // Note: Using appropriate APIs - low-level for raw engine, high-level for tx engine

    // Raw engine uses low-level API
    for i in 0..50 {
        let key = format!("overhead_key{i}");
        let value = format!("overhead_value{i}");
        raw_engine
            .set(key.as_bytes(), value.as_bytes().to_vec())
            .unwrap();
    }

    // TX engine uses high-level transactional API - batch multiple operations for efficiency
    {
        let mut tx = tx_engine.begin_transaction();
        for i in 0..50 {
            let key = format!("overhead_key{i}");
            let value = format!("overhead_value{i}");
            tx.set(key.as_bytes(), value.as_bytes().to_vec()).unwrap();
        }
        tx.commit().unwrap();
    }

    // ===== Transaction overhead analysis =====
    c.bench_function("transaction overhead: begin + rollback", |b| {
        b.iter(|| {
            let tx = black_box(tx_engine.begin_transaction());
            drop(tx); // Automatic rollback
        })
    });

    c.bench_function("transaction overhead: begin + empty commit", |b| {
        b.iter(|| {
            let mut tx = tx_engine.begin_transaction();
            tx.commit().unwrap();
        })
    });

    // ===== Compare read operations =====
    let test_key = b"overhead_key25";

    c.bench_function("direct engine get", |b| {
        b.iter(|| {
            let _ = black_box(raw_engine.get(black_box(test_key)));
        })
    });

    c.bench_function("transaction wrapped get", |b| {
        b.iter(|| {
            let tx = tx_engine.begin_transaction();
            let _ = black_box(tx.get(black_box(test_key)));
            drop(tx);
        })
    });

    // Note: SET operation benchmarks are already covered in engine_vs_transaction_comparison function
    // Removed duplicated "engine set lifecycle", "transaction set lifecycle",
    // "transaction rollback after set", and "transaction commit after set" benchmarks

    // Clean up test files
    drop(raw_engine);
    drop(tx_engine);
    let _ = fs::remove_file(&raw_overhead_path);
    let _ = fs::remove_file(&tx_overhead_path);
}

fn error_and_edge_case_comparison(c: &mut Criterion) {
    // Create separate engines for error case testing
    let raw_error_path = temp_db_path("error_raw");
    let tx_error_path = temp_db_path("error_tx");

    // Clean up any existing files
    if raw_error_path.exists() {
        fs::remove_file(&raw_error_path).expect("Failed to remove existing raw error test file");
    }
    if tx_error_path.exists() {
        fs::remove_file(&tx_error_path).expect("Failed to remove existing tx error test file");
    }

    let mut raw_engine =
        StorageEngine::new(raw_error_path.clone()).expect("Failed to create raw error engine");
    let mut tx_engine =
        StorageEngine::new(tx_error_path.clone()).expect("Failed to create tx error engine");

    // ===== Operations on non-existent keys =====
    let nonexistent_key = b"nonexistent_key_12345";

    c.bench_function("engine get nonexistent", |b| {
        b.iter(|| {
            let _ = black_box(raw_engine.get(black_box(nonexistent_key)));
        })
    });

    c.bench_function("transaction get nonexistent", |b| {
        b.iter(|| {
            let tx = tx_engine.begin_transaction();
            let _ = black_box(tx.get(black_box(nonexistent_key)));
            drop(tx);
        })
    });

    // ===== Delete operations on nonexistent keys =====
    c.bench_function("engine delete nonexistent", |b| {
        b.iter(|| {
            let _ = black_box(raw_engine.del(black_box(nonexistent_key)));
        })
    });

    c.bench_function("transaction delete nonexistent", |b| {
        b.iter(|| {
            let mut tx = tx_engine.begin_transaction();
            let _ = black_box(tx.delete(black_box(nonexistent_key)));
            let _ = tx.rollback();
        })
    });

    // ===== Scan empty ranges =====
    c.bench_function("engine scan empty range", |b| {
        let start_key = b"zzz_nonexistent_start".to_vec();
        let end_key = b"zzz_nonexistent_end".to_vec();
        b.iter(|| {
            let iter = raw_engine
                .scan(black_box(start_key.clone()..end_key.clone()))
                .unwrap();
            let results: Vec<_> = iter.collect();
            black_box(results);
        })
    });

    c.bench_function("transaction scan empty range", |b| {
        b.iter(|| {
            let tx = tx_engine.begin_transaction();
            let start_key = b"zzz_nonexistent_start".to_vec();
            let end_key = b"zzz_nonexistent_end".to_vec();
            let iter = tx.scan(black_box(start_key..end_key)).unwrap();
            let results: Vec<_> = iter.collect();
            black_box(results);
            drop(tx);
        })
    });

    // Clean up test files
    drop(raw_engine);
    drop(tx_engine);
    let _ = fs::remove_file(&raw_error_path);
    let _ = fs::remove_file(&tx_error_path);
}

criterion_group!(
    engine_vs_transaction_benches,
    engine_vs_transaction_comparison,
    batch_operations_comparison,
    transaction_overhead_analysis,
    error_and_edge_case_comparison
);
criterion_main!(engine_vs_transaction_benches);
