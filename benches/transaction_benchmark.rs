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

fn transaction_basic_operations(c: &mut Criterion) {
    let path = temp_db_path("transaction_basic");
    if path.exists() {
        fs::remove_file(&path).expect("Failed to remove existing test file");
    }
    let mut engine = Engine::new(path.clone()).expect("Failed to create engine");
    
    // Pre-populate with some data for get operations
    for i in 0..100 {
        let key = format!("key{}", i);
        let value = format!("value{}", i);
        engine.set(key.as_bytes(), value.into_bytes()).unwrap();
    }

    c.bench_function("transaction begin", |b| {
        b.iter(|| {
            let tx = black_box(engine.begin_transaction());
            drop(tx); // Rollback automatically on drop
        })
    });

    c.bench_function("transaction set", |b| {
        b.iter(|| {
            let mut tx = engine.begin_transaction();
            let key = b"tx_key";
            let value = b"tx_value";
            tx.set(black_box(key.to_vec()), black_box(value.to_vec())).unwrap();
            tx.rollback(); // Clean up
        })
    });

    c.bench_function("transaction get", |b| {
        let tx = engine.begin_transaction();
        let key = b"key0";
        b.iter(|| {
            let _ = black_box(tx.get(black_box(key)));
        })
    });

    c.bench_function("transaction delete", |b| {
        b.iter(|| {
            let mut tx = engine.begin_transaction();
            let key = b"tx_key";
            tx.delete(black_box(key.to_vec())).unwrap();
            tx.rollback(); // Clean up
        })
    });

    c.bench_function("transaction scan", |b| {
        let tx = engine.begin_transaction();
        b.iter(|| {
            let start = b"key0".to_vec();
            let end = b"key9".to_vec();
            let results = black_box(tx.scan(black_box(start..end)));
            black_box(results);
        })
    });

    c.bench_function("transaction commit empty", |b| {
        b.iter(|| {
            let mut tx = engine.begin_transaction();
            tx.commit().unwrap();
        })
    });

    c.bench_function("transaction rollback", |b| {
        b.iter(|| {
            let mut tx = engine.begin_transaction();
            let key = b"tx_key";
            let value = b"tx_value";
            tx.set(key.to_vec(), value.to_vec()).unwrap();
            black_box(tx.rollback());
        })
    });

    // Clean up test file
    drop(engine);
    let _ = fs::remove_file(&path);
}

fn transaction_batch_operations(c: &mut Criterion) {
    let path = temp_db_path("transaction_batch");
    if path.exists() {
        fs::remove_file(&path).expect("Failed to remove existing test file");
    }
    let mut engine = Engine::new(path.clone()).expect("Failed to create engine");

    // Benchmark small transaction (10 operations)
    c.bench_function("transaction commit small (10 ops)", |b| {
        b.iter(|| {
            let mut tx = engine.begin_transaction();
            for i in 0..10 {
                let key = format!("small_key{}", i);
                let value = format!("small_value{}", i);
                tx.set(black_box(key.into_bytes()), black_box(value.into_bytes())).unwrap();
            }
            tx.commit().unwrap();
        })
    });

    // Benchmark medium transaction (100 operations)
    c.bench_function("transaction commit medium (100 ops)", |b| {
        b.iter(|| {
            let mut tx = engine.begin_transaction();
            for i in 0..100 {
                let key = format!("medium_key{}", i);
                let value = format!("medium_value{}", i);
                tx.set(black_box(key.into_bytes()), black_box(value.into_bytes())).unwrap();
            }
            tx.commit().unwrap();
        })
    });

    // Benchmark large transaction (1000 operations)
    c.bench_function("transaction commit large (1000 ops)", |b| {
        b.iter(|| {
            let mut tx = engine.begin_transaction();
            for i in 0..1000 {
                let key = format!("large_key{}", i);
                let value = format!("large_value{}", i);
                tx.set(black_box(key.into_bytes()), black_box(value.into_bytes())).unwrap();
            }
            tx.commit().unwrap();
        })
    });

    // Clean up test file
    drop(engine);
    let _ = fs::remove_file(&path);
}

fn transaction_mixed_operations(c: &mut Criterion) {
    let path = temp_db_path("transaction_mixed");
    if path.exists() {
        fs::remove_file(&path).expect("Failed to remove existing test file");
    }
    let mut engine = Engine::new(path.clone()).expect("Failed to create engine");
    
    // Pre-populate with some data
    for i in 0..50 {
        let key = format!("existing_key{}", i);
        let value = format!("existing_value{}", i);
        engine.set(key.as_bytes(), value.into_bytes()).unwrap();
    }

    c.bench_function("transaction mixed operations", |b| {
        let mut counter = 0;
        b.iter(|| {
            let mut tx = engine.begin_transaction();
            
            // Mix of set, get, delete operations
            for i in 0..20 {
                let idx = (counter * 20 + i) % 50;
                
                // Set new key
                let new_key = format!("new_key{}_{}", counter, i);
                let new_value = format!("new_value{}_{}", counter, i);
                tx.set(black_box(new_key.into_bytes()), black_box(new_value.into_bytes())).unwrap();
                
                // Get existing key
                let existing_key = format!("existing_key{}", idx);
                let _ = black_box(tx.get(black_box(existing_key.as_bytes())));
                
                // Delete every 4th iteration
                if i % 4 == 0 {
                    let delete_key = format!("existing_key{}", idx);
                    tx.delete(black_box(delete_key.into_bytes())).unwrap();
                }
            }
            
            // Scan operation
            let start = b"existing_key0".to_vec();
            let end = b"existing_key9".to_vec();
            let _ = black_box(tx.scan(black_box(start..end)));
            
            tx.commit().unwrap();
            counter += 1;
        })
    });

    // Clean up test file
    drop(engine);
    let _ = fs::remove_file(&path);
}

fn transaction_conflict_scenarios(c: &mut Criterion) {
    let path = temp_db_path("transaction_conflict");
    if path.exists() {
        fs::remove_file(&path).expect("Failed to remove existing test file");
    }
    let mut engine = Engine::new(path.clone()).expect("Failed to create engine");
    
    // Pre-populate with data
    for i in 0..100 {
        let key = format!("conflict_key{}", i);
        let value = format!("original_value{}", i);
        engine.set(key.as_bytes(), value.into_bytes()).unwrap();
    }

    c.bench_function("transaction overwrite existing", |b| {
        let mut counter = 0;
        b.iter(|| {
            let mut tx = engine.begin_transaction();
            
            // Overwrite existing keys
            for i in 0..10 {
                let key = format!("conflict_key{}", i % 100);
                let value = format!("updated_value{}_{}", counter, i);
                tx.set(black_box(key.into_bytes()), black_box(value.into_bytes())).unwrap();
            }
            
            tx.commit().unwrap();
            counter += 1;
        })
    });

    c.bench_function("transaction read-modify-write", |b| {
        let mut counter = 0;
        b.iter(|| {
            let mut tx = engine.begin_transaction();
            
            // Read-modify-write pattern
            for i in 0..5 {
                let key = format!("conflict_key{}", i % 100);
                
                // Read current value
                let current = black_box(tx.get(black_box(key.as_bytes())));
                
                // Modify and write back
                let new_value = if let Some(val) = current {
                    format!("{}_modified_{}", String::from_utf8_lossy(&val), counter)
                } else {
                    format!("new_value_{}", counter)
                };
                
                tx.set(black_box(key.into_bytes()), black_box(new_value.into_bytes())).unwrap();
            }
            
            tx.commit().unwrap();
            counter += 1;
        })
    });

    // Clean up test file
    drop(engine);
    let _ = fs::remove_file(&path);
}

criterion_group!(
    transaction_benches,
    transaction_basic_operations,
    transaction_batch_operations,
    transaction_mixed_operations,
    transaction_conflict_scenarios
);
criterion_main!(transaction_benches);
