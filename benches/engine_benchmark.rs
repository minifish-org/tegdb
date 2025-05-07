//! Benchmark tests for TegDB engine operations using Criterion.

use criterion::{black_box, criterion_group, criterion_main, Criterion, Throughput};
use rand::distr::Alphanumeric;
use rand::Rng;
use std::fs;
use std::path::PathBuf;
use tegdb::Engine;
use tokio::runtime::Runtime;

fn engine_benchmark(c: &mut Criterion) {
    let rt = Runtime::new().unwrap();
    let engine = Engine::new(PathBuf::from("bench_test.db"));
    let key = b"key";
    let value = b"value";

    let mut group = c.benchmark_group("engine_basic");
    group.warm_up_time(std::time::Duration::from_secs(5));
    group.measurement_time(std::time::Duration::from_secs(10));
    group.sample_size(100);
    group.throughput(Throughput::Elements(1));

    // Benchmark for set operation.
    group.bench_function("set", |b| {
        b.iter(|| {
            rt.block_on(async {
                engine
                    .set(black_box(key), black_box(value.to_vec()))
                    .await
                    .unwrap();
            });
        })
    });

    // Benchmark for get operation.
    group.bench_function("get", |b| {
        b.iter(|| {
            rt.block_on(async {
                engine.get(black_box(key)).await.unwrap();
            });
        })
    });

    // Benchmark for scan operation.
    group.bench_function("scan", |b| {
        let start_key = b"a";
        let end_key = b"z";
        b.iter(|| {
            rt.block_on(async {
                let _ = engine
                    .scan(black_box(start_key.to_vec())..black_box(end_key.to_vec()))
                    .await
                    .unwrap();
            });
        })
    });

    // New: Benchmark for reverse_scan operation.
    group.bench_function("reverse_scan", |b| {
        let start_key = b"a";
        let end_key = b"z";
        b.iter(|| {
            rt.block_on(async {
                let _ = engine
                    .reverse_scan(black_box(start_key.to_vec())..black_box(end_key.to_vec()))
                    .await
                    .unwrap();
            });
        })
    });

    // Benchmark for delete operation.
    group.bench_function("del", |b| {
        b.iter(|| {
            rt.block_on(async {
                engine.del(black_box(key)).await.unwrap();
            });
        })
    });

    group.finish();
    drop(engine);
    fs::remove_dir_all("bench_test.db").unwrap();
}

/// Sequential benchmark tests using keys with varying value sizes.
async fn engine_seq_benchmark(c: &mut Criterion, value_size: usize) {
    let engine = Engine::new(PathBuf::from("test_seq_bench.db"));
    let value = vec![0; value_size];

    let mut group = c.benchmark_group(format!("engine_seq_{}", value_size));
    group.warm_up_time(std::time::Duration::from_secs(5));
    group.measurement_time(std::time::Duration::from_secs(10));
    group.sample_size(100);
    group.throughput(Throughput::Elements(1));

    // Sequential benchmark for set.
    group.bench_function("set", |b| {
        let mut i = 0;
        b.iter(|| {
            let key_str = format!("key{}", i);
            let key = key_str.as_bytes();
            tokio::task::block_in_place(|| {
                tokio::runtime::Handle::current().block_on(async {
                    engine
                        .set(black_box(key), black_box(value.to_vec()))
                        .await
                        .unwrap();
                });
            });
            i += 1;
        })
    });

    // Sequential benchmark for get.
    group.bench_function("get", |b| {
        let mut i = 0;
        b.iter(|| {
            let key_str = format!("key{}", i);
            let key = key_str.as_bytes();
            tokio::task::block_in_place(|| {
                tokio::runtime::Handle::current().block_on(async {
                    let _ = engine.get(black_box(key)).await.unwrap_or_default();
                });
            });
            i += 1;
        })
    });

    // Sequential benchmark for delete.
    group.bench_function("del", |b| {
        let mut i = 0;
        b.iter(|| {
            let key_str = format!("key{}", i);
            let key = key_str.as_bytes();
            tokio::task::block_in_place(|| {
                tokio::runtime::Handle::current().block_on(async {
                    engine.del(black_box(key)).await.unwrap();
                });
            });
            i += 1;
        })
    });

    group.finish();
    drop(engine);
    fs::remove_dir_all("test_seq_bench.db").unwrap();
}

/// Benchmark with a short value size.
fn engine_short_benchmark(c: &mut Criterion) {
    let value_size = 1024;
    let rt = Runtime::new().unwrap();
    rt.block_on(engine_seq_benchmark(c, value_size));
}

/// Benchmark with a long value size.
fn engine_long_benchmark(c: &mut Criterion) {
    let value_size = 255_000;
    let rt = Runtime::new().unwrap();
    rt.block_on(engine_seq_benchmark(c, value_size));
}

/// Benchmark concurrent operations.
fn engine_concurrency_benchmark(c: &mut Criterion) {
    let rt = Runtime::new().unwrap();
    let mut group = c.benchmark_group("engine_concurrent");
    group.warm_up_time(std::time::Duration::from_secs(5));
    group.measurement_time(std::time::Duration::from_secs(10));
    group.sample_size(100);
    group.throughput(Throughput::Elements(4));

    // Remove concurrent.db once before running the benchmarks.
    std::fs::remove_dir_all("concurrent.db").ok();

    // Concurrent benchmark for set.
    group.bench_function("set", |b| {
        // Create engine once outside the timed iteration.
        let engine = Engine::new(PathBuf::from("concurrent.db"));
        b.iter(|| {
            rt.block_on(async {
                let mut tasks = Vec::new();
                for _ in 0..4 {
                    let key: String = rand::rng()
                        .sample_iter(&Alphanumeric)
                        .take(8)
                        .map(char::from)
                        .collect();
                    let value: Vec<u8> = (0..10).map(|_| rand::rng().random()).collect();
                    let engine_clone = engine.clone();
                    tasks.push(tokio::spawn(async move {
                        engine_clone
                            .set(key.as_bytes(), value)
                            .await
                            .unwrap_or_default();
                    }));
                }
                for t in tasks {
                    t.await.unwrap();
                }
            });
        });
        drop(engine);
        fs::remove_dir_all("concurrent.db").unwrap();
    });

    // Concurrent benchmark for get.
    group.bench_function("get", |b| {
        let engine = Engine::new(PathBuf::from("concurrent.db"));
        b.iter(|| {
            rt.block_on(async {
                let mut tasks = Vec::new();
                for _ in 0..4 {
                    let key: String = rand::rng()
                        .sample_iter(&Alphanumeric)
                        .take(8)
                        .map(char::from)
                        .collect();
                    let engine_clone = engine.clone();
                    tasks.push(tokio::spawn(async move {
                        engine_clone.get(key.as_bytes()).await.unwrap_or_default();
                    }));
                }
                for t in tasks {
                    t.await.unwrap();
                }
            });
        });
        drop(engine);
        fs::remove_dir_all("concurrent.db").unwrap();
    });

    // Concurrent benchmark for scan.
    group.bench_function("scan", |b| {
        let engine = Engine::new(PathBuf::from("concurrent.db"));
        b.iter(|| {
            rt.block_on(async {
                let mut tasks = Vec::new();
                for _ in 0..4 {
                    let engine_clone = engine.clone();
                    tasks.push(tokio::spawn(async move {
                        let _ = engine_clone
                            .scan(b"a".to_vec()..b"z".to_vec())
                            .await
                            .unwrap();
                    }));
                }
                for t in tasks {
                    t.await.unwrap();
                }
            });
        });
        drop(engine);
        fs::remove_dir_all("concurrent.db").unwrap();
    });

    // New: Concurrent benchmark for reverse_scan.
    group.bench_function("reverse_scan", |b| {
        let engine = Engine::new(PathBuf::from("concurrent_rev.db"));
        b.iter(|| {
            rt.block_on(async {
                let mut tasks = Vec::new();
                for _ in 0..4 {
                    let engine_clone = engine.clone();
                    tasks.push(tokio::spawn(async move {
                        let _ = engine_clone
                            .reverse_scan(b"a".to_vec()..b"z".to_vec())
                            .await
                            .unwrap();
                    }));
                }
                for t in tasks {
                    t.await.unwrap();
                }
            });
        });
        drop(engine);
        fs::remove_dir_all("concurrent_rev.db").unwrap();
    });

    // Concurrent benchmark for delete.
    group.bench_function("del", |b| {
        let engine = Engine::new(PathBuf::from("concurrent.db"));
        b.iter(|| {
            rt.block_on(async {
                let mut tasks = Vec::new();
                for _ in 0..4 {
                    let key: String = rand::rng()
                        .sample_iter(&Alphanumeric)
                        .take(8)
                        .map(char::from)
                        .collect();
                    let engine_clone = engine.clone();
                    tasks.push(tokio::spawn(async move {
                        engine_clone.del(key.as_bytes()).await.unwrap_or_default();
                    }));
                }
                for t in tasks {
                    t.await.unwrap();
                }
            });
        });
        drop(engine);
        fs::remove_dir_all("concurrent.db").unwrap();
    });

    group.finish();
}

criterion_group!(
    benches,
    engine_benchmark,
    engine_short_benchmark,
    engine_long_benchmark,
    engine_concurrency_benchmark
);
criterion_main!(benches);
