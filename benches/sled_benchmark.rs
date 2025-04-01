//! Benchmark tests for sled embedded database operations using Criterion.

use criterion::{black_box, criterion_group, criterion_main, Criterion, Throughput};
use rand::distr::Alphanumeric;
use rand::Rng;
use tokio::runtime::Runtime;

fn sled_benchmark(c: &mut Criterion) {
    let rt = Runtime::new().unwrap();
    let path = "sled";
    let db = sled::open(path).unwrap();
    let key = b"key";
    let value = b"value";

    let mut group = c.benchmark_group("sled_basic");
    group.throughput(Throughput::Elements(1));

    // Benchmark for insert operation.
    group.bench_function("insert", |b| {
        b.iter(|| {
            rt.block_on(async {
                db.insert(black_box(key), black_box(value)).unwrap();
            });
        })
    });

    // Benchmark for get operation.
    group.bench_function("get", |b| {
        b.iter(|| {
            rt.block_on(async {
                let _ = db.get(black_box(key)).unwrap().map(|v| v.to_vec());
            });
        })
    });

    // Benchmark for scan operation.
    group.bench_function("scan", |b| {
        let start_key = "a";
        let end_key = "z";
        b.iter(|| {
            rt.block_on(async {
                let _ = db
                    .range(black_box(start_key)..black_box(end_key))
                    .values()
                    .collect::<Result<Vec<_>, _>>()
                    .unwrap();
            });
        })
    });

    // Benchmark for remove operation.
    group.bench_function("remove", |b| {
        b.iter(|| {
            rt.block_on(async {
                db.remove(black_box(key)).unwrap();
            });
        })
    });

    group.finish();
}

async fn sled_seq_benchmark(c: &mut Criterion, value_size: usize) {
    let path = "sled";
    let db = sled::open(path).unwrap();
    let value = vec![0; value_size];

    let mut group = c.benchmark_group(format!("sled_seq_{}", value_size));
    group.throughput(Throughput::Elements(1));

    // Sequential benchmark for insert.
    group.bench_function("insert", |b| {
        let mut i = 0;
        b.iter(|| {
            let key_str = format!("key{}", i);
            let key = key_str.as_bytes();
            tokio::task::block_in_place(|| {
                tokio::runtime::Handle::current().block_on(async {
                    db.insert(black_box(key), black_box(value.to_vec()))
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
                    let _ = db.get(black_box(key)).unwrap().map(|v| v.to_vec());
                });
            });
            i += 1;
        })
    });

    // Sequential benchmark for remove.
    group.bench_function("remove", |b| {
        let mut i = 0;
        b.iter(|| {
            let key_str = format!("key{}", i);
            let key = key_str.as_bytes();
            tokio::task::block_in_place(|| {
                tokio::runtime::Handle::current().block_on(async {
                    db.remove(black_box(key)).unwrap();
                });
            });
            i += 1;
        })
    });

    group.finish();
}

fn sled_short_benchmark(c: &mut Criterion) {
    let value_size = 1024;
    let rt = Runtime::new().unwrap();
    rt.block_on(sled_seq_benchmark(c, value_size));
}

fn sled_long_benchmark(c: &mut Criterion) {
    let value_size = 255_000;
    let rt = Runtime::new().unwrap();
    rt.block_on(sled_seq_benchmark(c, value_size));
}

fn sled_concurrency_benchmark(c: &mut Criterion) {
    let rt = Runtime::new().unwrap();
    let mut group = c.benchmark_group("sled_concurrent");
    group.throughput(Throughput::Elements(4));

    // Remove concurrent_sled directory once before running the benchmarks.
    std::fs::remove_dir_all("concurrent_sled").ok();

    // Concurrent benchmark for insert.
    group.bench_function("insert", |b| {
        let db = sled::open("concurrent_sled").unwrap();
        b.iter(|| {
            rt.block_on(async {
                let mut tasks = Vec::new();
                for _ in 0..4 {
                    let key: String = rand::rng()
                        .sample_iter(&Alphanumeric)
                        .take(8)
                        .map(char::from)
                        .collect();
                    let mut rng = rand::rng();
                    let value: Vec<u8> = (0..10).map(|_| rng.random()).collect();
                    let db_clone = db.clone();
                    tasks.push(tokio::spawn(async move {
                        db_clone.insert(key.as_bytes(), value).unwrap();
                    }));
                }
                for t in tasks {
                    t.await.unwrap();
                }
            });
        });
    });

    // Concurrent benchmark for get.
    group.bench_function("get", |b| {
        let db = sled::open("concurrent_sled").unwrap();
        b.iter(|| {
            rt.block_on(async {
                let mut tasks = Vec::new();
                for _ in 0..4 {
                    let key: String = rand::rng()
                        .sample_iter(&Alphanumeric)
                        .take(8)
                        .map(char::from)
                        .collect();
                    let db_clone = db.clone();
                    tasks.push(tokio::spawn(async move {
                        let _ = db_clone.get(key.as_bytes()).unwrap();
                    }));
                }
                for t in tasks {
                    t.await.unwrap();
                }
            });
        });
    });

    // Concurrent benchmark for scan.
    group.bench_function("scan", |b| {
        let db = sled::open("concurrent_sled").unwrap();
        b.iter(|| {
            rt.block_on(async {
                let mut tasks = Vec::new();
                for _ in 0..4 {
                    let db_clone = db.clone();
                    tasks.push(tokio::spawn(async move {
                        let _ = db_clone
                            .range(black_box("a")..black_box("z"))
                            .values()
                            .collect::<Result<Vec<_>, _>>()
                            .unwrap();
                    }));
                }
                for t in tasks {
                    t.await.unwrap();
                }
            });
        });
    });

    // Concurrent benchmark for remove.
    group.bench_function("remove", |b| {
        let db = sled::open("concurrent_sled").unwrap();
        b.iter(|| {
            rt.block_on(async {
                let mut tasks = Vec::new();
                for _ in 0..4 {
                    let key: String = rand::rng()
                        .sample_iter(&Alphanumeric)
                        .take(8)
                        .map(char::from)
                        .collect();
                    let db_clone = db.clone();
                    tasks.push(tokio::spawn(async move {
                        let _ = db_clone.remove(key.as_bytes());
                    }));
                }
                for t in tasks {
                    t.await.unwrap();
                }
            });
        });
    });

    group.finish();
}

criterion_group!(
    sled_benches,
    sled_benchmark,
    sled_short_benchmark,
    sled_long_benchmark,
    sled_concurrency_benchmark
);
criterion_main!(sled_benches);
