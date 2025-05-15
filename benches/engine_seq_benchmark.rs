use criterion::{black_box, criterion_group, criterion_main, Criterion};
use std::path::PathBuf;
use tegdb::Engine;

fn engine_benchmark(c: &mut Criterion, value_size: usize) {
    let mut engine = Engine::new(PathBuf::from("test.db")).expect("Failed to create engine");
    let value = vec![0; value_size];

    c.bench_function(&format!("engine seq set {}", value_size), |b| {
        let mut i = 0;
        b.iter(|| {
            let key_str = format!("key{}", i);
            let key = key_str.as_bytes();
            engine.set(black_box(key), black_box(value.to_vec())).unwrap();
            i += 1;
        })
    });

    c.bench_function(&format!("engine seq get {}", value_size), |b| {
        let mut i = 0;
        b.iter(|| {
            let key_str = format!("key{}", i);
            let key = key_str.as_bytes();
            engine.get(black_box(key)).unwrap();
            i += 1;
        })
    });

    c.bench_function(&format!("engine seq del {}", value_size), |b| {
        let mut i = 0;
        b.iter(|| {
            let key_str = format!("key{}", i);
            let key = key_str.as_bytes();
            engine.del(black_box(key)).unwrap();
            i += 1;
        })
    });
}



fn sled_benchmark(c: &mut Criterion, value_size: usize) {
    let db = sled::open("sled").unwrap();
    let value = vec![0; value_size];

    c.bench_function(&format!("sled seq insert {}", value_size), |b| {
        let mut i = 0;
        b.iter(|| {
            let key_str = format!("key{}", i);
            let key = key_str.as_bytes();
            db.insert(black_box(key), black_box(value.as_slice())).unwrap();
            i += 1;
        })
    });

    c.bench_function(&format!("sled seq get {}", value_size), |b| {
        let mut i = 0;
        b.iter(|| {
            let key_str = format!("key{}", i);
            let key = key_str.as_bytes();
            db.get(black_box(key)).unwrap();
            i += 1;
        })
    });

    c.bench_function(&format!("sled seq remove {}", value_size), |b| {
        let mut i = 0;
        b.iter(|| {
            let key_str = format!("key{}", i);
            let key = key_str.as_bytes();
            db.remove(black_box(key)).unwrap();
            i += 1;
        })
    });

    std::fs::remove_dir_all("sled").unwrap_or_default();
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
