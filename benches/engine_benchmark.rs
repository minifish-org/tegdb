use criterion::{black_box, criterion_group, criterion_main, Criterion};
use tegdb::Engine;
use std::path::PathBuf;

fn engine_benchmark(c: &mut Criterion) {
    let mut engine = Engine::new(PathBuf::from("test.db"));
    let key = b"key";
    let value = b"value";

    c.bench_function("engine set", |b| b.iter(|| {
        engine.set(black_box(key), black_box(value.to_vec()));
    }));

    c.bench_function("engine get", |b| b.iter(|| {
        engine.get(black_box(key));
    }));

    c.bench_function("engine del", |b| b.iter(|| {
        engine.del(black_box(key));
    }));
}

fn sled_benchmark(c: &mut Criterion) {
    let path = "sled";
    let db = sled::open(path).unwrap();
    let key = b"key";
    let value = b"value";

    c.bench_function("sled insert", |b| b.iter(|| {
        db.insert(black_box(key), black_box(value)).unwrap();
    }));

    c.bench_function("sled get", |b| b.iter(|| {
        db.get(black_box(key)).unwrap();
    }));

    c.bench_function("sled remove", |b| b.iter(|| {
        db.remove(black_box(key)).unwrap();
    }));
}

criterion_group!(benches, engine_benchmark, sled_benchmark);
criterion_main!(benches);