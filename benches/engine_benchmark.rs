use criterion::{black_box, criterion_group, criterion_main, Criterion};
use tegdb::Engine;
use std::path::PathBuf;

fn engine_benchmark(c: &mut Criterion) {
    let mut engine = Engine::new(PathBuf::from("test.db"));
    let key = b"key";
    let value = b"value";
    let start_key = b"a";
    let end_key = b"z";

    c.bench_function("engine set", |b| b.iter(|| {
        engine.set(black_box(key), black_box(value.to_vec()));
    }));

    c.bench_function("engine get", |b| b.iter(|| {
        engine.get(black_box(key));
    }));

    c.bench_function("engine scan", |b| b.iter(|| {
        engine.scan(black_box(start_key), black_box(end_key));
    }));

    c.bench_function("engine del", |b| b.iter(|| {
        engine.del(black_box(key));
    }));
}

criterion_group!(benches, engine_benchmark);
criterion_main!(benches);