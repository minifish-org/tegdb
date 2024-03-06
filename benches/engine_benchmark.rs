use criterion::{black_box, criterion_group, criterion_main, Criterion};
use std::path::PathBuf;
use tegdb::Engine;
use redb::{Database, ReadableTable, TableDefinition};

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

fn redb_benchmark(c: &mut Criterion) {
    let path = PathBuf::from("redb");
    let db = Database::create(path).unwrap();
    let table_def: TableDefinition<&str, &str> = TableDefinition::new("my_table");
    
    let key = "key";
    let value = "value";

    c.bench_function("redb put", |b| b.iter(|| {
        let tx = db.begin_write().unwrap();
        {
            let mut table = tx.open_table(table_def).unwrap();
            table.insert(black_box(key), black_box(value)).unwrap();
        }
        tx.commit().unwrap();
    }));

    c.bench_function("redb get", |b| b.iter(|| {
        let tx = db.begin_read().unwrap();
        let table = tx.open_table(table_def).unwrap();
        table.get(black_box(key)).unwrap();
    }));

    c.bench_function("redb del", |b| b.iter(|| {
        let tx = db.begin_write().unwrap();
        {
            let mut table = tx.open_table(table_def).unwrap();
            table.remove(black_box(key)).unwrap();
        }
        tx.commit().unwrap();
    }));
}

criterion_group!(benches, engine_benchmark, sled_benchmark, redb_benchmark);
criterion_main!(benches);