use criterion::{black_box, criterion_group, criterion_main, Criterion};
use redb::{Database, ReadableTable, TableDefinition};
use std::path::PathBuf;
use tegdb::Engine;

fn engine_benchmark(c: &mut Criterion) {
    let mut engine = Engine::new(PathBuf::from("test.db"));
    let value = b"value";

    c.bench_function("engine seq set", |b| {
        let mut i = 0;
        b.iter(|| {
            let key_str = format!("key{}", i);
            let key = key_str.as_bytes();
            engine.set(black_box(key), black_box(value.to_vec()));
            i += 1;
        })
    });

    c.bench_function("engine seq get", |b| {
        let mut i = 0;
        b.iter(|| {
            let key_str = format!("key{}", i);
            let key = key_str.as_bytes();
            engine.get(black_box(key));
            i += 1;
        })
    });

    c.bench_function("engine seq del", |b| {
        let mut i = 0;
        b.iter(|| {
            let key_str = format!("key{}", i);
            let key = key_str.as_bytes();
            engine.del(black_box(key));
            i += 1;
        })
    });
}

fn sled_benchmark(c: &mut Criterion) {
    let path = "sled";
    let db = sled::open(path).unwrap();
    let value = b"value";

    c.bench_function("sled seq insert", |b| {
        let mut i = 0;
        b.iter(|| {
            let key_str = format!("key{}", i);
            let key = key_str.as_bytes();
            db.insert(black_box(key), black_box(value)).unwrap();
            i += 1;
        })
    });

    c.bench_function("sled seq get", |b| {
        let mut i = 0;
        b.iter(|| {
            let key_str = format!("key{}", i);
            let key = key_str.as_bytes();
            db.get(black_box(key)).unwrap();
            i += 1;
        })
    });

    c.bench_function("sled seq remove", |b| {
        let mut i = 0;
        b.iter(|| {
            let key_str = format!("key{}", i);
            let key = key_str.as_bytes();
            db.remove(black_box(key)).unwrap();
            i += 1;
        })
    });
}

fn redb_benchmark(c: &mut Criterion) {
    let path = PathBuf::from("redb");
    let db = Database::create(path).unwrap();
    let table_def: TableDefinition<&str, &str> = TableDefinition::new("my_table");

    let value = "value";

    c.bench_function("redb seq put", |b| {
        let mut i = 0;
        b.iter(|| {
            let tx = db.begin_write().unwrap();
            {
                let key = format!("key{}", i);
                let mut table = tx.open_table(table_def).unwrap();
                table
                    .insert(black_box(key.as_str()), black_box(value))
                    .unwrap();
                i += 1;
            }
            tx.commit().unwrap();
        })
    });

    c.bench_function("redb seq get", |b| {
        let mut i = 0;
        b.iter(|| {
            let tx = db.begin_read().unwrap();
            let table = tx.open_table(table_def).unwrap();
            let key = format!("key{}", i);
            table.get(black_box(key.as_str())).unwrap();
            i += 1;
        })
    });

    c.bench_function("redb seq del", |b| {
        let mut i = 0;
        b.iter(|| {
            let tx = db.begin_write().unwrap();
            {
                let key = format!("key{}", i);
                let mut table = tx.open_table(table_def).unwrap();
                table.remove(black_box(key.as_str())).unwrap();
                i += 1;
            }
            tx.commit().unwrap();
        })
    });
}

criterion_group!(benches, engine_benchmark, sled_benchmark, redb_benchmark);
criterion_main!(benches);
