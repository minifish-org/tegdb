use criterion::{black_box, criterion_group, criterion_main, Criterion};
use redb::{Database, ReadableTable, TableDefinition};
use std::path::PathBuf;
use tegdb::Engine;
use tokio::runtime::Runtime;

async fn engine_benchmark(c: &mut Criterion, value_size: usize) {
    let mut engine = Engine::new(PathBuf::from("test.db"));
    let value = vec![0; value_size];

    c.bench_function(&format!("engine seq set {}", value_size), |b| {
        let mut i = 0;
        b.iter(|| {
            let key_str = format!("key{}", i);
            let key = key_str.as_bytes();
            tokio::task::block_in_place(|| {
                tokio::runtime::Handle::current().block_on(async {
                    engine.set(black_box(key), black_box(value.to_vec())).await.unwrap();
                });
            });
            i += 1;
        })
    });

    c.bench_function(&format!("engine seq get {}", value_size), |b| {
        let mut i = 0;
        b.iter(|| {
            let key_str = format!("key{}", i);
            let key = key_str.as_bytes();
            tokio::task::block_in_place(|| {
                tokio::runtime::Handle::current().block_on(async {
                    engine.get(black_box(key)).await.unwrap();
                });
            });
            i += 1;
        })
    });

    c.bench_function(&format!("engine seq scan {}", value_size), |b| {
        let start_key = b"a";
        let end_key = b"z";
        b.iter(|| {
            tokio::task::block_in_place(|| {
                tokio::runtime::Handle::current().block_on(async {
                    let _ = engine
                        .scan(black_box(start_key.to_vec())..black_box(end_key.to_vec()))
                        .await
                        .unwrap()
                        .collect::<Vec<_>>();
                });
            });
        })
    });

    c.bench_function(&format!("engine seq del {}", value_size), |b| {
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
}

async fn sled_benchmark(c: &mut Criterion, value_size: usize) {
    let path = "sled";
    let db = sled::open(path).unwrap();
    let value = vec![0; value_size];

    c.bench_function(&format!("sled seq insert {}", value_size), |b| {
        let mut i = 0;
        b.iter(|| {
            let key_str = format!("key{}", i);
            let key = key_str.as_bytes();
            tokio::task::block_in_place(|| {
                tokio::runtime::Handle::current().block_on(async {
                    db.insert(black_box(key), black_box(value.to_vec())).unwrap();
                });
            });
            i += 1;
        })
    });

    c.bench_function(&format!("sled seq get {}", value_size), |b| {
        let mut i = 0;
        b.iter(|| {
            let key_str = format!("key{}", i);
            let key = key_str.as_bytes();
            tokio::task::block_in_place(|| {
                tokio::runtime::Handle::current().block_on(async {
                    db.get(black_box(key)).unwrap();
                });
            });
            i += 1;
        })
    });

    c.bench_function(&format!("sled seq scan {}", value_size), |b| {
        let start_key = "a";
        let end_key = "z";
        b.iter(|| {
            tokio::task::block_in_place(|| {
                tokio::runtime::Handle::current().block_on(async {
                    let _ = db
                        .range(black_box(start_key)..black_box(end_key))
                        .values()
                        .collect::<Result<Vec<_>, _>>()
                        .unwrap();
                });
            });
        })
    });

    c.bench_function(&format!("sled seq remove {}", value_size), |b| {
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
}

async fn redb_benchmark(c: &mut Criterion, value_size: usize) {
    let path = PathBuf::from("redb");
    let db = Database::create(path).unwrap();
    let table_def: TableDefinition<&str, &str> = TableDefinition::new("my_table");

    let value = vec![0; value_size];
    let value_str = String::from_utf8(value).unwrap();

    c.bench_function(&format!("redb seq put {}", value_size), |b| {
        let mut i = 0;
        b.iter(|| {
            tokio::task::block_in_place(|| {
                tokio::runtime::Handle::current().block_on(async {
                    let tx = db.begin_write().unwrap();
                    {
                        let key = format!("key{}", i);
                        let mut table = tx.open_table(table_def).unwrap();
                        table
                            .insert(black_box(key.as_str()), black_box(value_str.as_str()))
                            .unwrap();
                        i += 1;
                    }
                    tx.commit().unwrap();
                });
            });
        })
    });

    c.bench_function(&format!("redb seq get {}", value_size), |b| {
        let mut i = 0;
        b.iter(|| {
            tokio::task::block_in_place(|| {
                tokio::runtime::Handle::current().block_on(async {
                    let tx = db.begin_read().unwrap();
                    let table = tx.open_table(table_def).unwrap();
                    let key = format!("key{}", i);
                    table.get(black_box(key.as_str())).unwrap();
                    i += 1;
                });
            });
        })
    });

    c.bench_function(&format!("redb seq scan {}", value_size), |b| {
        let start_key = "a";
        let end_key = "z";
        b.iter(|| {
            tokio::task::block_in_place(|| {
                tokio::runtime::Handle::current().block_on(async {
                    let tx = db.begin_read().unwrap();
                    let table = tx.open_table(table_def).unwrap();
                    let _ = table
                        .range(black_box(start_key)..black_box(end_key))
                        .unwrap()
                        .collect::<Result<Vec<_>, _>>()
                        .unwrap();
                });
            });
        })
    });

    c.bench_function(&format!("redb seq del {}", value_size), |b| {
        let mut i = 0;
        b.iter(|| {
            tokio::task::block_in_place(|| {
                tokio::runtime::Handle::current().block_on(async {
                    let tx = db.begin_write().unwrap();
                    {
                        let key = format!("key{}", i);
                        let mut table = tx.open_table(table_def).unwrap();
                        table.remove(black_box(key.as_str())).unwrap();
                        i += 1;
                    }
                    tx.commit().unwrap();
                });
            });
        })
    });
}

fn engine_short_benchmark(c: &mut Criterion) {
    let value_size = 1024;
    let rt = Runtime::new().unwrap();
    rt.block_on(engine_benchmark(c, value_size));
}

fn sled_short_benchmark(c: &mut Criterion) {
    let value_size = 1024;
    let rt = Runtime::new().unwrap();
    rt.block_on(sled_benchmark(c, value_size));
}

fn redb_short_benchmark(c: &mut Criterion) {
    let value_size = 1024;
    let rt = Runtime::new().unwrap();
    rt.block_on(redb_benchmark(c, value_size));
}

fn engine_long_benchmark(c: &mut Criterion) {
    let value_size = 1_000_000;
    let rt = Runtime::new().unwrap();
    rt.block_on(engine_benchmark(c, value_size));
}

fn sled_long_benchmark(c: &mut Criterion) {
    let value_size = 1_000_000;
    let rt = Runtime::new().unwrap();
    rt.block_on(sled_benchmark(c, value_size));
}

fn redb_long_benchmark(c: &mut Criterion) {
    let value_size = 1_000_000;
    let rt = Runtime::new().unwrap();
    rt.block_on(redb_benchmark(c, value_size));
}

criterion_group!(
    short_benches,
    engine_short_benchmark,
    sled_short_benchmark,
    redb_short_benchmark
);

criterion_group!(
    long_benches,
    engine_long_benchmark,
    sled_long_benchmark,
    redb_long_benchmark
);

criterion_main!(short_benches, long_benches);
