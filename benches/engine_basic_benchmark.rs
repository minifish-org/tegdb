use criterion::{black_box, criterion_group, criterion_main, Criterion};
use redb::{Database, ReadableTable, TableDefinition};
use rusqlite::{params, Connection};
use tempfile::NamedTempFile;
use std::path::PathBuf;
use tegdb::Engine;

fn engine_benchmark(c: &mut Criterion) {
    let mut engine = Engine::new(PathBuf::from("test.db"));
    let key = b"key";
    let value = b"value";

    c.bench_function("engine set", |b| {
        b.iter(|| {
            engine.set(black_box(key), black_box(value.to_vec()));
        })
    });

    c.bench_function("engine get", |b| {
        b.iter(|| {
            engine.get(black_box(key));
        })
    });

    c.bench_function("engine scan", |b| {
        let start_key = b"a";
        let end_key = b"z";
        b.iter(|| {
            let _ = engine
                .scan(black_box(start_key.to_vec())..black_box(end_key.to_vec()))
                .collect::<Vec<_>>();
        })
    });

    c.bench_function("engine del", |b| {
        b.iter(|| {
            engine.del(black_box(key));
        })
    });
}

fn sled_benchmark(c: &mut Criterion) {
    let path = "sled";
    let db = sled::open(path).unwrap();
    let key = b"key";
    let value = b"value";

    c.bench_function("sled insert", |b| {
        b.iter(|| {
            db.insert(black_box(key), black_box(value)).unwrap();
        })
    });

    c.bench_function("sled get", |b| {
        b.iter(|| {
            db.get(black_box(key)).unwrap();
        })
    });

    c.bench_function("sled scan", |b| {
        let start_key = "a";
        let end_key = "z";
        b.iter(|| {
            let _ = db
                .range(black_box(start_key)..black_box(end_key))
                .values()
                .collect::<Result<Vec<_>, _>>()
                .unwrap();
        })
    });

    c.bench_function("sled remove", |b| {
        b.iter(|| {
            db.remove(black_box(key)).unwrap();
        })
    });
}

fn redb_benchmark(c: &mut Criterion) {
    let path = PathBuf::from("redb");
    let db = Database::create(path).unwrap();
    let table_def: TableDefinition<&str, &str> = TableDefinition::new("my_table");

    let key = "key";
    let value = "value";

    c.bench_function("redb put", |b| {
        b.iter(|| {
            let tx = db.begin_write().unwrap();
            {
                let mut table = tx.open_table(table_def).unwrap();
                table.insert(black_box(key), black_box(value)).unwrap();
            }
            tx.commit().unwrap();
        })
    });

    c.bench_function("redb get", |b| {
        b.iter(|| {
            let tx = db.begin_read().unwrap();
            let table = tx.open_table(table_def).unwrap();
            table.get(black_box(key)).unwrap();
        })
    });

    c.bench_function("redb scan", |b| {
        let start_key = "a";
        let end_key = "z";
        b.iter(|| {
            let tx = db.begin_read().unwrap();
            let table = tx.open_table(table_def).unwrap();
            let _ = table
                .range(black_box(start_key)..black_box(end_key))
                .unwrap()
                .collect::<Result<Vec<_>, _>>()
                .unwrap();
        })
    });

    c.bench_function("redb del", |b| {
        b.iter(|| {
            let tx = db.begin_write().unwrap();
            {
                let mut table = tx.open_table(table_def).unwrap();
                table.remove(black_box(key)).unwrap();
            }
            tx.commit().unwrap();
        })
    });
}

fn sqlite_benchmark(c: &mut Criterion) {
    let temp_file = NamedTempFile::new().unwrap();
    let conn = Connection::open(temp_file.path()).unwrap();

    conn.execute(
        "CREATE TABLE IF NOT EXISTS test (key TEXT, value TEXT NOT NULL)",
        [],
    )
    .unwrap();

    let key = "key";
    let value = "value";

    c.bench_function("sqlite insert", |b| {
        b.iter(|| {
            conn.execute("INSERT INTO test (key, value) VALUES (?1, ?2)", params![key, value])
                .unwrap();
        })
    });

    c.bench_function("sqlite get", |b| {
        b.iter(|| {
            let _: String = conn
                .query_row("SELECT value FROM test WHERE key = ?1", params![key], |row| row.get(0))
                .unwrap();
        })
    });

    c.bench_function("sqlite delete", |b| {
        b.iter(|| {
            conn.execute("DELETE FROM test WHERE key = ?1", params![key]).unwrap();
        })
    });
}

criterion_group!(benches, engine_benchmark, sled_benchmark, redb_benchmark, sqlite_benchmark);
criterion_main!(benches);
