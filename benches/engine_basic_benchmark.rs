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
            engine.set(black_box(key), black_box(value.to_vec())).unwrap();
        })
    });

    c.bench_function("engine get", |b| {
        b.iter(|| {
            engine.get(black_box(key)).unwrap();
        })
    });

    c.bench_function("engine scan", |b| {
        let start_key = b"a";
        let end_key = b"z";
        b.iter(|| {
            black_box(
                engine
                    .scan(black_box(start_key.to_vec())..black_box(end_key.to_vec()))
                    .unwrap()
                    .collect::<Vec<_>>()
            );
        })
    });

    c.bench_function("engine del", |b| {
        b.iter(|| {
            engine.del(black_box(key)).unwrap();
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
        b.iter(|| {
            black_box(
                db.range::<&[u8], _>(black_box(b"a".as_ref())..black_box(b"z".as_ref()))
                    .map(|x| x.unwrap())
                    .collect::<Vec<_>>()
            );
        })
    });

    c.bench_function("sled remove", |b| {
        b.iter(|| {
            db.remove(black_box(key)).unwrap();
        })
    });

    // Clean up
    std::fs::remove_dir_all(path).unwrap();
}

fn redb_benchmark(c: &mut Criterion) {
    let db_file = NamedTempFile::new().unwrap();
    let db = Database::create(db_file.path()).unwrap();
    let table_def: TableDefinition<&[u8], &[u8]> = TableDefinition::new("my_table");
    let key = b"key";
    let value = b"value";

    c.bench_function("redb put", |b| {
        b.iter(|| {
            let write_txn = db.begin_write().unwrap();
            {
                let mut table = write_txn.open_table(table_def).unwrap();
                table.insert(black_box(key), black_box(value)).unwrap();
            }
            write_txn.commit().unwrap();
        })
    });

    c.bench_function("redb get", |b| {
        b.iter(|| {
            let read_txn = db.begin_read().unwrap();
            let table = read_txn.open_table(table_def).unwrap();
            table.get(black_box(key)).unwrap();
        })
    });

    c.bench_function("redb scan", |b| {
        b.iter(|| {
            let read_txn = db.begin_read().unwrap();
            let table = read_txn.open_table(table_def).unwrap();
            black_box(
                table
                    .range(black_box(b"a".as_slice())..black_box(b"z".as_slice()))
                    .unwrap()
                    .map(|x| x.unwrap())
                    .collect::<Vec<_>>()
            );
        })
    });

    c.bench_function("redb del", |b| {
        b.iter(|| {
            let write_txn = db.begin_write().unwrap();
            {
                let mut table = write_txn.open_table(table_def).unwrap();
                table.remove(black_box(key)).unwrap();
            }
            write_txn.commit().unwrap();
        })
    });
}

fn sqlite_benchmark(c: &mut Criterion) {
    let conn = Connection::open_in_memory().unwrap();
    conn.execute(
        "CREATE TABLE test (key BLOB PRIMARY KEY, value BLOB)",
        [],
    )
    .unwrap();
    let key = b"key";
    let value = b"value";

    c.bench_function("sqlite insert", |b| {
        b.iter(|| {
            conn.execute(
                "INSERT OR REPLACE INTO test VALUES (?, ?)",
                params![black_box(key), black_box(value)],
            )
            .unwrap();
        })
    });

    c.bench_function("sqlite get", |b| {
        b.iter(|| {
            let mut stmt = conn
                .prepare("SELECT value FROM test WHERE key = ?")
                .unwrap();
            let mut rows = stmt.query([black_box(key)]).unwrap();
            rows.next().unwrap();
        })
    });

    c.bench_function("sqlite scan", |b| {
        b.iter(|| {
            let mut stmt = conn
                .prepare("SELECT key, value FROM test WHERE key >= ? AND key <= ? ORDER BY key")
                .unwrap();
            let rows = stmt
                .query_map([black_box(b"a".as_slice()), black_box(b"z".as_slice())], |row| {
                    Ok((row.get::<_, Vec<u8>>(0).unwrap(), row.get::<_, Vec<u8>>(1).unwrap()))
                })
                .unwrap()
                .collect::<Result<Vec<_>, _>>()
                .unwrap();
            black_box(rows);
        })
    });

    c.bench_function("sqlite delete", |b| {
        b.iter(|| {
            conn.execute("DELETE FROM test WHERE key = ?", params![black_box(key)])
                .unwrap();
        })
    });
}

criterion_group!(
    benches,
    engine_benchmark,
    sled_benchmark,
    redb_benchmark,
    sqlite_benchmark
);
criterion_main!(benches);
