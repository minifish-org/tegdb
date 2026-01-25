use criterion::{criterion_group, criterion_main, Criterion};
use std::hint::black_box;
use tegdb::Database;
use tempfile::TempDir;

struct DbHarness {
    _temp_dir: Option<TempDir>,
    db: Database,
    table: String,
}

fn setup_file_db() -> DbHarness {
    let temp_dir = TempDir::new().expect("temp dir");
    let db_path = temp_dir.path().join("rpc_vs_file.teg");
    let identifier = format!("file://{}", db_path.to_string_lossy());
    let mut db = Database::open(identifier).expect("open file database");
    let table = format!("bench_{}", fastrand::u32(..));
    db.execute(&format!(
        "CREATE TABLE {table} (id INTEGER PRIMARY KEY, name TEXT(16))"
    ))
    .expect("create table");
    db.execute(&format!(
        "INSERT INTO {table} (id, name) VALUES (1, 'init')"
    ))
    .expect("seed row");

    DbHarness {
        _temp_dir: Some(temp_dir),
        db,
        table,
    }
}

fn setup_rpc_db() -> DbHarness {
    let mut db = Database::open("rpc://127.0.0.1:9000").expect("open rpc database");
    let table = format!("bench_{}", fastrand::u32(..));
    db.execute(&format!(
        "CREATE TABLE {table} (id INTEGER PRIMARY KEY, name TEXT(16))"
    ))
    .expect("create table");
    db.execute(&format!(
        "INSERT INTO {table} (id, name) VALUES (1, 'init')"
    ))
    .expect("seed row");

    DbHarness {
        _temp_dir: None,
        db,
        table,
    }
}

fn sql_benchmark(c: &mut Criterion) {
    let mut file = setup_file_db();
    let mut rpc = setup_rpc_db();

    let file_select = format!("SELECT name FROM {} WHERE id = 1", file.table);
    let rpc_select = format!("SELECT name FROM {} WHERE id = 1", rpc.table);
    let file_counter = std::cell::Cell::new(1000u64);
    let rpc_counter = std::cell::Cell::new(2000u64);

    c.bench_function("sql file insert", |b| {
        b.iter(|| {
            let id = file_counter.get();
            file_counter.set(id + 1);
            let insert = format!(
                "INSERT INTO {} (id, name) VALUES ({}, 'rpc')",
                file.table, id
            );
            let delete = format!("DELETE FROM {} WHERE id = {}", file.table, id);
            file.db.execute(black_box(&insert)).unwrap();
            file.db.execute(black_box(&delete)).unwrap();
        })
    });

    c.bench_function("sql rpc insert", |b| {
        b.iter(|| {
            let id = rpc_counter.get();
            rpc_counter.set(id + 1);
            let insert = format!(
                "INSERT INTO {} (id, name) VALUES ({}, 'rpc')",
                rpc.table, id
            );
            let delete = format!("DELETE FROM {} WHERE id = {}", rpc.table, id);
            rpc.db.execute(black_box(&insert)).unwrap();
            rpc.db.execute(black_box(&delete)).unwrap();
        })
    });

    c.bench_function("sql file select", |b| {
        b.iter(|| {
            let result = file.db.query(black_box(&file_select)).unwrap();
            black_box(result.rows());
        })
    });

    c.bench_function("sql rpc select", |b| {
        b.iter(|| {
            let result = rpc.db.query(black_box(&rpc_select)).unwrap();
            black_box(result.rows());
        })
    });
}

criterion_group!(benches, sql_benchmark);
criterion_main!(benches);
