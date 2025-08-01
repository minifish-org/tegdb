use criterion::{criterion_group, criterion_main, Criterion};
use std::hint::black_box;
use std::path::PathBuf;
use tempfile::tempdir;

fn temp_db_path(prefix: &str) -> PathBuf {
    let temp_dir = tempdir().unwrap();
    temp_dir.path().join(format!("{}.db", prefix))
}

fn query_prepared_breakdown(c: &mut Criterion) {
    let db_path = temp_db_path("query_prepared_breakdown");
    let mut db = tegdb::Database::open(db_path.to_str().unwrap()).unwrap();

    // Setup test data
    db.execute("CREATE TABLE test (id INTEGER PRIMARY KEY, value INTEGER, name TEXT(32))").unwrap();
    
    // Insert test data
    for i in 1..=100 {
        db.execute(&format!("INSERT INTO test (id, value, name) VALUES ({}, {}, 'test{}')", i, i * 10, i)).unwrap();
    }

    // Prepare statements
    let pk_lookup_stmt = db.prepare("SELECT * FROM test WHERE id = ?1").unwrap();
    let range_stmt = db.prepare("SELECT * FROM test WHERE id BETWEEN ?1 AND ?2").unwrap();

    let mut group = c.benchmark_group("Query Prepared Breakdown");

    // Test prepared statement execution
    group.bench_function("prepared_pk_lookup", |b| {
        b.iter(|| {
            let params = vec![tegdb::SqlValue::Integer(50)];
            let _result = db.query_prepared(&pk_lookup_stmt, &params).unwrap();
            black_box(_result);
        });
    });

    // Test prepared statement with range
    group.bench_function("prepared_range_query", |b| {
        b.iter(|| {
            let params = vec![tegdb::SqlValue::Integer(10), tegdb::SqlValue::Integer(20)];
            let _result = db.query_prepared(&range_stmt, &params).unwrap();
            black_box(_result);
        });
    });

    // Test parameter binding overhead
    group.bench_function("parameter_binding_overhead", |b| {
        b.iter(|| {
            let params = vec![tegdb::SqlValue::Integer(25)];
            black_box(params);
        });
    });

    group.finish();
}

criterion_group!(benches, query_prepared_breakdown);
criterion_main!(benches);
