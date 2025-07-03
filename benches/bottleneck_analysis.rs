use criterion::{black_box, criterion_group, criterion_main, Criterion};
use std::collections::HashMap;
use std::env;
use std::fs;
use std::path::PathBuf;

/// Creates a unique temporary file path for benchmarks
fn temp_db_path(prefix: &str) -> PathBuf {
    let mut path = env::temp_dir();
    path.push(format!("tegdb_bench_{}_{}", prefix, std::process::id()));
    path
}

fn bottleneck_analysis(c: &mut Criterion) {
    let path = temp_db_path("bottleneck");
    if path.exists() {
        fs::remove_file(&path).expect("Failed to remove existing test file");
    }

    let mut db = tegdb::Database::open(&path).expect("Failed to create database");
    db.execute("CREATE TABLE test (id INTEGER PRIMARY KEY, value INTEGER)")
        .unwrap();

    // Benchmark just the parsing
    c.bench_function("just parsing", |b| {
        b.iter(|| {
            let (_remaining, _statement) =
                tegdb::parser::parse_sql(black_box("SELECT * FROM test WHERE id = 1")).unwrap();
        })
    });

    // Benchmark schema clone (simulate what Database.execute does)
    let schemas = {
        let mut dummy_schemas = HashMap::new();
        dummy_schemas.insert(
            "test".to_string(),
            tegdb::executor::TableSchema {
                name: "test".to_string(),
                columns: vec![
                    tegdb::executor::ColumnInfo {
                        name: "id".to_string(),
                        data_type: tegdb::parser::DataType::Integer,
                        constraints: vec![tegdb::parser::ColumnConstraint::PrimaryKey],
                    },
                    tegdb::executor::ColumnInfo {
                        name: "value".to_string(),
                        data_type: tegdb::parser::DataType::Integer,
                        constraints: vec![],
                    },
                ],
            },
        );
        dummy_schemas
    };

    c.bench_function("schema clone", |b| {
        b.iter(|| {
            let _cloned = black_box(schemas.clone());
        })
    });

    // Benchmark transaction creation
    let mut engine = tegdb::Engine::new(temp_db_path("tx_test")).unwrap();
    c.bench_function("transaction creation", |b| {
        b.iter(|| {
            let tx = engine.begin_transaction();
            drop(tx);
        })
    });

    // Benchmark the actual native row format serialization
    let test_row_data = {
        let mut row = HashMap::new();
        row.insert("id".to_string(), tegdb::parser::SqlValue::Integer(123));
        row.insert("value".to_string(), tegdb::parser::SqlValue::Integer(456));
        row
    };

    let test_schema = tegdb::executor::TableSchema {
        name: "test".to_string(),
        columns: vec![
            tegdb::executor::ColumnInfo {
                name: "id".to_string(),
                data_type: tegdb::parser::DataType::Integer,
                constraints: vec![tegdb::parser::ColumnConstraint::PrimaryKey],
            },
            tegdb::executor::ColumnInfo {
                name: "value".to_string(),
                data_type: tegdb::parser::DataType::Integer,
                constraints: vec![],
            },
        ],
    };

    // Benchmark native row format serialization
    c.bench_function("native serialization", |b| {
        b.iter(|| {
            let serialized = tegdb::native_row_format::NativeRowFormat::serialize(
                black_box(&test_row_data),
                black_box(&test_schema),
            )
            .unwrap();
            black_box(serialized);
        })
    });

    // Benchmark native row format deserialization
    let serialized_data =
        tegdb::native_row_format::NativeRowFormat::serialize(&test_row_data, &test_schema).unwrap();
    c.bench_function("native deserialization", |b| {
        b.iter(|| {
            let deserialized = tegdb::native_row_format::NativeRowFormat::deserialize_full(
                black_box(&serialized_data),
                black_box(&test_schema),
            )
            .unwrap();
            black_box(deserialized);
        })
    });

    // Benchmark partial column deserialization (LIMIT optimization)
    let columns_to_select = vec!["id".to_string()];
    c.bench_function("partial deserialization", |b| {
        b.iter(|| {
            let values = tegdb::native_row_format::NativeRowFormat::deserialize_columns(
                black_box(&serialized_data),
                black_box(&test_schema),
                black_box(&columns_to_select),
            )
            .unwrap();
            black_box(values);
        })
    });

    // Benchmark storage format interface
    let storage_format = tegdb::storage_format::StorageFormat::new();
    c.bench_function("storage format serialize", |b| {
        b.iter(|| {
            let serialized = storage_format
                .serialize_row(black_box(&test_row_data), black_box(&test_schema))
                .unwrap();
            black_box(serialized);
        })
    });

    c.bench_function("storage format deserialize", |b| {
        b.iter(|| {
            let deserialized = storage_format
                .deserialize_row(black_box(&serialized_data), black_box(&test_schema))
                .unwrap();
            black_box(deserialized);
        })
    });

    // Benchmark complete query execution pipeline
    c.bench_function("complete query pipeline", |b| {
        b.iter(|| {
            let result = db.query(black_box("SELECT id, value FROM test")).unwrap();
            black_box(result);
        })
    });

    // Add some test data for more realistic benchmarks
    for i in 1..=100 {
        db.execute(&format!(
            "INSERT INTO test (id, value) VALUES ({}, {})",
            i,
            i * 2
        ))
        .unwrap();
    }

    c.bench_function("query with data", |b| {
        b.iter(|| {
            let result = db
                .query(black_box("SELECT id, value FROM test WHERE id < 50"))
                .unwrap();
            black_box(result);
        })
    });

    c.bench_function("limited query", |b| {
        b.iter(|| {
            let result = db
                .query(black_box("SELECT id, value FROM test LIMIT 10"))
                .unwrap();
            black_box(result);
        })
    });

    // Clean up
    drop(db);
    drop(engine);
    let _ = fs::remove_file(&path);
    let _ = fs::remove_file(temp_db_path("tx_test"));
}

criterion_group!(benches, bottleneck_analysis);
criterion_main!(benches);
