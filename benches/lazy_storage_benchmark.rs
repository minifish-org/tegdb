use criterion::{criterion_group, criterion_main, Criterion};
use std::collections::HashMap;
use std::env;
use std::fs;
use std::hint::black_box;
use std::path::PathBuf;

/// Creates a unique temporary file path for benchmarks
fn temp_db_path(prefix: &str) -> PathBuf {
    let mut path = env::temp_dir();
    path.push(format!(
        "tegdb_lazy_bench_{}_{}",
        prefix,
        std::process::id()
    ));
    path
}

fn lazy_storage_benchmark(c: &mut Criterion) {
    let path = temp_db_path("lazy_storage");
    if path.exists() {
        fs::remove_file(&path).expect("Failed to remove existing test file");
    }

    let mut db = tegdb::Database::open(format!("file://{}", path.display()))
        .expect("Failed to create database");
    db.execute("CREATE TABLE test (id INTEGER PRIMARY KEY, value INTEGER, name TEXT(10))")
        .unwrap();

    // Add test data
    for i in 1..=100 {
        db.execute(&format!(
            "INSERT INTO test (id, value, name) VALUES ({}, {}, 'user{}')",
            i,
            i * 2,
            i
        ))
        .unwrap();
    }

    let storage_format = tegdb::storage_format::StorageFormat::new();
    let mut test_schema = tegdb::query_processor::TableSchema {
        name: "test".to_string(),
        columns: vec![
            tegdb::query_processor::ColumnInfo {
                name: "id".to_string(),
                data_type: tegdb::parser::DataType::Integer,
                constraints: vec![tegdb::parser::ColumnConstraint::PrimaryKey],
                storage_offset: 0,
                storage_size: 0,
                storage_type_code: 0,
            },
            tegdb::query_processor::ColumnInfo {
                name: "value".to_string(),
                data_type: tegdb::parser::DataType::Integer,
                constraints: vec![],
                storage_offset: 0,
                storage_size: 0,
                storage_type_code: 0,
            },
            tegdb::query_processor::ColumnInfo {
                name: "name".to_string(),
                data_type: tegdb::parser::DataType::Text(Some(10)),
                constraints: vec![],
                storage_offset: 0,
                storage_size: 0,
                storage_type_code: 0,
            },
        ],
        indexes: Vec::new(),
    };
    let _ = tegdb::catalog::Catalog::compute_table_metadata(&mut test_schema);

    // Create test row data
    let test_row_data = {
        let mut row = HashMap::new();
        row.insert("id".to_string(), tegdb::parser::SqlValue::Integer(123));
        row.insert("value".to_string(), tegdb::parser::SqlValue::Integer(456));
        row.insert(
            "name".to_string(),
            tegdb::parser::SqlValue::Text("test".to_string()),
        );
        row
    };

    // Serialize test data
    let serialized_data = storage_format
        .serialize_row(&test_row_data, &test_schema)
        .unwrap();

    // ===== LAZY STORAGE BENCHMARKS =====

    // 1. Single column access (zero-copy)
    c.bench_function("lazy_single_column_access", |b| {
        b.iter(|| {
            let _value = storage_format
                .get_column_value(black_box(&serialized_data), black_box(&test_schema), "id")
                .unwrap();
        })
    });

    // 2. Multiple column access (zero-copy)
    c.bench_function("lazy_multiple_column_access", |b| {
        b.iter(|| {
            let _values = storage_format
                .get_columns(
                    black_box(&serialized_data),
                    black_box(&test_schema),
                    &["id", "value"],
                )
                .unwrap();
        })
    });

    // 3. Column access by index (zero-copy)
    c.bench_function("lazy_column_by_index", |b| {
        b.iter(|| {
            let _value = storage_format
                .get_column_by_index(black_box(&serialized_data), black_box(&test_schema), 0)
                .unwrap();
        })
    });

    // 7. Condition evaluation (zero-copy)
    let simple_condition = tegdb::parser::Condition::Comparison {
        left: "id".to_string(),
        operator: tegdb::parser::ComparisonOperator::Equal,
        right: tegdb::parser::SqlValue::Integer(123),
    };

    c.bench_function("lazy_condition_evaluation", |b| {
        b.iter(|| {
            let _matches = storage_format
                .matches_condition(
                    black_box(&serialized_data),
                    black_box(&test_schema),
                    &simple_condition,
                )
                .unwrap();
        })
    });

    // 8. Complex condition evaluation (fallback to full deserialization)
    let complex_condition = tegdb::parser::Condition::And(
        Box::new(tegdb::parser::Condition::Comparison {
            left: "id".to_string(),
            operator: tegdb::parser::ComparisonOperator::Equal,
            right: tegdb::parser::SqlValue::Integer(123),
        }),
        Box::new(tegdb::parser::Condition::Comparison {
            left: "value".to_string(),
            operator: tegdb::parser::ComparisonOperator::GreaterThan,
            right: tegdb::parser::SqlValue::Integer(100),
        }),
    );

    c.bench_function("lazy_complex_condition_evaluation", |b| {
        b.iter(|| {
            let _matches = storage_format
                .matches_condition(
                    black_box(&serialized_data),
                    black_box(&test_schema),
                    &complex_condition,
                )
                .unwrap();
        })
    });

    // 9. Full deserialization (only when needed)
    c.bench_function("lazy_full_deserialization", |b| {
        b.iter(|| {
            let _row_data = storage_format
                .deserialize_row_full(black_box(&serialized_data), black_box(&test_schema))
                .unwrap();
        })
    });

    // ===== COMPARISON WITH OLD APPROACH =====

    // 11. Old approach: full deserialization every time
    c.bench_function("old_full_deserialization", |b| {
        b.iter(|| {
            let _row_data = storage_format
                .deserialize_row_full(black_box(&serialized_data), black_box(&test_schema))
                .unwrap();
        })
    });

    // 12. Old approach: deserialize columns
    c.bench_function("old_deserialize_columns", |b| {
        b.iter(|| {
            let columns = ["id".to_string(), "value".to_string()];
            let columns_ref: Vec<&str> = columns.iter().map(|s| s.as_str()).collect();
            let _values = storage_format
                .get_columns(
                    black_box(&serialized_data),
                    black_box(&test_schema),
                    &columns_ref,
                )
                .unwrap();
        })
    });

    // ===== REAL-WORLD SCENARIOS =====

    // 13. Query with WHERE clause (using lazy evaluation)
    c.bench_function("query_with_where_lazy", |b| {
        b.iter(|| {
            let result = db
                .query(black_box("SELECT id, value FROM test WHERE id = 50"))
                .unwrap();
            black_box(result);
        })
    });

    // 14. Query with LIMIT (using lazy evaluation)
    c.bench_function("query_with_limit_lazy", |b| {
        b.iter(|| {
            let result = db
                .query(black_box("SELECT id, value FROM test LIMIT 10"))
                .unwrap();
            black_box(result);
        })
    });

    // 15. Query with multiple conditions
    c.bench_function("query_with_multiple_conditions", |b| {
        b.iter(|| {
            let result = db
                .query(black_box(
                    "SELECT id, value FROM test WHERE id > 10 AND value < 100",
                ))
                .unwrap();
            black_box(result);
        })
    });

    // 16. String operations comparison
    c.bench_function("old_string_operations", |b| {
        b.iter(|| {
            let row_data = storage_format
                .deserialize_row_full(black_box(&serialized_data), black_box(&test_schema))
                .unwrap();
            let _name = row_data.get("name").unwrap();
        })
    });

    // 18. HashMap operations comparison
    c.bench_function("old_hashmap_creation", |b| {
        b.iter(|| {
            // Old approach: HashMap created every time
            let _row_data = storage_format
                .deserialize_row_full(black_box(&serialized_data), black_box(&test_schema))
                .unwrap();
        })
    });

    // Clean up
    drop(db);
    let _ = fs::remove_file(&path);
}

criterion_group!(benches, lazy_storage_benchmark);
criterion_main!(benches);
