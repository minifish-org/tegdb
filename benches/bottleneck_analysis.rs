use criterion::{criterion_group, criterion_main, Criterion};
use std::collections::HashMap;
use std::env;
use std::fs;
use std::hint::black_box;
use std::path::PathBuf;

/// Creates a unique temporary file path for benchmarks
fn temp_db_path(prefix: &str) -> PathBuf {
    let mut path = env::temp_dir();
    let pid = std::process::id();
    path.push(format!("tegdb_bench_{prefix}_{pid}"));
    path
}

/// Detailed execution breakdown benchmark for prepared statements
/// Goal: Identify bottlenecks in the execution phase of a single prepared statement
fn bottleneck_analysis(c: &mut Criterion) {
    let path = temp_db_path("bottleneck");
    if path.exists() {
        fs::remove_file(&path).expect("Failed to remove existing test file");
    }

    let display_path = path.display();
    let mut db =
        tegdb::Database::open(format!("file://{display_path}")).expect("Failed to create database");

    // Create test table with primary key
    db.execute("CREATE TABLE test (id INTEGER PRIMARY KEY, value INTEGER, name TEXT(32))")
        .unwrap();

    // Insert test data for realistic benchmarks
    println!("Setting up test data...");
    for i in 1..=1000 {
        let value = i * 2;
        db.execute(&format!(
            "INSERT INTO test (id, value, name) VALUES ({i}, {value}, 'item_{i}')"
        ))
        .unwrap();
    }
    println!("Test data setup complete. Running detailed execution breakdown...");

    // === FOCUSED EXECUTION BREAKDOWN ===
    // Use a single prepared statement: PK lookup
    let pk_lookup_stmt = db
        .prepare("SELECT id, value FROM test WHERE id = ?1")
        .unwrap();

    // === 1. PARAMETER BINDING BREAKDOWN ===
    let mut group = c.benchmark_group("Parameter Binding Breakdown");

    // Test parameter binding overhead
    group.bench_function("bind_single_integer", |b| {
        b.iter(|| {
            let params = vec![tegdb::SqlValue::Integer(500)];
            black_box(params);
        });
    });

    group.bench_function("bind_single_text", |b| {
        b.iter(|| {
            let params = vec![tegdb::SqlValue::Text("item_500".to_string())];
            black_box(params);
        });
    });

    group.bench_function("bind_mixed_types", |b| {
        b.iter(|| {
            let params = vec![
                tegdb::SqlValue::Integer(500),
                tegdb::SqlValue::Text("test".to_string()),
                tegdb::SqlValue::Integer(100),
            ];
            black_box(params);
        });
    });

    group.finish();

    // === 2. EXECUTION PLAN BINDING BREAKDOWN ===
    let mut group = c.benchmark_group("Execution Plan Binding Breakdown");

    // Test binding parameters to execution plan
    group.bench_function("bind_parameters_to_plan", |b| {
        b.iter(|| {
            let params = vec![tegdb::SqlValue::Integer(500)];
            let _result = db.query_prepared(&pk_lookup_stmt, &params).unwrap();
            black_box(_result);
        });
    });

    group.finish();

    // === 3. TRANSACTION CREATION BREAKDOWN ===
    let mut group = c.benchmark_group("Transaction Creation Breakdown");

    // Test transaction creation overhead
    group.bench_function("create_transaction", |b| {
        b.iter(|| {
            let _tx = db.begin_transaction().unwrap();
            black_box(_tx);
        });
    });

    group.finish();

    // === 4. QUERY PROCESSOR CREATION BREAKDOWN ===
    // Note: QueryProcessor creation is internal to the database API
    // and not directly measurable from the public interface

    // === 5. STORAGE ACCESS BREAKDOWN ===
    let mut group = c.benchmark_group("Storage Access Breakdown");

    // Test storage engine operations
    let engine = tegdb::StorageEngine::new(temp_db_path("storage_test")).unwrap();

    group.bench_function("storage_get_nonexistent", |b| {
        b.iter(|| {
            let _result = engine.get(b"nonexistent_key");
            black_box(_result);
        });
    });

    group.bench_function("storage_scan_empty", |b| {
        b.iter(|| {
            let _result = engine.scan(b"test:".to_vec()..b"test~".to_vec());
            let _ = black_box(_result);
        });
    });

    group.finish();

    // === 6. ROW DESERIALIZATION BREAKDOWN ===
    let mut group = c.benchmark_group("Row Deserialization Breakdown");

    // Create test data for deserialization
    let storage_format = tegdb::storage_format::StorageFormat::new();
    let test_schema = {
        let mut schema = tegdb::query_processor::TableSchema {
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
            ],
            indexes: Vec::new(),
        };
        let _ = tegdb::catalog::Catalog::compute_table_metadata(&mut schema);
        schema
    };

    let test_row_data = {
        let mut row = HashMap::new();
        row.insert("id".to_string(), tegdb::parser::SqlValue::Integer(123));
        row.insert("value".to_string(), tegdb::parser::SqlValue::Integer(456));
        row
    };

    let serialized_data = storage_format
        .serialize_row(&test_row_data, &test_schema)
        .unwrap();

    group.bench_function("deserialize_full_row", |b| {
        b.iter(|| {
            let _result = storage_format
                .deserialize_row_full(black_box(&serialized_data), black_box(&test_schema))
                .unwrap();
            black_box(_result);
        });
    });

    group.bench_function("deserialize_partial_columns", |b| {
        b.iter(|| {
            let columns = ["id".to_string()];
            let columns_ref: Vec<&str> = columns.iter().map(|s| s.as_str()).collect();
            let _result = storage_format
                .get_columns(
                    black_box(&serialized_data),
                    black_box(&test_schema),
                    &columns_ref,
                )
                .unwrap();
            black_box(_result);
        });
    });

    group.finish();

    // === 7. CONDITION EVALUATION BREAKDOWN ===
    let mut group = c.benchmark_group("Condition Evaluation Breakdown");

    // Test condition evaluation overhead
    let condition = tegdb::Condition::Comparison {
        left: tegdb::Expression::Column("id".to_string()),
        operator: tegdb::ComparisonOperator::Equal,
        right: tegdb::SqlValue::Integer(500),
    };

    group.bench_function("evaluate_simple_condition", |b| {
        b.iter(|| {
            let row_data = {
                let mut map = HashMap::new();
                map.insert("id".to_string(), tegdb::SqlValue::Integer(500));
                map.insert("value".to_string(), tegdb::SqlValue::Integer(1000));
                map
            };
            let _result = tegdb::sql_utils::evaluate_condition(&condition, &row_data);
            black_box(_result);
        });
    });

    group.finish();

    // === 8. COMPLETE EXECUTION PIPELINE BREAKDOWN ===
    let mut group = c.benchmark_group("Complete Execution Pipeline Breakdown");

    // Test the complete execution pipeline
    group.bench_function("complete_pk_lookup_execution", |b| {
        b.iter(|| {
            let params = vec![tegdb::SqlValue::Integer(500)];
            let _result = db.query_prepared(&pk_lookup_stmt, &params).unwrap();
            black_box(_result);
        });
    });

    // Test execution with different parameters (to avoid caching effects)
    group.bench_function("pk_lookup_varying_params", |b| {
        b.iter_with_setup(
            || {
                let mut params = Vec::new();
                for i in 1..=10 {
                    params.push(tegdb::SqlValue::Integer(i * 50));
                }
                params
            },
            |params| {
                for param in params {
                    let _result = db.query_prepared(&pk_lookup_stmt, &[param]).unwrap();
                    black_box(_result);
                }
            },
        );
    });

    group.finish();

    // === 9. MEMORY ALLOCATION BREAKDOWN ===
    let mut group = c.benchmark_group("Memory Allocation Breakdown");

    // Test various allocation patterns in execution
    group.bench_function("allocate_result_vector", |b| {
        b.iter(|| {
            let mut vec = Vec::with_capacity(10);
            for i in 0..10 {
                vec.push(tegdb::SqlValue::Integer(i));
            }
            black_box(vec);
        });
    });

    group.bench_function("allocate_row_hashmap", |b| {
        b.iter(|| {
            let mut map = HashMap::with_capacity(3);
            map.insert("id".to_string(), tegdb::SqlValue::Integer(500));
            map.insert("value".to_string(), tegdb::SqlValue::Integer(1000));
            map.insert(
                "name".to_string(),
                tegdb::SqlValue::Text("test".to_string()),
            );
            black_box(map);
        });
    });

    group.finish();

    // === 10. STRING OPERATIONS BREAKDOWN ===
    let mut group = c.benchmark_group("String Operations Breakdown");

    // Test string operations that happen during execution
    group.bench_function("format_column_key", |b| {
        b.iter(|| {
            let table_name = "test";
            let column_name = "id";
            let key = format!("{table_name}:{column_name}");
            black_box(key);
        });
    });

    group.bench_function("string_clone_operations", |b| {
        b.iter(|| {
            let column_names = vec!["id".to_string(), "value".to_string(), "name".to_string()];
            for name in &column_names {
                let _cloned = name.clone();
                black_box(_cloned);
            }
        });
    });

    group.finish();

    // === CLEANUP ===
    drop(pk_lookup_stmt);
    drop(engine);
    drop(db);
    let _ = fs::remove_file(&path);
    let _ = fs::remove_file(temp_db_path("storage_test"));

    println!("Detailed execution breakdown completed!");
}

criterion_group!(benches, bottleneck_analysis);
criterion_main!(benches);
