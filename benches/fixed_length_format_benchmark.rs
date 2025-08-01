use criterion::{criterion_group, criterion_main, Criterion};
use std::collections::HashMap;
use std::hint::black_box;
use tegdb::parser::{DataType, SqlValue};
use tegdb::query_processor::{ColumnInfo, TableSchema};
use tegdb::storage_format::StorageFormat;

/// Create a test schema with fixed-length columns
fn create_fixed_length_schema() -> TableSchema {
    TableSchema {
        name: "users".to_string(),
        columns: vec![
            ColumnInfo {
                name: "id".to_string(),
                data_type: DataType::Integer,
                constraints: vec![tegdb::parser::ColumnConstraint::PrimaryKey],
                storage_offset: 0,
                storage_size: 0,
                storage_type_code: 0,
            },
            ColumnInfo {
                name: "name".to_string(),
                data_type: DataType::Text(Some(50)), // Fixed 50-byte text
                constraints: vec![],
                storage_offset: 0,
                storage_size: 0,
                storage_type_code: 0,
            },
            ColumnInfo {
                name: "email".to_string(),
                data_type: DataType::Text(Some(100)), // Fixed 100-byte text
                constraints: vec![],
                storage_offset: 0,
                storage_size: 0,
                storage_type_code: 0,
            },
            ColumnInfo {
                name: "age".to_string(),
                data_type: DataType::Integer,
                constraints: vec![],
                storage_offset: 0,
                storage_size: 0,
                storage_type_code: 0,
            },
            ColumnInfo {
                name: "score".to_string(),
                data_type: DataType::Real,
                constraints: vec![],
                storage_offset: 0,
                storage_size: 0,
                storage_type_code: 0,
            },
            ColumnInfo {
                name: "avatar".to_string(),
                data_type: DataType::Text(Some(256)), // Fixed 256-byte text
                constraints: vec![],
                storage_offset: 0,
                storage_size: 0,
                storage_type_code: 0,
            },
        ],
        indexes: Vec::new(),
    }
}

/// Create test row data
fn create_test_row() -> HashMap<String, SqlValue> {
    let mut row = HashMap::new();
    row.insert("id".to_string(), SqlValue::Integer(12345));
    row.insert("name".to_string(), SqlValue::Text("John Doe".to_string()));
    row.insert(
        "email".to_string(),
        SqlValue::Text("john.doe@example.com".to_string()),
    );
    row.insert("age".to_string(), SqlValue::Integer(30));
    row.insert("score".to_string(), SqlValue::Real(95.5));
    row.insert(
        "avatar".to_string(),
        SqlValue::Text("binary_data_here".to_string()),
    );
    row
}

fn fixed_length_format_benchmark(c: &mut Criterion) {
    let mut schema = create_fixed_length_schema();
    // Compute metadata once, outside of benchmarks
    let _ = tegdb::catalog::Catalog::compute_table_metadata(&mut schema);
    let storage = StorageFormat::new();
    let test_row = create_test_row();

    // Benchmark 1: Record size calculation
    c.bench_function("record_size_calculation", |b| {
        b.iter(|| {
            let _size = storage.get_record_size(black_box(&schema)).unwrap();
        })
    });

    // Benchmark 3: Row serialization
    c.bench_function("row_serialization", |b| {
        b.iter(|| {
            let _serialized = storage
                .serialize_row(black_box(&test_row), black_box(&schema))
                .unwrap();
        })
    });

    // Benchmark 4: Row deserialization
    let serialized_data = storage.serialize_row(&test_row, &schema).unwrap();
    c.bench_function("row_deserialization", |b| {
        b.iter(|| {
            let _deserialized = storage
                .deserialize_row_full(black_box(&serialized_data), black_box(&schema))
                .unwrap();
        })
    });

    // Benchmark 5: Partial column deserialization
    let column_names = ["id".to_string(), "name".to_string(), "score".to_string()];
    let column_refs: Vec<&str> = column_names.iter().map(|s| s.as_str()).collect();
    c.bench_function("partial_column_deserialization", |b| {
        b.iter(|| {
            let _values = storage
                .get_columns(
                    black_box(&serialized_data),
                    black_box(&schema),
                    black_box(&column_refs),
                )
                .unwrap();
        })
    });

    // Benchmark 6: Single column access by index
    c.bench_function("single_column_by_index", |b| {
        b.iter(|| {
            let _value = storage
                .get_column_by_index(black_box(&serialized_data), black_box(&schema), 0)
                .unwrap();
        })
    });

    // Benchmark 7: Multiple columns by index
    let column_indices = vec![0, 1, 4]; // id, name, score
    c.bench_function("multiple_columns_by_index", |b| {
        b.iter(|| {
            let mut values = Vec::new();
            for &index in &column_indices {
                values.push(
                    storage
                        .get_column_by_index(black_box(&serialized_data), black_box(&schema), index)
                        .unwrap(),
                );
            }
            values
        })
    });

    // Benchmark 8: Large dataset simulation
    let mut large_schema = TableSchema {
        name: "large_table".to_string(),
        columns: vec![
            ColumnInfo {
                name: "id".to_string(),
                data_type: DataType::Integer,
                constraints: vec![tegdb::parser::ColumnConstraint::PrimaryKey],
                storage_offset: 0,
                storage_size: 0,
                storage_type_code: 0,
            },
            ColumnInfo {
                name: "data1".to_string(),
                data_type: DataType::Text(Some(200)),
                constraints: vec![],
                storage_offset: 0,
                storage_size: 0,
                storage_type_code: 0,
            },
            ColumnInfo {
                name: "data2".to_string(),
                data_type: DataType::Text(Some(200)),
                constraints: vec![],
                storage_offset: 0,
                storage_size: 0,
                storage_type_code: 0,
            },
            ColumnInfo {
                name: "data3".to_string(),
                data_type: DataType::Text(Some(200)),
                constraints: vec![],
                storage_offset: 0,
                storage_size: 0,
                storage_type_code: 0,
            },
            ColumnInfo {
                name: "value1".to_string(),
                data_type: DataType::Integer,
                constraints: vec![],
                storage_offset: 0,
                storage_size: 0,
                storage_type_code: 0,
            },
            ColumnInfo {
                name: "value2".to_string(),
                data_type: DataType::Real,
                constraints: vec![],
                storage_offset: 0,
                storage_size: 0,
                storage_type_code: 0,
            },
            ColumnInfo {
                name: "blob_data".to_string(),
                data_type: DataType::Text(Some(500)),
                constraints: vec![],
                storage_offset: 0,
                storage_size: 0,
                storage_type_code: 0,
            },
        ],
        indexes: Vec::new(),
    };
    let _ = tegdb::catalog::Catalog::compute_table_metadata(&mut large_schema);

    let large_row = {
        let mut row = HashMap::new();
        row.insert("id".to_string(), SqlValue::Integer(999999));
        row.insert("data1".to_string(), SqlValue::Text("A".repeat(150)));
        row.insert("data2".to_string(), SqlValue::Text("B".repeat(150)));
        row.insert("data3".to_string(), SqlValue::Text("C".repeat(150)));
        row.insert("value1".to_string(), SqlValue::Integer(123456));
        row.insert("value2".to_string(), SqlValue::Real(987.654));
        row.insert("blob_data".to_string(), SqlValue::Text("X".repeat(400)));
        row
    };

    let large_serialized = storage.serialize_row(&large_row, &large_schema).unwrap();

    c.bench_function("large_row_serialization", |b| {
        b.iter(|| {
            let _serialized = storage
                .serialize_row(black_box(&large_row), black_box(&large_schema))
                .unwrap();
        })
    });

    c.bench_function("large_row_deserialization", |b| {
        b.iter(|| {
            let _deserialized = storage
                .deserialize_row_full(black_box(&large_serialized), black_box(&large_schema))
                .unwrap();
        })
    });

    // Benchmark 9: Memory efficiency test
    c.bench_function("memory_efficiency", |b| {
        b.iter(|| {
            // Simulate processing many rows
            for i in 0..100 {
                let mut row = HashMap::new();
                row.insert("id".to_string(), SqlValue::Integer(i));
                row.insert("name".to_string(), SqlValue::Text(format!("User{i}")));
                row.insert(
                    "email".to_string(),
                    SqlValue::Text(format!("user{i}@example.com")),
                );
                row.insert("age".to_string(), SqlValue::Integer(20 + (i % 50)));
                row.insert("score".to_string(), SqlValue::Real(50.0 + (i as f64 * 0.5)));
                row.insert(
                    "avatar".to_string(),
                    SqlValue::Text(format!("avatar_data_{i}")),
                );

                let _serialized = storage.serialize_row(&row, &schema).unwrap();
            }
        })
    });

    // Benchmark 10: Record size comparison
    let record_size = storage.get_record_size(&schema).unwrap();
    println!("Fixed-length record size: {record_size} bytes");
    println!("Record layout:");
    println!("  - Integer columns: 8 bytes each");
    println!("  - Real columns: 8 bytes each");
    println!("  - Text(50): 50 bytes");
    println!("  - Text(100): 100 bytes");
    println!("  - Text(256): 256 bytes");
    println!("  - Total: {record_size} bytes (predictable!)");

    // Benchmark 11: Zero-copy access simulation
    c.bench_function("zero_copy_access_simulation", |b| {
        b.iter(|| {
            // Access the pre-computed metadata from the schema
            for column in &schema.columns {
                let _offset = column.storage_offset;
                let _size = column.storage_size;
                let _type_code = column.storage_type_code;
            }
        })
    });
}

criterion_group!(benches, fixed_length_format_benchmark);
criterion_main!(benches);
