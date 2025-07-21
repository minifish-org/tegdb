use criterion::{criterion_group, criterion_main, Criterion};
use std::collections::HashMap;
use std::env;
use std::hint::black_box;
use std::path::PathBuf;
use tegdb::{ColumnInfo, DataType, SqlValue, StorageFormat, TableSchema};

/// Creates a unique temporary file path for benchmarks
fn temp_db_path(prefix: &str) -> PathBuf {
    let mut path = env::temp_dir();
    path.push(format!(
        "tegdb_storage_bench_{}_{}",
        prefix,
        std::process::id()
    ));
    path
}

/// Create a test schema with various data types
fn create_test_schema() -> TableSchema {
    TableSchema {
        name: "benchmark_table".to_string(),
        columns: vec![
            ColumnInfo {
                name: "id".to_string(),
                data_type: DataType::Integer,
                constraints: vec![],
                storage_offset: 0,
                storage_size: 0,
                storage_type_code: 0,
            },
            ColumnInfo {
                name: "name".to_string(),
                data_type: DataType::Text(Some(50)),
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
                name: "description".to_string(),
                data_type: DataType::Text(Some(100)),
                constraints: vec![],
                storage_offset: 0,
                storage_size: 0,
                storage_type_code: 0,
            },
            ColumnInfo {
                name: "metadata".to_string(),
                data_type: DataType::Text(Some(200)),
                constraints: vec![],
                storage_offset: 0,
                storage_size: 0,
                storage_type_code: 0,
            },
        ],
    }
}

/// Create test row data
fn create_test_row(id: i64) -> HashMap<String, SqlValue> {
    let mut row = HashMap::new();
    row.insert("id".to_string(), SqlValue::Integer(id));
    row.insert("name".to_string(), SqlValue::Text(format!("User_{id}")));
    row.insert("score".to_string(), SqlValue::Real(id as f64 * 1.5));
    row.insert(
        "description".to_string(),
        SqlValue::Text(format!("Description for user {id}")),
    );
    row.insert(
        "metadata".to_string(),
        SqlValue::Text(format!("metadata_{id}_long_string_for_testing")),
    );
    row
}

/// Create test row with long text (to test variable-length encoding)
fn create_test_row_with_long_text(id: i64) -> HashMap<String, SqlValue> {
    let mut row = HashMap::new();
    row.insert("id".to_string(), SqlValue::Integer(id));
    row.insert("name".to_string(), SqlValue::Text(format!("User_{id}")));
    row.insert("score".to_string(), SqlValue::Real(id as f64 * 1.5));
    row.insert(
        "description".to_string(),
        SqlValue::Text(format!("Description for user {id}")),
    );

    // Create a very long string to test variable-length encoding
    let long_text = "This is a very long text field that will exceed the 65535 character limit for fixed-length encoding and will therefore use variable-length encoding with varint prefix. ".repeat(1000);
    row.insert("metadata".to_string(), SqlValue::Text(long_text));
    row
}

fn storage_format_benchmarks(c: &mut Criterion) {
    let storage_format = StorageFormat::new();
    let mut schema = create_test_schema();
    // Compute metadata once, outside of benchmarks
    let _ = tegdb::catalog::Catalog::compute_table_metadata(&mut schema);

    // ===== SERIALIZATION BENCHMARKS =====

    c.bench_function("serialize_row_small_text", |b| {
        let row_data = create_test_row(1);
        b.iter(|| {
            let serialized = storage_format
                .serialize_row(black_box(&row_data), black_box(&schema))
                .unwrap();
            black_box(serialized);
        })
    });

    c.bench_function("serialize_row_large_text", |b| {
        let row_data = create_test_row_with_long_text(1);
        b.iter(|| {
            let serialized = storage_format
                .serialize_row(black_box(&row_data), black_box(&schema))
                .unwrap();
            black_box(serialized);
        })
    });

    c.bench_function("serialize_row_batch_100", |b| {
        let rows: Vec<_> = (1..=100).map(create_test_row).collect();
        b.iter(|| {
            for row in &rows {
                let serialized = storage_format
                    .serialize_row(black_box(row), black_box(&schema))
                    .unwrap();
                black_box(serialized);
            }
        })
    });

    // ===== DESERIALIZATION BENCHMARKS =====

    let serialized_small = storage_format
        .serialize_row(&create_test_row(1), &schema)
        .unwrap();

    let serialized_large = storage_format
        .serialize_row(&create_test_row_with_long_text(1), &schema)
        .unwrap();

    c.bench_function("deserialize_row_full_small", |b| {
        b.iter(|| {
            let deserialized = storage_format
                .deserialize_row_full(black_box(&serialized_small), black_box(&schema))
                .unwrap();
            black_box(deserialized);
        })
    });

    c.bench_function("deserialize_row_full_large", |b| {
        b.iter(|| {
            let deserialized = storage_format
                .deserialize_row_full(black_box(&serialized_large), black_box(&schema))
                .unwrap();
            black_box(deserialized);
        })
    });

    // ===== PARTIAL DESERIALIZATION BENCHMARKS =====

    c.bench_function("deserialize_columns_single", |b| {
        let columns = vec!["id".to_string()];
        let columns_ref: Vec<&str> = columns.iter().map(|s| s.as_str()).collect();
        b.iter(|| {
            let values = storage_format
                .get_columns(
                    black_box(&serialized_small),
                    black_box(&schema),
                    black_box(&columns_ref),
                )
                .unwrap();
            black_box(values);
        })
    });

    c.bench_function("deserialize_columns_multiple", |b| {
        let columns = vec!["id".to_string(), "name".to_string(), "score".to_string()];
        let columns_ref: Vec<&str> = columns.iter().map(|s| s.as_str()).collect();
        b.iter(|| {
            let values = storage_format
                .get_columns(
                    black_box(&serialized_small),
                    black_box(&schema),
                    black_box(&columns_ref),
                )
                .unwrap();
            black_box(values);
        })
    });

    c.bench_function("deserialize_column_indices", |b| {
        let indices = vec![0, 1, 2]; // id, name, score
        b.iter(|| {
            let values: Vec<_> = indices.iter().map(|&i| storage_format.get_column_by_index(black_box(&serialized_small), black_box(&schema), i).unwrap()).collect();
            black_box(values);
        })
    });

    // ===== ULTRA-FAST DESERIALIZATION BENCHMARKS =====

    // ===== ULTRA-FAST DESERIALIZATION BENCHMARKS =====

    // Note: We can't directly access private header parsing methods in benchmarks
    // So we'll focus on the public API performance

    c.bench_function("deserialize_columns_fast", |b| {
        b.iter(|| {
            let columns = vec!["id".to_string(), "name".to_string(), "score".to_string()];
            let columns_ref: Vec<&str> = columns.iter().map(|s| s.as_str()).collect();
            let values = storage_format
                .get_columns(
                    black_box(&serialized_small),
                    black_box(&schema),
                    black_box(&columns_ref),
                )
                .unwrap();
            black_box(values);
        })
    });

    c.bench_function("deserialize_column_indices_fast", |b| {
        b.iter(|| {
            let indices = vec![0, 1, 2]; // id, name, score
            let values: Vec<_> = indices.iter().map(|&i| storage_format.get_column_by_index(black_box(&serialized_small), black_box(&schema), i).unwrap()).collect();
            black_box(values);
        })
    });

    // ===== MICRO-BENCHMARKS FOR INDIVIDUAL OPERATIONS =====

    c.bench_function("varint_encode_simulation", |b| {
        b.iter(|| {
            // Simulate varint encoding overhead
            for i in 1..=100 {
                let mut result = Vec::new();
                let mut value = i;
                while value >= 128 {
                    result.push((value & 0x7F) as u8 | 0x80);
                    value >>= 7;
                }
                result.push(value as u8);
                black_box(result);
            }
        })
    });

    c.bench_function("varint_decode_simulation", |b| {
        let test_data = vec![0x89, 0x60]; // 12345 in varint format
        b.iter(|| {
            for _ in 0..100 {
                // Simulate varint decoding
                let mut result = 0;
                let mut shift = 0;
                for &byte in &test_data {
                    result |= ((byte & 0x7F) as usize) << shift;
                    if byte & 0x80 == 0 {
                        break;
                    }
                    shift += 7;
                }
                black_box(result);
            }
        })
    });

    // ===== COMPARISON WITH OLD FORMAT (SIMULATED) =====

    // Simulate the overhead of the old format with HashMap lookups and string cloning
    c.bench_function("old_format_simulation", |b| {
        b.iter(|| {
            // Simulate HashMap creation and string lookups
            let mut map = HashMap::new();
            map.insert("id".to_string(), SqlValue::Integer(1));
            map.insert("name".to_string(), SqlValue::Text("test".to_string()));
            map.insert("score".to_string(), SqlValue::Real(1.5));

            // Simulate string cloning overhead
            for _ in 0..10 {
                let cloned_name = map.get("name").unwrap().clone();
                black_box(cloned_name);
            }
        })
    });

    // ===== REAL-WORLD SCENARIO BENCHMARKS =====

    c.bench_function("real_world_query_simulation", |b| {
        // Simulate a real-world scenario: query with WHERE clause on indexed column
        let rows: Vec<_> = (1..=50).map(create_test_row).collect();
        let serialized_rows: Vec<_> = rows
            .iter()
            .map(|row| storage_format.serialize_row(row, &schema).unwrap())
            .collect();

        b.iter(|| {
            let mut results = Vec::new();
            for serialized_row in &serialized_rows {
                // Simulate: SELECT id, name FROM table WHERE id = 25
                let columns = vec!["id".to_string(), "name".to_string()];
                let columns_ref: Vec<&str> = columns.iter().map(|s| s.as_str()).collect();
                let values = storage_format
                    .get_columns(serialized_row, &schema, &columns_ref)
                    .unwrap();

                // Simulate condition check
                if let SqlValue::Integer(id) = values[0] {
                    if id == 25 {
                        results.push(values);
                    }
                }
            }
            black_box(results);
        })
    });

    c.bench_function("bulk_processing_1000_rows", |b| {
        let rows: Vec<_> = (1..=1000).map(create_test_row).collect();
        let serialized_rows: Vec<_> = rows
            .iter()
            .map(|row| storage_format.serialize_row(row, &schema).unwrap())
            .collect();

        b.iter(|| {
            let mut total_score = 0.0;
            for serialized_row in &serialized_rows {
                // Extract only the score column for aggregation
                let columns = vec!["score".to_string()];
                let columns_ref: Vec<&str> = columns.iter().map(|s| s.as_str()).collect();
                let values = storage_format
                    .get_columns(serialized_row, &schema, &columns_ref)
                    .unwrap();

                if let SqlValue::Real(score) = values[0] {
                    total_score += score;
                }
            }
            black_box(total_score);
        })
    });

    // ===== MEMORY ALLOCATION BENCHMARKS =====

    c.bench_function("zero_allocation_column_access", |b| {
        // Use the public API to extract just the id column
        let columns = vec!["id".to_string()];

        b.iter(|| {
            // Direct access using public API
            let columns_ref: Vec<&str> = columns.iter().map(|s| s.as_str()).collect();
            let values = storage_format
                .get_columns(&serialized_small, &schema, &columns_ref)
                .unwrap();
            black_box(values[0].clone());
        })
    });

    // ===== STRING OPERATIONS BENCHMARKS =====

    c.bench_function("fixed_length_text_encoding", |b| {
        let short_text = "Hello, World!";
        b.iter(|| {
            // Provide all required columns for the schema
            let mut row_data = HashMap::new();
            row_data.insert("id".to_string(), SqlValue::Integer(1));
            row_data.insert("name".to_string(), SqlValue::Text(short_text.to_string()));
            row_data.insert("email".to_string(), SqlValue::Text("a@b.com".to_string()));
            row_data.insert("age".to_string(), SqlValue::Integer(42));
            row_data.insert("score".to_string(), SqlValue::Real(3.14));
            row_data.insert(
                "description".to_string(),
                SqlValue::Text("desc".to_string()),
            );
            row_data.insert("metadata".to_string(), SqlValue::Text("meta".to_string()));
            let serialized = storage_format.serialize_row(&row_data, &schema).unwrap();
            black_box(serialized);
        })
    });

    c.bench_function("variable_length_text_encoding", |b| {
        let long_text =
            "This is a very long text that will use variable-length encoding".repeat(100);
        b.iter(|| {
            // Provide all required columns for the schema
            let mut row_data = HashMap::new();
            row_data.insert("id".to_string(), SqlValue::Integer(1));
            row_data.insert("name".to_string(), SqlValue::Text("short".to_string()));
            row_data.insert("email".to_string(), SqlValue::Text("a@b.com".to_string()));
            row_data.insert("age".to_string(), SqlValue::Integer(42));
            row_data.insert("score".to_string(), SqlValue::Real(3.14));
            row_data.insert(
                "description".to_string(),
                SqlValue::Text("desc".to_string()),
            );
            row_data.insert("metadata".to_string(), SqlValue::Text(long_text.clone()));
            let serialized = storage_format.serialize_row(&row_data, &schema).unwrap();
            black_box(serialized);
        })
    });

    // ===== LOOKUP TABLE BENCHMARKS =====

    c.bench_function("type_size_lookup_simulation", |b| {
        b.iter(|| {
            // Simulate type size lookup overhead
            for type_code in 0..10 {
                let size = match type_code {
                    0 => 0, // NULL
                    1 => 1, // Integer1
                    2 => 2, // Integer2
                    3 => 4, // Integer4
                    4 => 8, // Integer8
                    5 => 8, // Real
                    6 => 4, // TextFixed
                    _ => 0,
                };
                black_box(size);
            }
        })
    });

    // ===== COMPREHENSIVE PERFORMANCE TEST =====

    c.bench_function("comprehensive_performance_test", |b| {
        let rows: Vec<_> = (1..=100).map(create_test_row).collect();
        let serialized_rows: Vec<_> = rows
            .iter()
            .map(|row| storage_format.serialize_row(row, &schema).unwrap())
            .collect();

        b.iter(|| {
            let mut results = Vec::new();

            // Simulate complex query processing using public API
            for serialized_row in &serialized_rows {
                // Extract multiple columns efficiently using public API
                let columns = vec!["id".to_string(), "name".to_string(), "score".to_string()];
                let columns_ref: Vec<&str> = columns.iter().map(|s| s.as_str()).collect();
                let values = storage_format
                    .get_columns(serialized_row, &schema, &columns_ref)
                    .unwrap();

                // Simulate filtering and transformation
                if let (SqlValue::Integer(id_val), SqlValue::Real(score_val)) =
                    (&values[0], &values[2])
                {
                    if *id_val > 50 && *score_val > 75.0 {
                        results.push((values[0].clone(), values[1].clone(), values[2].clone()));
                    }
                }
            }
            black_box(results);
        })
    });
}

criterion_group!(benches, storage_format_benchmarks);
criterion_main!(benches);
