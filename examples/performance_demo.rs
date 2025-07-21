use std::collections::HashMap;
use std::time::Instant;
use tegdb::{ColumnConstraint, ColumnInfo, DataType, SqlValue, StorageFormat, TableSchema};

fn main() {
    println!("=== TegDB Fixed-Length Storage Format Performance Demo ===\n");

    // Create a test schema with fixed-length columns
    let mut schema = TableSchema {
        name: "users".to_string(),
        columns: vec![
            ColumnInfo {
                name: "id".to_string(),
                data_type: DataType::Integer,
                constraints: vec![ColumnConstraint::PrimaryKey],
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
                name: "email".to_string(),
                data_type: DataType::Text(Some(100)),
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
        ],
    };
    let _ = tegdb::catalog::Catalog::compute_table_metadata(&mut schema);

    let storage = StorageFormat::new();

    // Calculate record size
    let record_size = storage.get_record_size(&schema).unwrap();
    println!("📏 Record size: {record_size} bytes (predictable!)");
    println!("📊 Layout: 3x Integer (24 bytes) + 2x Text (150 bytes) = 174 bytes\n");

    // Create test data
    let test_row = {
        let mut row = HashMap::new();
        row.insert("id".to_string(), SqlValue::Integer(12345));
        row.insert("name".to_string(), SqlValue::Text("John Doe".to_string()));
        row.insert(
            "email".to_string(),
            SqlValue::Text("john.doe@example.com".to_string()),
        );
        row.insert("age".to_string(), SqlValue::Integer(30));
        row.insert("score".to_string(), SqlValue::Real(95.5));
        row
    };

    // Benchmark 1: Serialization
    println!("🚀 Benchmarking Serialization...");
    let iterations = 1_000_000;

    let start = Instant::now();
    for _ in 0..iterations {
        let _serialized = storage.serialize_row(&test_row, &schema).unwrap();
    }
    let serialization_time = start.elapsed();

    let serialized_data = storage.serialize_row(&test_row, &schema).unwrap();
    println!("   ✅ Serialized {iterations} rows in {serialization_time:?}");
    println!(
        "   ⚡ Average: {:?} per row",
        serialization_time / iterations
    );

    // Benchmark 2: Deserialization
    println!("\n🔄 Benchmarking Deserialization...");
    let start = Instant::now();
    for _ in 0..iterations {
        let _deserialized = storage.deserialize_row_full(&serialized_data, &schema).unwrap();
    }
    let deserialization_time = start.elapsed();

    println!("   ✅ Deserialized {iterations} rows in {deserialization_time:?}");
    println!(
        "   ⚡ Average: {:?} per row",
        deserialization_time / iterations
    );

    // Benchmark 3: Partial Column Access
    println!("\n🎯 Benchmarking Partial Column Access...");
    let column_names = vec!["id".to_string(), "name".to_string()];
    let column_refs: Vec<&str> = column_names.iter().map(|s| s.as_str()).collect();

    let start = Instant::now();
    for _ in 0..iterations {
        let _values = storage
            .get_columns(&serialized_data, &schema, &column_refs)
            .unwrap();
    }
    let partial_time = start.elapsed();

    println!("   ✅ Accessed {iterations} partial columns in {partial_time:?}");
    println!("   ⚡ Average: {:?} per access", partial_time / iterations);

    // Benchmark 4: Single Column Access
    println!("\n🎯 Benchmarking Single Column Access...");
    let start = Instant::now();
    for _ in 0..iterations {
        let _value = storage
            .get_column_by_index(&serialized_data, &schema, 0)
            .unwrap();
    }
    let single_time = start.elapsed();

    println!("   ✅ Accessed {iterations} single columns in {single_time:?}");
    println!("   ⚡ Average: {:?} per access", single_time / iterations);

    // Benchmark 5: Large Dataset Simulation
    println!("\n📊 Benchmarking Large Dataset...");
    let mut large_schema = TableSchema {
        name: "large_table".to_string(),
        columns: vec![
            ColumnInfo {
                name: "id".to_string(),
                data_type: DataType::Integer,
                constraints: vec![ColumnConstraint::PrimaryKey],
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
        ],
    };
    let _ = tegdb::catalog::Catalog::compute_table_metadata(&mut large_schema);

    let large_row = {
        let mut row = HashMap::new();
        row.insert("id".to_string(), SqlValue::Integer(999999));
        row.insert(
            "data1".to_string(),
            SqlValue::Text("A".to_string().repeat(150)),
        );
        row.insert(
            "data2".to_string(),
            SqlValue::Text("B".to_string().repeat(150)),
        );
        row.insert("value1".to_string(), SqlValue::Integer(123456));
        row.insert("value2".to_string(), SqlValue::Real(987.654));
        row
    };

    let large_record_size = storage.get_record_size(&large_schema).unwrap();
    println!("   📏 Large record size: {large_record_size} bytes");

    let large_iterations = 100_000;
    let start = Instant::now();
    for _ in 0..large_iterations {
        let _serialized = storage.serialize_row(&large_row, &large_schema).unwrap();
    }
    let large_serialization_time = start.elapsed();

    let large_serialized = storage.serialize_row(&large_row, &large_schema).unwrap();
    let start = Instant::now();
    for _ in 0..large_iterations {
        let _deserialized = storage
            .deserialize_row_full(&large_serialized, &large_schema)
            .unwrap();
    }
    let large_deserialization_time = start.elapsed();

    println!(
        "   ✅ Large serialization: {:?} per row",
        large_serialization_time / large_iterations
    );
    println!(
        "   ✅ Large deserialization: {:?} per row",
        large_deserialization_time / large_iterations
    );

    // Performance Summary
    println!();
    println!("{}", "=".repeat(60));
    println!("📈 PERFORMANCE SUMMARY");
    println!("{}", "=".repeat(60));
    println!(
        "🔹 Serialization:     {:?} per row",
        serialization_time / iterations
    );
    println!(
        "🔹 Deserialization:   {:?} per row",
        deserialization_time / iterations
    );
    println!(
        "🔹 Partial Access:    {:?} per access",
        partial_time / iterations
    );
    println!(
        "🔹 Single Column:     {:?} per access",
        single_time / iterations
    );
    println!(
        "🔹 Large Records:     {:?} per row",
        large_serialization_time / large_iterations
    );
    println!();
    println!("🚀 This demonstrates NANOSECOND-level performance!");
    println!("💡 Fixed-length format enables:");
    println!("   • Direct offset-based access");
    println!("   • Zero-copy deserialization");
    println!("   • Predictable memory layout");
    println!("   • Maximum cache efficiency");
}
