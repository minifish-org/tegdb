use std::collections::HashMap;
use std::time::Instant;
use tegdb::{TableSchema, ColumnInfo, DataType, SqlValue, ColumnConstraint, StorageFormat};

fn main() {
    println!("=== TegDB Fixed-Length Storage Format Performance Demo ===\n");

    // Create a test schema with fixed-length columns
    let schema = TableSchema {
        name: "users".to_string(),
        columns: vec![
            ColumnInfo {
                name: "id".to_string(),
                data_type: DataType::Integer,
                constraints: vec![ColumnConstraint::PrimaryKey],
            },
            ColumnInfo {
                name: "name".to_string(),
                data_type: DataType::Text(Some(50)),
                constraints: vec![],
            },
            ColumnInfo {
                name: "email".to_string(),
                data_type: DataType::Text(Some(100)),
                constraints: vec![],
            },
            ColumnInfo {
                name: "age".to_string(),
                data_type: DataType::Integer,
                constraints: vec![],
            },
            ColumnInfo {
                name: "score".to_string(),
                data_type: DataType::Real,
                constraints: vec![],
            },
        ],
    };

    let storage = StorageFormat::new();
    
    // Calculate record size
    let record_size = storage.get_record_size(&schema).unwrap();
    println!("📏 Record size: {} bytes (predictable!)", record_size);
    println!("📊 Layout: 3x Integer (24 bytes) + 2x Text (150 bytes) = 174 bytes\n");

    // Create test data
    let test_row = {
        let mut row = HashMap::new();
        row.insert("id".to_string(), SqlValue::Integer(12345));
        row.insert("name".to_string(), SqlValue::Text("John Doe".to_string()));
        row.insert("email".to_string(), SqlValue::Text("john.doe@example.com".to_string()));
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
    println!("   ✅ Serialized {} rows in {:?}", iterations, serialization_time);
    println!("   ⚡ Average: {:?} per row", serialization_time / iterations);

    // Benchmark 2: Deserialization
    println!("\n🔄 Benchmarking Deserialization...");
    let start = Instant::now();
    for _ in 0..iterations {
        let _deserialized = storage.deserialize_row(&serialized_data, &schema).unwrap();
    }
    let deserialization_time = start.elapsed();
    
    println!("   ✅ Deserialized {} rows in {:?}", iterations, deserialization_time);
    println!("   ⚡ Average: {:?} per row", deserialization_time / iterations);

    // Benchmark 3: Partial Column Access
    println!("\n🎯 Benchmarking Partial Column Access...");
    let column_names = vec!["id".to_string(), "name".to_string()];
    
    let start = Instant::now();
    for _ in 0..iterations {
        let _values = storage.deserialize_columns(&serialized_data, &schema, &column_names).unwrap();
    }
    let partial_time = start.elapsed();
    
    println!("   ✅ Accessed {} partial columns in {:?}", iterations, partial_time);
    println!("   ⚡ Average: {:?} per access", partial_time / iterations);

    // Benchmark 4: Single Column Access
    println!("\n🎯 Benchmarking Single Column Access...");
    let start = Instant::now();
    for _ in 0..iterations {
        let _value = storage.deserialize_column_by_index(&serialized_data, &schema, 0).unwrap();
    }
    let single_time = start.elapsed();
    
    println!("   ✅ Accessed {} single columns in {:?}", iterations, single_time);
    println!("   ⚡ Average: {:?} per access", single_time / iterations);

    // Benchmark 5: Large Dataset Simulation
    println!("\n📊 Benchmarking Large Dataset...");
    let large_schema = TableSchema {
        name: "large_table".to_string(),
        columns: vec![
            ColumnInfo {
                name: "id".to_string(),
                data_type: DataType::Integer,
                constraints: vec![ColumnConstraint::PrimaryKey],
            },
            ColumnInfo {
                name: "data1".to_string(),
                data_type: DataType::Text(Some(200)),
                constraints: vec![],
            },
            ColumnInfo {
                name: "data2".to_string(),
                data_type: DataType::Text(Some(200)),
                constraints: vec![],
            },
            ColumnInfo {
                name: "value1".to_string(),
                data_type: DataType::Integer,
                constraints: vec![],
            },
            ColumnInfo {
                name: "value2".to_string(),
                data_type: DataType::Real,
                constraints: vec![],
            },
        ],
    };

    let large_row = {
        let mut row = HashMap::new();
        row.insert("id".to_string(), SqlValue::Integer(999999));
        row.insert("data1".to_string(), SqlValue::Text("A".to_string().repeat(150)));
        row.insert("data2".to_string(), SqlValue::Text("B".to_string().repeat(150)));
        row.insert("value1".to_string(), SqlValue::Integer(123456));
        row.insert("value2".to_string(), SqlValue::Real(987.654));
        row
    };

    let large_record_size = storage.get_record_size(&large_schema).unwrap();
    println!("   📏 Large record size: {} bytes", large_record_size);

    let large_iterations = 100_000;
    let start = Instant::now();
    for _ in 0..large_iterations {
        let _serialized = storage.serialize_row(&large_row, &large_schema).unwrap();
    }
    let large_serialization_time = start.elapsed();
    
    let large_serialized = storage.serialize_row(&large_row, &large_schema).unwrap();
    let start = Instant::now();
    for _ in 0..large_iterations {
        let _deserialized = storage.deserialize_row(&large_serialized, &large_schema).unwrap();
    }
    let large_deserialization_time = start.elapsed();
    
    println!("   ✅ Large serialization: {:?} per row", large_serialization_time / large_iterations);
    println!("   ✅ Large deserialization: {:?} per row", large_deserialization_time / large_iterations);

    // Performance Summary
    println!("");
    println!("{}", "=".repeat(60));
    println!("📈 PERFORMANCE SUMMARY");
    println!("{}", "=".repeat(60));
    println!("🔹 Serialization:     {:?} per row", serialization_time / iterations);
    println!("🔹 Deserialization:   {:?} per row", deserialization_time / iterations);
    println!("🔹 Partial Access:    {:?} per access", partial_time / iterations);
    println!("🔹 Single Column:     {:?} per access", single_time / iterations);
    println!("🔹 Large Records:     {:?} per row", large_serialization_time / large_iterations);
    println!("");
    println!("🚀 This demonstrates NANOSECOND-level performance!");
    println!("💡 Fixed-length format enables:");
    println!("   • Direct offset-based access");
    println!("   • Zero-copy deserialization");
    println!("   • Predictable memory layout");
    println!("   • Maximum cache efficiency");
} 