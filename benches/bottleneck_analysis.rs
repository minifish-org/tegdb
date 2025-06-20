use criterion::{black_box, criterion_group, criterion_main, Criterion};
use std::path::PathBuf;
use std::env;
use std::fs;
use std::collections::HashMap;

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
    db.execute("CREATE TABLE test (id INTEGER PRIMARY KEY, value INTEGER)").unwrap();
    
    // Benchmark just the parsing
    c.bench_function("just parsing", |b| {
        b.iter(|| {
            let (_remaining, _statement) = tegdb::parser::parse_sql(black_box("SELECT * FROM test WHERE id = 1")).unwrap();
        })
    });
    
    // Benchmark schema clone (simulate what Database.execute does)
    let schemas = {
        let mut dummy_schemas = HashMap::new();
        dummy_schemas.insert("test".to_string(), tegdb::executor::TableSchema {
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
        });
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
    
    // Benchmark the actual serialization bottleneck
    let test_row_data = {
        let mut row = HashMap::new();
        row.insert("id".to_string(), tegdb::parser::SqlValue::Integer(123));
        row.insert("name".to_string(), tegdb::parser::SqlValue::Text("John Doe".to_string()));
        row.insert("value".to_string(), tegdb::parser::SqlValue::Real(45.67));
        row
    };
    
    // Current text-based serialization (the slow approach)
    c.bench_function("text serialization", |b| {
        b.iter(|| {
            // Simulate current serialize_row implementation
            let serialized = black_box(test_row_data.iter()
                .map(|(k, v)| match v {
                    tegdb::parser::SqlValue::Integer(i) => format!("{}:int:{}", k, i),
                    tegdb::parser::SqlValue::Real(r) => format!("{}:real:{}", k, r),
                    tegdb::parser::SqlValue::Text(t) => format!("{}:text:{}", k, t),
                    tegdb::parser::SqlValue::Null => format!("{}:null:", k),
                })
                .collect::<Vec<_>>()
                .join("|"));
            
            // Simulate current deserialize_row implementation
            let mut row_data = HashMap::new();
            for part in serialized.split('|') {
                if !part.is_empty() {
                    let components: Vec<&str> = part.splitn(3, ':').collect();
                    if components.len() >= 3 {
                        let column_name = components[0].to_string();
                        let value_type = components[1];
                        let value_str = components[2];
                        
                        let value = match value_type {
                            "int" => tegdb::parser::SqlValue::Integer(value_str.parse().unwrap_or(0)),
                            "real" => tegdb::parser::SqlValue::Real(value_str.parse().unwrap_or(0.0)),
                            "text" => tegdb::parser::SqlValue::Text(value_str.to_string()),
                            "null" => tegdb::parser::SqlValue::Null,
                            _ => tegdb::parser::SqlValue::Null,
                        };
                        
                        row_data.insert(column_name, value);
                    }
                }
            }
            black_box(row_data);
        })
    });
    
    // Fast binary serialization (the fix)
    c.bench_function("binary serialization", |b| {
        b.iter(|| {
            // Efficient binary serialization
            let mut buffer = Vec::with_capacity(256);
            
            // Write number of columns
            buffer.extend_from_slice(&(test_row_data.len() as u32).to_le_bytes());
            
            for (key, value) in &test_row_data {
                // Write key length and key
                buffer.extend_from_slice(&(key.len() as u32).to_le_bytes());
                buffer.extend_from_slice(key.as_bytes());
                
                // Write value type and value
                match value {
                    tegdb::parser::SqlValue::Integer(i) => {
                        buffer.push(1); // type tag
                        buffer.extend_from_slice(&i.to_le_bytes());
                    },
                    tegdb::parser::SqlValue::Real(r) => {
                        buffer.push(2); // type tag
                        buffer.extend_from_slice(&r.to_le_bytes());
                    },
                    tegdb::parser::SqlValue::Text(t) => {
                        buffer.push(3); // type tag
                        buffer.extend_from_slice(&(t.len() as u32).to_le_bytes());
                        buffer.extend_from_slice(t.as_bytes());
                    },
                    tegdb::parser::SqlValue::Null => {
                        buffer.push(0); // type tag
                    },
                }
            }
            
            // Fast binary deserialization
            let mut cursor = 0;
            let mut row_data = HashMap::new();
            
            // Read number of columns
            let num_cols = u32::from_le_bytes([
                buffer[cursor], buffer[cursor+1], buffer[cursor+2], buffer[cursor+3]
            ]) as usize;
            cursor += 4;
            
            for _ in 0..num_cols {
                // Read key
                let key_len = u32::from_le_bytes([
                    buffer[cursor], buffer[cursor+1], buffer[cursor+2], buffer[cursor+3]
                ]) as usize;
                cursor += 4;
                let key = String::from_utf8_lossy(&buffer[cursor..cursor+key_len]).to_string();
                cursor += key_len;
                
                // Read value
                let value_type = buffer[cursor];
                cursor += 1;
                
                let value = match value_type {
                    1 => { // Integer
                        let val = i64::from_le_bytes([
                            buffer[cursor], buffer[cursor+1], buffer[cursor+2], buffer[cursor+3],
                            buffer[cursor+4], buffer[cursor+5], buffer[cursor+6], buffer[cursor+7]
                        ]);
                        cursor += 8;
                        tegdb::parser::SqlValue::Integer(val)
                    },
                    2 => { // Real
                        let val = f64::from_le_bytes([
                            buffer[cursor], buffer[cursor+1], buffer[cursor+2], buffer[cursor+3],
                            buffer[cursor+4], buffer[cursor+5], buffer[cursor+6], buffer[cursor+7]
                        ]);
                        cursor += 8;
                        tegdb::parser::SqlValue::Real(val)
                    },
                    3 => { // Text
                        let text_len = u32::from_le_bytes([
                            buffer[cursor], buffer[cursor+1], buffer[cursor+2], buffer[cursor+3]
                        ]) as usize;
                        cursor += 4;
                        let text = String::from_utf8_lossy(&buffer[cursor..cursor+text_len]).to_string();
                        cursor += text_len;
                        tegdb::parser::SqlValue::Text(text)
                    },
                    _ => tegdb::parser::SqlValue::Null,
                };
                
                row_data.insert(key, value);
            }
            
            black_box(row_data);
        })
    });
    
    // Clean up
    drop(db);
    drop(engine);
    let _ = fs::remove_file(&path);
    let _ = fs::remove_file(&temp_db_path("tx_test"));
}

criterion_group!(benches, bottleneck_analysis);
criterion_main!(benches);
