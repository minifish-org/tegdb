use criterion::{criterion_group, criterion_main, Criterion};
use std::collections::HashMap;
use std::env;
use std::fs;
use std::hint::black_box;
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

    let mut db = tegdb::Database::open(format!("file://{}", path.display()))
        .expect("Failed to create database");
    db.execute("CREATE TABLE test (id INTEGER PRIMARY KEY, value INTEGER)")
        .unwrap();

    // Benchmark just the parsing
    c.bench_function("just parsing", |b| {
        b.iter(|| {
            let _statement =
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
    let mut engine = tegdb::StorageEngine::new(temp_db_path("tx_test")).unwrap();
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
    let storage_format = tegdb::storage_format::StorageFormat::new();
    c.bench_function("native serialization", |b| {
        b.iter(|| {
            let serialized = storage_format
                .serialize_row(black_box(&test_row_data), black_box(&test_schema))
                .unwrap();
            black_box(serialized);
        })
    });

    // Benchmark native row format deserialization
    let serialized_data = storage_format
        .serialize_row(&test_row_data, &test_schema)
        .unwrap();
    c.bench_function("native deserialization", |b| {
        b.iter(|| {
            let deserialized = storage_format
                .deserialize_row(black_box(&serialized_data), black_box(&test_schema))
                .unwrap();
            black_box(deserialized);
        })
    });

    // Benchmark partial column deserialization (LIMIT optimization)
    let columns_to_select = vec!["id".to_string()];
    c.bench_function("partial deserialization", |b| {
        b.iter(|| {
            let values = storage_format
                .deserialize_columns(
                    black_box(&serialized_data),
                    black_box(&test_schema),
                    black_box(&columns_to_select),
                )
                .unwrap();
            black_box(values);
        })
    });

    // ===== ENHANCED BOTTLENECK ANALYSIS =====
    
    // Benchmark Database.query() breakdown
    let sql = "SELECT id, value FROM test";
    
    // 1. Parse SQL
    c.bench_function("parse_sql", |b| {
        b.iter(|| {
            let _stmt = tegdb::parser::parse_sql(black_box(sql)).unwrap();
        })
    });

    // 2. Create planner
    let stmt = tegdb::parser::parse_sql(sql).unwrap();
    let table_schemas = db.get_table_schemas_ref().clone();
    c.bench_function("create_planner", |b| {
        b.iter(|| {
            let _planner = tegdb::planner::QueryPlanner::new(black_box(table_schemas.clone()));
        })
    });

    // 3. Plan creation
    let planner = tegdb::planner::QueryPlanner::new(table_schemas.clone());
    c.bench_function("plan_creation", |b| {
        b.iter(|| {
            let _plan = planner.plan(black_box(stmt.clone())).unwrap();
        })
    });

    // 4. Database internal query processing (without execution)
    c.bench_function("database_query_internal", |b| {
        b.iter(|| {
            // Simulate what Database.query() does internally
            let stmt = tegdb::parser::parse_sql(sql).unwrap();
            let planner = tegdb::planner::QueryPlanner::new(table_schemas.clone());
            let _plan = planner.plan(stmt).unwrap();
        })
    });

    // 5. Database transaction creation
    c.bench_function("database_transaction_creation", |b| {
        b.iter(|| {
            let _tx = db.begin_transaction().unwrap();
        })
    });

    // 6. Plan execution (just the processor part)
    c.bench_function("plan_execution_only", |b| {
        b.iter(|| {
            let mut tx = db.begin_transaction().unwrap();
            let _result = tx.query(sql).unwrap();
        })
    });

    // 7. Result processing
    c.bench_function("result_processing", |b| {
        b.iter(|| {
            let result = db.query(sql).unwrap();
            // Just iterate through results without doing anything
            for _row in result.rows() {
                // Do nothing, just iterate
            }
        })
    });

    // 8. Complete query pipeline breakdown
    c.bench_function("complete_query_breakdown", |b| {
        b.iter(|| {
            // Step 1: Parse
            let stmt = tegdb::parser::parse_sql(sql).unwrap();
            
            // Step 2: Plan
            let planner = tegdb::planner::QueryPlanner::new(table_schemas.clone());
            let _plan = planner.plan(stmt).unwrap();
            
            // Step 3: Execute (simulate with transaction)
            let mut tx = db.begin_transaction().unwrap();
            let _result = tx.query(sql).unwrap();
        })
    });

    // 9. Memory allocation analysis
    c.bench_function("memory_allocation_test", |b| {
        b.iter(|| {
            // Test various allocation patterns
            let mut vec = Vec::new();
            for i in 0..100 {
                vec.push(format!("key_{}", i));
            }
            black_box(vec);
        })
    });

    // 10. HashMap operations
    c.bench_function("hashmap_operations", |b| {
        b.iter(|| {
            let mut map = HashMap::new();
            for i in 0..10 {
                map.insert(format!("key_{}", i), format!("value_{}", i));
            }
            let _val = map.get("key_5");
            black_box(map);
        })
    });

    // 11. String operations
    c.bench_function("string_operations", |b| {
        b.iter(|| {
            let mut s = String::new();
            for i in 0..50 {
                s.push_str(&format!("part_{}", i));
            }
            black_box(s);
        })
    });

    // 12. Schema validation cache creation
    c.bench_function("schema_validation_cache", |b| {
        b.iter(|| {
            let _cache = tegdb::executor::SchemaValidationCache::new(&test_schema);
        })
    });

    // 15. Storage engine operations
    c.bench_function("storage_engine_get", |b| {
        b.iter(|| {
            let _ = engine.get(b"nonexistent_key");
        })
    });

    c.bench_function("storage_engine_set", |b| {
        b.iter(|| {
            let _ = engine.set(b"test_key", b"test_value".to_vec());
        })
    });

    // 16. Transaction operations
    c.bench_function("transaction_get", |b| {
        b.iter(|| {
            let tx = engine.begin_transaction();
            let _ = tx.get(b"nonexistent_key");
        })
    });

    c.bench_function("transaction_set", |b| {
        b.iter(|| {
            let mut tx = engine.begin_transaction();
            let _ = tx.set(b"test_key", b"test_value".to_vec());
        })
    });

    // ===== DRILL-DOWN INTO PLAN EXECUTION BOTTLENECK =====
    
    // 17. Storage engine scan operations
    c.bench_function("storage_engine_scan_empty", |b| {
        b.iter(|| {
            let _ = engine.scan(b"test:".to_vec()..b"test~".to_vec());
        })
    });

    // 18. Transaction scan operations
    c.bench_function("transaction_scan_empty", |b| {
        b.iter(|| {
            let tx = engine.begin_transaction();
            let _ = tx.scan(b"test:".to_vec()..b"test~".to_vec());
        })
    });

    // 19. Query processor creation overhead
    c.bench_function("query_processor_creation_overhead", |b| {
        b.iter(|| {
            // Simulate the overhead of creating a query processor
            let mut dummy_schemas = HashMap::new();
            dummy_schemas.insert(
                "test".to_string(),
                std::rc::Rc::new(tegdb::executor::TableSchema {
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
                }),
            );
            let transaction = engine.begin_transaction();
            let _processor = tegdb::executor::QueryProcessor::new_with_rc_schemas(
                transaction,
                dummy_schemas,
            );
        })
    });

    // 20. Schema validation cache overhead
    c.bench_function("schema_validation_cache_overhead", |b| {
        b.iter(|| {
            let _cache = tegdb::executor::SchemaValidationCache::new(&test_schema);
        })
    });

    // 21. Primary key lookup plan execution
    let pk_lookup_plan = tegdb::planner::ExecutionPlan::PrimaryKeyLookup {
        table: "test".to_string(),
        pk_values: {
            let mut map = HashMap::new();
            map.insert("id".to_string(), tegdb::parser::SqlValue::Integer(1));
            map
        },
        selected_columns: vec!["id".to_string(), "value".to_string()],
        additional_filter: None,
    };

    c.bench_function("primary_key_lookup_execution", |b| {
        b.iter(|| {
            let mut processor = tegdb::executor::QueryProcessor::new_with_rc_schemas(
                engine.begin_transaction(),
                table_schemas.clone(),
            );
            let _result = processor.execute_plan(pk_lookup_plan.clone()).unwrap();
        })
    });

    // 22. Table scan plan execution
    let table_scan_plan = tegdb::planner::ExecutionPlan::TableScan {
        table: "test".to_string(),
        selected_columns: vec!["id".to_string(), "value".to_string()],
        filter: None,
        limit: None,
    };

    c.bench_function("table_scan_execution", |b| {
        b.iter(|| {
            let mut processor = tegdb::executor::QueryProcessor::new_with_rc_schemas(
                engine.begin_transaction(),
                table_schemas.clone(),
            );
            let _result = processor.execute_plan(table_scan_plan.clone()).unwrap();
        })
    });

    // 23. Result set materialization
    c.bench_function("result_set_materialization", |b| {
        b.iter(|| {
            let result = db.query(sql).unwrap();
            // Force materialization by iterating all rows
            let _rows: Vec<_> = result.rows().iter().collect();
        })
    });

    // 24. Streaming vs materialized results
    c.bench_function("streaming_result_iteration", |b| {
        b.iter(|| {
            let result = db.query(sql).unwrap();
            // Just iterate without collecting
            let mut count = 0;
            for _row in result.rows() {
                count += 1;
            }
            black_box(count);
        })
    });

    // 25. Memory allocation patterns in query processing
    c.bench_function("query_memory_allocation", |b| {
        b.iter(|| {
            // Simulate memory allocation patterns in query processing
            let mut vec = Vec::with_capacity(10);
            for i in 0..10 {
                vec.push(format!("column_{}", i));
            }
            let mut map = HashMap::new();
            for i in 0..5 {
                map.insert(format!("key_{}", i), format!("value_{}", i));
            }
            black_box((vec, map));
        })
    });

    // 26. String operations in query processing
    c.bench_function("query_string_operations", |b| {
        b.iter(|| {
            // Simulate string operations in query processing
            let table_name = "test";
            let column_name = "id";
            let key = format!("{}:{}", table_name, column_name);
            let _bytes = key.as_bytes();
        })
    });

    // 27. HashMap lookups in query processing
    c.bench_function("query_hashmap_lookups", |b| {
        b.iter(|| {
            let mut map = HashMap::new();
            map.insert("id".to_string(), "value".to_string());
            map.insert("name".to_string(), "test".to_string());
            
            for _ in 0..10 {
                let _val1 = map.get("id");
                let _val2 = map.get("name");
                let _val3 = map.get("nonexistent");
            }
        })
    });

    // 28. Vector operations in query processing
    c.bench_function("query_vector_operations", |b| {
        b.iter(|| {
            let mut vec = Vec::new();
            for i in 0..100 {
                vec.push(i);
            }
            let _sum: i32 = vec.iter().sum();
            let _filtered: Vec<_> = vec.iter().filter(|&&x| x > 50).collect();
        })
    });

    // 29. Storage format micro-benchmarks
    let test_data = {
        let mut row = HashMap::new();
        row.insert("id".to_string(), tegdb::parser::SqlValue::Integer(123));
        row.insert("value".to_string(), tegdb::parser::SqlValue::Integer(456));
        row.insert("name".to_string(), tegdb::parser::SqlValue::Text("test".to_string()));
        row
    };
    
    let serialized_data = storage_format
        .serialize_row(&test_data, &test_schema)
        .unwrap();

    // Benchmark parse_header specifically (by calling deserialize_row and measuring overhead)
    c.bench_function("parse_header_only", |b| {
        b.iter(|| {
            // This will call parse_header internally
            let _result = storage_format
                .deserialize_row(black_box(&serialized_data), black_box(&test_schema))
                .unwrap();
        })
    });

    // Benchmark varint decoding (simulate with a simple loop)
    c.bench_function("varint_decode", |b| {
        b.iter(|| {
            // Simulate varint decoding overhead
            let mut result = 0;
            let mut shift = 0;
            for &byte in &[0x89, 0x60] { // 12345 in varint format
                result |= ((byte & 0x7F) as usize) << shift;
                shift += 7;
            }
            black_box(result);
        })
    });

    // Benchmark string cloning
    let test_strings = vec!["id".to_string(), "value".to_string(), "name".to_string()];
    c.bench_function("string_cloning", |b| {
        b.iter(|| {
            for s in &test_strings {
                let _cloned = black_box(s.clone());
            }
        })
    });

    // Benchmark HashMap creation
    c.bench_function("hashmap_creation", |b| {
        b.iter(|| {
            let mut map = HashMap::new();
            map.insert("id".to_string(), tegdb::parser::SqlValue::Integer(123));
            map.insert("value".to_string(), tegdb::parser::SqlValue::Integer(456));
            black_box(map);
        })
    });

    // Benchmark full deserialize_row
    c.bench_function("deserialize_row_full", |b| {
        b.iter(|| {
            let _result = storage_format
                .deserialize_row(black_box(&serialized_data), black_box(&test_schema))
                .unwrap();
        })
    });

    // Benchmark deserialize_columns (should be faster)
    let column_names = vec!["id".to_string(), "value".to_string()];
    c.bench_function("deserialize_columns_partial", |b| {
        b.iter(|| {
            let _result = storage_format
                .deserialize_columns(black_box(&serialized_data), black_box(&test_schema), black_box(&column_names))
                .unwrap();
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
