use criterion::{criterion_group, criterion_main, Criterion};
use std::env;
use std::fs;
use std::hint::black_box;
use std::path::PathBuf;
use tegdb::Database;

/// Creates a unique temporary file path for benchmarks
fn temp_db_path(prefix: &str) -> PathBuf {
    let mut path = env::temp_dir();
    path.push(format!("tegdb_vector_bench_{}_{}", prefix, std::process::id()));
    path
}

/// Generate random vector of specified dimension
fn random_vector(dimension: usize) -> Vec<f64> {
    use rand::Rng;
    let mut rng = rand::thread_rng();
    (0..dimension).map(|_| rng.gen_range(-1.0..1.0)).collect()
}

/// Generate normalized random vector
fn random_normalized_vector(dimension: usize) -> Vec<f64> {
    let mut vec = random_vector(dimension);
    let norm: f64 = vec.iter().map(|x| x * x).sum::<f64>().sqrt();
    if norm > 0.0 {
        for x in &mut vec {
            *x /= norm;
        }
    }
    vec
}

/// Setup vector embeddings table with test data
fn setup_vector_table(db: &mut Database, table_name: &str, num_vectors: usize, dimension: usize) -> Result<(), Box<dyn std::error::Error>> {
    // Create table with vector column
    db.execute(&format!(
        "CREATE TABLE {} (id INTEGER PRIMARY KEY, text TEXT(50), embedding VECTOR({}))",
        table_name, dimension
    ))?;

    // Insert test vectors
    for i in 0..num_vectors {
        let vector = random_normalized_vector(dimension);
        let vector_str = format!("[{:.6}, {:.6}, {:.6}]", vector[0], vector[1], vector[2]);
        db.execute(&format!(
            "INSERT INTO {} (id, text, embedding) VALUES ({}, 'text_{}', {})",
            table_name, i, i, vector_str
        ))?;
    }

    Ok(())
}

/// Setup table with secondary index for benchmarking
fn setup_indexed_table(db: &mut Database, table_name: &str, num_rows: usize) -> Result<(), Box<dyn std::error::Error>> {
    // Create table
    db.execute(&format!(
        "CREATE TABLE {} (id INTEGER PRIMARY KEY, name TEXT(50), value INTEGER, category TEXT(20))",
        table_name
    ))?;

    // Insert test data
    for i in 0..num_rows {
        let category = if i % 3 == 0 { "A" } else if i % 3 == 1 { "B" } else { "C" };
        db.execute(&format!(
            "INSERT INTO {} (id, name, value, category) VALUES ({}, 'item_{}', {}, '{}')",
            table_name, i, i, i * 10, category
        ))?;
    }

    // Create secondary index
    db.execute(&format!("CREATE INDEX idx_{}_category ON {} (category)", table_name, table_name))?;
    db.execute(&format!("CREATE INDEX idx_{}_value ON {} (value)", table_name, table_name))?;

    Ok(())
}

fn vector_similarity_functions_benchmark(c: &mut Criterion) {
    let path = temp_db_path("vector_similarity");
    if path.exists() {
        fs::remove_file(&path).expect("Failed to remove existing test file");
    }
    
    let mut db = Database::open(&path.to_string_lossy()).expect("Failed to create database");
    setup_vector_table(&mut db, "embeddings", 1000, 3).expect("Failed to setup vector table");

    let mut group = c.benchmark_group("Vector Similarity Functions");

    // Test COSINE_SIMILARITY function
    group.bench_function("COSINE_SIMILARITY", |b| {
        let query_vector = "[0.8, 0.2, 0.0]";
        b.iter(|| {
            let result = db.query(&format!(
                "SELECT id, COSINE_SIMILARITY(embedding, {}) FROM embeddings WHERE id = 1",
                query_vector
            )).unwrap();
            black_box(result);
        });
    });

    // Test EUCLIDEAN_DISTANCE function
    group.bench_function("EUCLIDEAN_DISTANCE", |b| {
        let query_vector = "[0.0, 1.0, 0.0]";
        b.iter(|| {
            let result = db.query(&format!(
                "SELECT id, EUCLIDEAN_DISTANCE(embedding, {}) FROM embeddings WHERE id = 2",
                query_vector
            )).unwrap();
            black_box(result);
        });
    });

    // Test DOT_PRODUCT function
    group.bench_function("DOT_PRODUCT", |b| {
        let query_vector = "[1.0, 0.0, 0.0]";
        b.iter(|| {
            let result = db.query(&format!(
                "SELECT id, DOT_PRODUCT(embedding, {}) FROM embeddings WHERE id = 1",
                query_vector
            )).unwrap();
            black_box(result);
        });
    });

    // Test L2_NORMALIZE function
    group.bench_function("L2_NORMALIZE", |b| {
        b.iter(|| {
            let result = db.query("SELECT id, L2_NORMALIZE(embedding) FROM embeddings WHERE id = 4").unwrap();
            black_box(result);
        });
    });

    group.finish();

    // Clean up
    drop(db);
    let _ = fs::remove_file(&path);
}

fn vector_search_operations_benchmark(c: &mut Criterion) {
    let path = temp_db_path("vector_search");
    if path.exists() {
        fs::remove_file(&path).expect("Failed to remove existing test file");
    }
    
    let mut db = Database::open(&path.to_string_lossy()).expect("Failed to create database");
    setup_vector_table(&mut db, "embeddings", 1000, 3).expect("Failed to setup vector table");

    let mut group = c.benchmark_group("Vector Search Operations");

    // Test K-NN query with cosine similarity
    group.bench_function("K-NN Cosine Similarity", |b| {
        let query_vector = "[0.8, 0.2, 0.0]";
        b.iter(|| {
            let result = db.query(&format!(
                "SELECT id, text, COSINE_SIMILARITY(embedding, {}) FROM embeddings ORDER BY COSINE_SIMILARITY(embedding, {}) DESC LIMIT 10",
                query_vector, query_vector
            )).unwrap();
            black_box(result);
        });
    });

    // Test similarity threshold in WHERE clause
    group.bench_function("Similarity Threshold", |b| {
        let query_vector = "[0.8, 0.2, 0.0]";
        b.iter(|| {
            let result = db.query(&format!(
                "SELECT id, text FROM embeddings WHERE COSINE_SIMILARITY(embedding, {}) > 0.5",
                query_vector
            )).unwrap();
            black_box(result);
        });
    });

    // Test range query with euclidean distance
    group.bench_function("Euclidean Distance Range", |b| {
        let query_vector = "[0.0, 1.0, 0.0]";
        b.iter(|| {
            let result = db.query(&format!(
                "SELECT id, text FROM embeddings WHERE EUCLIDEAN_DISTANCE(embedding, {}) < 0.5",
                query_vector
            )).unwrap();
            black_box(result);
        });
    });

    group.finish();

    // Clean up
    drop(db);
    let _ = fs::remove_file(&path);
}

fn aggregate_functions_benchmark(c: &mut Criterion) {
    let path = temp_db_path("aggregate");
    if path.exists() {
        fs::remove_file(&path).expect("Failed to remove existing test file");
    }
    
    let mut db = Database::open(&path.to_string_lossy()).expect("Failed to create database");
    setup_indexed_table(&mut db, "test_data", 10000).expect("Failed to setup indexed table");

    let mut group = c.benchmark_group("Aggregate Functions");

    // Test COUNT aggregate function
    group.bench_function("COUNT All Rows", |b| {
        b.iter(|| {
            let result = db.query("SELECT COUNT(*) FROM test_data").unwrap();
            black_box(result);
        });
    });

    group.bench_function("COUNT With Filter", |b| {
        b.iter(|| {
            let result = db.query("SELECT COUNT(*) FROM test_data WHERE value > 5000").unwrap();
            black_box(result);
        });
    });



    // Test SUM aggregate function
    group.bench_function("SUM All Values", |b| {
        b.iter(|| {
            let result = db.query("SELECT SUM(value) FROM test_data").unwrap();
            black_box(result);
        });
    });

    group.bench_function("SUM With Filter", |b| {
        b.iter(|| {
            let result = db.query("SELECT SUM(value) FROM test_data WHERE value > 5000").unwrap();
            black_box(result);
        });
    });



    group.finish();

    // Clean up
    drop(db);
    let _ = fs::remove_file(&path);
}

fn secondary_indexes_benchmark(c: &mut Criterion) {
    let path = temp_db_path("secondary_indexes");
    if path.exists() {
        fs::remove_file(&path).expect("Failed to remove existing test file");
    }
    
    let mut db = Database::open(&path.to_string_lossy()).expect("Failed to create database");
    setup_indexed_table(&mut db, "test_data", 10000).expect("Failed to setup indexed table");

    let mut group = c.benchmark_group("Secondary Indexes");

    // Test index scan on category
    group.bench_function("Index Scan Category A", |b| {
        b.iter(|| {
            let result = db.query("SELECT * FROM test_data WHERE category = 'A'").unwrap();
            black_box(result);
        });
    });

    group.bench_function("Index Scan Category B", |b| {
        b.iter(|| {
            let result = db.query("SELECT * FROM test_data WHERE category = 'B'").unwrap();
            black_box(result);
        });
    });

    // Test index scan on value range
    group.bench_function("Index Scan Value Range", |b| {
        b.iter(|| {
            let result = db.query("SELECT * FROM test_data WHERE value BETWEEN 1000 AND 2000").unwrap();
            black_box(result);
        });
    });

    // Test index scan with multiple conditions
    group.bench_function("Index Scan Multi Condition", |b| {
        b.iter(|| {
            let result = db.query("SELECT * FROM test_data WHERE category = 'A' AND value > 5000").unwrap();
            black_box(result);
        });
    });

    // Compare with table scan (no index usage)
    group.bench_function("Table Scan No Index", |b| {
        b.iter(|| {
            let result = db.query("SELECT * FROM test_data WHERE name LIKE '%item%'").unwrap();
            black_box(result);
        });
    });

    group.finish();

    // Clean up
    drop(db);
    let _ = fs::remove_file(&path);
}

fn order_by_benchmark(c: &mut Criterion) {
    let path = temp_db_path("order_by");
    if path.exists() {
        fs::remove_file(&path).expect("Failed to remove existing test file");
    }
    
    let mut db = Database::open(&path.to_string_lossy()).expect("Failed to create database");
    setup_indexed_table(&mut db, "test_data", 10000).expect("Failed to setup indexed table");
    setup_vector_table(&mut db, "embeddings", 1000, 3).expect("Failed to setup vector table");

    let mut group = c.benchmark_group("ORDER BY Operations");

    // Test ORDER BY on indexed column
    group.bench_function("ORDER BY Indexed Column ASC", |b| {
        b.iter(|| {
            let result = db.query("SELECT * FROM test_data ORDER BY value ASC LIMIT 100").unwrap();
            black_box(result);
        });
    });

    group.bench_function("ORDER BY Indexed Column DESC", |b| {
        b.iter(|| {
            let result = db.query("SELECT * FROM test_data ORDER BY value DESC LIMIT 100").unwrap();
            black_box(result);
        });
    });

    // Test ORDER BY on non-indexed column
    group.bench_function("ORDER BY Non-Indexed Column ASC", |b| {
        b.iter(|| {
            let result = db.query("SELECT * FROM test_data ORDER BY name ASC LIMIT 100").unwrap();
            black_box(result);
        });
    });

    group.bench_function("ORDER BY Non-Indexed Column DESC", |b| {
        b.iter(|| {
            let result = db.query("SELECT * FROM test_data ORDER BY name DESC LIMIT 100").unwrap();
            black_box(result);
        });
    });

    // Test ORDER BY with WHERE clause
    group.bench_function("ORDER BY With Filter", |b| {
        b.iter(|| {
            let result = db.query("SELECT * FROM test_data WHERE category = 'A' ORDER BY value DESC LIMIT 50").unwrap();
            black_box(result);
        });
    });

    // Test ORDER BY with vector similarity
    group.bench_function("ORDER BY Vector Similarity", |b| {
        let query_vector = "[0.8, 0.2, 0.0]";
        b.iter(|| {
            let result = db.query(&format!(
                "SELECT id, text, COSINE_SIMILARITY(embedding, {}) FROM embeddings ORDER BY COSINE_SIMILARITY(embedding, {}) DESC LIMIT 20",
                query_vector, query_vector
            )).unwrap();
            black_box(result);
        });
    });

    group.finish();

    // Clean up
    drop(db);
    let _ = fs::remove_file(&path);
}

fn vector_indexing_benchmark(c: &mut Criterion) {
    let path = temp_db_path("vector_indexing");
    if path.exists() {
        fs::remove_file(&path).expect("Failed to remove existing test file");
    }
    
    let mut db = Database::open(&path.to_string_lossy()).expect("Failed to create database");
    setup_vector_table(&mut db, "embeddings", 1000, 3).expect("Failed to setup vector table");

    let mut group = c.benchmark_group("Vector Indexing");

    // Test HNSW index performance (if implemented)
    group.bench_function("HNSW Index Search", |b| {
        let query_vector = "[0.8, 0.2, 0.0]";
        b.iter(|| {
            let result = db.query(&format!(
                "SELECT id, text, COSINE_SIMILARITY(embedding, {}) FROM embeddings ORDER BY COSINE_SIMILARITY(embedding, {}) DESC LIMIT 10",
                query_vector, query_vector
            )).unwrap();
            black_box(result);
        });
    });

    // Test IVF index performance (if implemented)
    group.bench_function("IVF Index Search", |b| {
        let query_vector = "[0.0, 1.0, 0.0]";
        b.iter(|| {
            let result = db.query(&format!(
                "SELECT id, text, EUCLIDEAN_DISTANCE(embedding, {}) FROM embeddings ORDER BY EUCLIDEAN_DISTANCE(embedding, {}) ASC LIMIT 10",
                query_vector, query_vector
            )).unwrap();
            black_box(result);
        });
    });

    // Test LSH index performance (if implemented)
    group.bench_function("LSH Index Search", |b| {
        let query_vector = "[0.5, 0.5, 0.0]";
        b.iter(|| {
            let result = db.query(&format!(
                "SELECT id, text, DOT_PRODUCT(embedding, {}) FROM embeddings ORDER BY DOT_PRODUCT(embedding, {}) DESC LIMIT 10",
                query_vector, query_vector
            )).unwrap();
            black_box(result);
        });
    });

    // Test without index (baseline)
    group.bench_function("No Index Baseline", |b| {
        b.iter(|| {
            let result = db.query("SELECT id, text FROM embeddings WHERE id < 100").unwrap();
            black_box(result);
        });
    });

    group.finish();

    // Clean up
    drop(db);
    let _ = fs::remove_file(&path);
}

fn expression_framework_benchmark(c: &mut Criterion) {
    let path = temp_db_path("expression_framework");
    if path.exists() {
        fs::remove_file(&path).expect("Failed to remove existing test file");
    }
    
    let mut db = Database::open(&path.to_string_lossy()).expect("Failed to create database");
    setup_indexed_table(&mut db, "test_data", 10000).expect("Failed to setup indexed table");

    let mut group = c.benchmark_group("Expression Framework");

    // Test arithmetic expressions
    group.bench_function("Arithmetic Expression", |b| {
        b.iter(|| {
            let result = db.query("SELECT id, value, value * 2 + 10 FROM test_data WHERE id < 100").unwrap();
            black_box(result);
        });
    });

    // Test function calls in expressions
    group.bench_function("Function in Expression", |b| {
        b.iter(|| {
            let result = db.query("SELECT id, value, ABS(value - 5000) FROM test_data WHERE id < 100").unwrap();
            black_box(result);
        });
    });

    // Test complex expressions with multiple operations
    group.bench_function("Complex Expression", |b| {
        b.iter(|| {
            let result = db.query("SELECT id, value, (value * 2 + 10) / 3 FROM test_data WHERE id < 100").unwrap();
            black_box(result);
        });
    });

    // Test expressions in WHERE clause
    group.bench_function("Expression in WHERE", |b| {
        b.iter(|| {
            let result = db.query("SELECT * FROM test_data WHERE value * 2 > 10000").unwrap();
            black_box(result);
        });
    });

    // Test expressions in ORDER BY
    group.bench_function("Expression in ORDER BY", |b| {
        b.iter(|| {
            let result = db.query("SELECT * FROM test_data ORDER BY value * 2 DESC LIMIT 100").unwrap();
            black_box(result);
        });
    });

    group.finish();

    // Clean up
    drop(db);
    let _ = fs::remove_file(&path);
}

fn comprehensive_vector_benchmark(c: &mut Criterion) {
    let path = temp_db_path("comprehensive_vector");
    if path.exists() {
        fs::remove_file(&path).expect("Failed to remove existing test file");
    }
    
    let mut db = Database::open(&path.to_string_lossy()).expect("Failed to create database");
    
    // Setup comprehensive test data
    setup_vector_table(&mut db, "embeddings", 5000, 3).expect("Failed to setup vector table");

    let mut group = c.benchmark_group("Comprehensive Vector Operations");

    // Test complex vector search with filtering
    group.bench_function("Vector Search with Filter", |b| {
        let query_vector = "[0.8, 0.2, 0.0]";
        b.iter(|| {
            let result = db.query(&format!(
                "SELECT id, text, COSINE_SIMILARITY(embedding, {}) 
                 FROM embeddings 
                 WHERE id < 100 
                 ORDER BY COSINE_SIMILARITY(embedding, {}) DESC 
                 LIMIT 20",
                query_vector, query_vector
            )).unwrap();
            black_box(result);
        });
    });



    // Test multiple vector similarity functions
    group.bench_function("Multiple Vector Functions", |b| {
        let query_vector = "[0.5, 0.5, 0.0]";
        b.iter(|| {
            let result = db.query(&format!(
                "SELECT id, text, 
                        COSINE_SIMILARITY(embedding, {}),
                        EUCLIDEAN_DISTANCE(embedding, {}),
                        DOT_PRODUCT(embedding, {})
                 FROM embeddings 
                 WHERE id < 100",
                query_vector, query_vector, query_vector
            )).unwrap();
            black_box(result);
        });
    });

    group.finish();

    // Clean up
    drop(db);
    let _ = fs::remove_file(&path);
}

criterion_group!(
    benches,
    vector_similarity_functions_benchmark,
    vector_search_operations_benchmark,
    aggregate_functions_benchmark,
    secondary_indexes_benchmark,
    order_by_benchmark,
    vector_indexing_benchmark,
    expression_framework_benchmark,
    comprehensive_vector_benchmark
);

criterion_main!(benches); 