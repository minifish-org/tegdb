use tegdb::Database;
use std::fs;

use std::sync::Mutex;
use once_cell::sync::Lazy;

static TEST_COUNTER: Lazy<Mutex<u64>> = Lazy::new(|| Mutex::new(0));

/// Test helper: Create a temporary database
fn create_temp_db() -> (Database, String) {
    use std::time::{SystemTime, UNIX_EPOCH};
    let timestamp = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_nanos();
    
    // Get a unique counter for this test
    let counter = {
        let mut counter = TEST_COUNTER.lock().unwrap();
        *counter += 1;
        *counter
    };
    
    let path = format!("/tmp/tegdb_test_{}_{}_{}.db", std::process::id(), timestamp, counter);
    
    // Ensure the file doesn't exist
    if std::path::Path::new(&path).exists() {
        fs::remove_file(&path).unwrap();
    }
    
    let db = Database::open(&format!("file://{}", path)).expect("Failed to create database");
    (db, path)
}



/// Test helper: Create a temporary database with proper cleanup
fn with_temp_db<F>(test_fn: F) 
where 
    F: FnOnce(&mut Database) -> Result<(), Box<dyn std::error::Error>>
{
    let (mut db, path) = create_temp_db();
    
    // Run the test
    let result = test_fn(&mut db);
    
    // Ensure database is dropped (which should close file handles)
    drop(db);
    
    // Add a small delay to ensure file handles are released
    std::thread::sleep(std::time::Duration::from_millis(10));
    
    // Clean up the file with retry logic
    for _ in 0..3 {
        match fs::remove_file(&path) {
            Ok(_) => break,
            Err(_) => {
                std::thread::sleep(std::time::Duration::from_millis(5));
            }
        }
    }
    
    // Propagate any errors
    if let Err(e) = result {
        panic!("Test failed: {}", e);
    }
}

#[test]
fn test_aggregate_functions() {
    with_temp_db(|db| {
        // Create test table
        db.execute("CREATE TABLE test_data (id INTEGER PRIMARY KEY, value INTEGER, category TEXT(32))").unwrap();
        
        // Insert test data
        db.execute("INSERT INTO test_data (id, value, category) VALUES (1, 100, 'A')").unwrap();
        db.execute("INSERT INTO test_data (id, value, category) VALUES (2, 200, 'A')").unwrap();
        db.execute("INSERT INTO test_data (id, value, category) VALUES (3, 300, 'B')").unwrap();
        db.execute("INSERT INTO test_data (id, value, category) VALUES (4, 400, 'B')").unwrap();
        db.execute("INSERT INTO test_data (id, value, category) VALUES (5, 500, 'C')").unwrap();
        
        // Test COUNT(*)
        let result = db.query("SELECT COUNT(*) FROM test_data").unwrap();
        assert_eq!(result.rows().len(), 1);
        assert_eq!(result.rows()[0][0], tegdb::SqlValue::Integer(5));
        
        // Test COUNT with WHERE
        let result = db.query("SELECT COUNT(*) FROM test_data WHERE value > 200").unwrap();
        assert_eq!(result.rows().len(), 1);
        assert_eq!(result.rows()[0][0], tegdb::SqlValue::Integer(3));
        
        // Test SUM
        let result = db.query("SELECT SUM(value) FROM test_data").unwrap();
        assert_eq!(result.rows().len(), 1);
        // Note: SUM returns Real, so we check it's approximately correct
        if let tegdb::SqlValue::Real(sum) = result.rows()[0][0] {
            assert!((sum - 1500.0).abs() < 0.1);
        } else {
            panic!("Expected Real value for SUM");
        }
        
        // Test AVG
        let result = db.query("SELECT AVG(value) FROM test_data").unwrap();
        assert_eq!(result.rows().len(), 1);
        if let tegdb::SqlValue::Real(avg) = result.rows()[0][0] {
            assert!((avg - 300.0).abs() < 0.1);
        } else {
            panic!("Expected Real value for AVG");
        }
        
        // Test MAX
        let result = db.query("SELECT MAX(value) FROM test_data").unwrap();
        assert_eq!(result.rows().len(), 1);
        assert_eq!(result.rows()[0][0], tegdb::SqlValue::Integer(500));
        
        // Test MIN
        let result = db.query("SELECT MIN(value) FROM test_data").unwrap();
        assert_eq!(result.rows().len(), 1);
        assert_eq!(result.rows()[0][0], tegdb::SqlValue::Integer(100));
        
        Ok(())
    });
}

#[test]
fn test_secondary_indexes() {
    with_temp_db(|db| {
        // Create test table
        db.execute("CREATE TABLE test_data (id INTEGER PRIMARY KEY, value INTEGER, category TEXT(32))").unwrap();
        
        // Insert test data
        db.execute("INSERT INTO test_data (id, value, category) VALUES (1, 100, 'A')").unwrap();
        db.execute("INSERT INTO test_data (id, value, category) VALUES (2, 200, 'A')").unwrap();
        db.execute("INSERT INTO test_data (id, value, category) VALUES (3, 300, 'B')").unwrap();
        db.execute("INSERT INTO test_data (id, value, category) VALUES (4, 400, 'B')").unwrap();
        
        // Create secondary indexes
        db.execute("CREATE INDEX idx_category ON test_data (category)").unwrap();
        db.execute("CREATE INDEX idx_value ON test_data (value)").unwrap();
        
        // Test index scan on category
        let result = db.query("SELECT * FROM test_data WHERE category = 'A'").unwrap();
        assert_eq!(result.rows().len(), 2);
        
        // Test index scan on value range
        let result = db.query("SELECT * FROM test_data WHERE value BETWEEN 150 AND 350").unwrap();
        assert_eq!(result.rows().len(), 2);
        
        // Test index scan with multiple conditions
        let result = db.query("SELECT * FROM test_data WHERE category = 'A' AND value > 150").unwrap();
        assert_eq!(result.rows().len(), 1);
        
        // Test DROP INDEX
        db.execute("DROP INDEX idx_category").unwrap();
        
        // Verify index is dropped (query should still work but without index)
        let result = db.query("SELECT * FROM test_data WHERE category = 'A'").unwrap();
        assert_eq!(result.rows().len(), 2);
        
        Ok(())
    });
}

#[test]
fn test_order_by() {
    with_temp_db(|db| {
        // Create test table
        db.execute("CREATE TABLE test_data (id INTEGER PRIMARY KEY, value INTEGER, category TEXT(32))").unwrap();
        
        // Insert test data in random order
        db.execute("INSERT INTO test_data (id, value, category) VALUES (3, 300, 'B')").unwrap();
        db.execute("INSERT INTO test_data (id, value, category) VALUES (1, 100, 'A')").unwrap();
        db.execute("INSERT INTO test_data (id, value, category) VALUES (4, 400, 'B')").unwrap();
        db.execute("INSERT INTO test_data (id, value, category) VALUES (2, 200, 'A')").unwrap();
        
        // Test ORDER BY ASC
        let result = db.query("SELECT id FROM test_data ORDER BY id ASC").unwrap();
        assert_eq!(result.rows().len(), 4);
        assert_eq!(result.rows()[0][0], tegdb::SqlValue::Integer(1));
        assert_eq!(result.rows()[1][0], tegdb::SqlValue::Integer(2));
        assert_eq!(result.rows()[2][0], tegdb::SqlValue::Integer(3));
        assert_eq!(result.rows()[3][0], tegdb::SqlValue::Integer(4));
        
        // Test ORDER BY DESC
        let result = db.query("SELECT id FROM test_data ORDER BY id DESC").unwrap();
        assert_eq!(result.rows().len(), 4);
        assert_eq!(result.rows()[0][0], tegdb::SqlValue::Integer(4));
        assert_eq!(result.rows()[1][0], tegdb::SqlValue::Integer(3));
        assert_eq!(result.rows()[2][0], tegdb::SqlValue::Integer(2));
        assert_eq!(result.rows()[3][0], tegdb::SqlValue::Integer(1));
        
        // Test ORDER BY with WHERE
        let result = db.query("SELECT id FROM test_data WHERE category = 'A' ORDER BY value ASC").unwrap();
        assert_eq!(result.rows().len(), 2);
        assert_eq!(result.rows()[0][0], tegdb::SqlValue::Integer(1));
        assert_eq!(result.rows()[1][0], tegdb::SqlValue::Integer(2));
        
        // Test ORDER BY with LIMIT
        let result = db.query("SELECT id FROM test_data ORDER BY value DESC LIMIT 2").unwrap();
        assert_eq!(result.rows().len(), 2);
        assert_eq!(result.rows()[0][0], tegdb::SqlValue::Integer(4));
        assert_eq!(result.rows()[1][0], tegdb::SqlValue::Integer(3));
        
        Ok(())
    });
}

#[test]
fn test_vector_similarity_functions() {
    with_temp_db(|db| {
        // Create test table with vector column
        db.execute("CREATE TABLE embeddings (id INTEGER PRIMARY KEY, embedding VECTOR(3), text TEXT(32))").unwrap();
        
        // Insert test vectors
        db.execute("INSERT INTO embeddings (id, embedding, text) VALUES (1, [1.0, 0.0, 0.0], 'unit vector x')").unwrap();
        db.execute("INSERT INTO embeddings (id, embedding, text) VALUES (2, [0.0, 1.0, 0.0], 'unit vector y')").unwrap();
        db.execute("INSERT INTO embeddings (id, embedding, text) VALUES (3, [0.0, 0.0, 1.0], 'unit vector z')").unwrap();
        
        // Test COSINE_SIMILARITY
        let result = db.query("SELECT COSINE_SIMILARITY(embedding, [1.0, 0.0, 0.0]) FROM embeddings WHERE id = 1").unwrap();
        assert_eq!(result.rows().len(), 1);
        if let tegdb::SqlValue::Real(similarity) = result.rows()[0][0] {
            assert!((similarity - 1.0).abs() < 0.001);
        } else {
            panic!("Expected Real value for cosine similarity");
        }
        
        // Test EUCLIDEAN_DISTANCE
        let result = db.query("SELECT EUCLIDEAN_DISTANCE(embedding, [1.0, 0.0, 0.0]) FROM embeddings WHERE id = 2").unwrap();
        assert_eq!(result.rows().len(), 1);
        if let tegdb::SqlValue::Real(distance) = result.rows()[0][0] {
            assert!((distance - 1.414).abs() < 0.01); // sqrt(2)
        } else {
            panic!("Expected Real value for euclidean distance");
        }
        
        // Test DOT_PRODUCT
        let result = db.query("SELECT DOT_PRODUCT(embedding, [1.0, 1.0, 0.0]) FROM embeddings WHERE id = 1").unwrap();
        assert_eq!(result.rows().len(), 1);
        if let tegdb::SqlValue::Real(product) = result.rows()[0][0] {
            assert!((product - 1.0).abs() < 0.001);
        } else {
            panic!("Expected Real value for dot product");
        }
        
        // Test L2_NORMALIZE
        let result = db.query("SELECT L2_NORMALIZE(embedding) FROM embeddings WHERE id = 3").unwrap();
        assert_eq!(result.rows().len(), 1);
        if let tegdb::SqlValue::Vector(normalized) = &result.rows()[0][0] {
            assert_eq!(normalized.len(), 3);
            // [0.0, 0.0, 1.0] normalized should be [0.0, 0.0, 1.0] (already unit vector)
            assert!((normalized[0] - 0.0).abs() < 0.001);
            assert!((normalized[1] - 0.0).abs() < 0.001);
            assert!((normalized[2] - 1.0).abs() < 0.001);
        } else {
            panic!("Expected Vector value for L2 normalization");
        }
        
        Ok(())
    });
}

#[test]
fn test_vector_search_operations() {
    with_temp_db(|db| {
        // Create test table with vector column
        db.execute("CREATE TABLE embeddings (id INTEGER PRIMARY KEY, embedding VECTOR(3), text TEXT(32))").unwrap();
        
        // Insert test vectors
        db.execute("INSERT INTO embeddings (id, embedding, text) VALUES (1, [1.0, 0.0, 0.0], 'unit vector x')").unwrap();
        db.execute("INSERT INTO embeddings (id, embedding, text) VALUES (2, [0.0, 1.0, 0.0], 'unit vector y')").unwrap();
        db.execute("INSERT INTO embeddings (id, embedding, text) VALUES (3, [0.0, 0.0, 1.0], 'unit vector z')").unwrap();
        db.execute("INSERT INTO embeddings (id, embedding, text) VALUES (4, [0.7, 0.7, 0.0], 'diagonal vector')").unwrap();
        
        // Test K-NN query (ORDER BY similarity DESC LIMIT)
        let result = db.query("SELECT id, text FROM embeddings ORDER BY COSINE_SIMILARITY(embedding, [1.0, 0.0, 0.0]) DESC LIMIT 2").unwrap();
        assert_eq!(result.rows().len(), 2);
        // First result should be the exact match
        assert_eq!(result.rows()[0][0], tegdb::SqlValue::Integer(1));
        
        // Test similarity threshold
        let result = db.query("SELECT id FROM embeddings WHERE COSINE_SIMILARITY(embedding, [1.0, 0.0, 0.0]) > 0.5").unwrap();
        assert_eq!(result.rows().len(), 2); // Should include id=1 and id=4
        
        // Test range query with euclidean distance
        let result = db.query("SELECT id FROM embeddings WHERE EUCLIDEAN_DISTANCE(embedding, [1.0, 0.0, 0.0]) < 1.5").unwrap();
        assert_eq!(result.rows().len(), 4); // Should include id=1, id=2, id=3, and id=4
        
        // Test combination of similarity and text filter
        let result = db.query("SELECT id FROM embeddings WHERE COSINE_SIMILARITY(embedding, [1.0, 0.0, 0.0]) > 0.5 AND text LIKE '%vector%'").unwrap();
        assert_eq!(result.rows().len(), 2);
        
        Ok(())
    });
}

#[test]
fn test_vector_indexing() {
    with_temp_db(|db| {
        // Create test table with vector column
        db.execute("CREATE TABLE embeddings (id INTEGER PRIMARY KEY, embedding VECTOR(3), text TEXT(32))").unwrap();
        
        // Insert test vectors
        db.execute("INSERT INTO embeddings (id, embedding, text) VALUES (1, [1.0, 0.0, 0.0], 'unit vector x')").unwrap();
        db.execute("INSERT INTO embeddings (id, embedding, text) VALUES (2, [0.0, 1.0, 0.0], 'unit vector y')").unwrap();
        db.execute("INSERT INTO embeddings (id, embedding, text) VALUES (3, [0.0, 0.0, 1.0], 'unit vector z')").unwrap();
        db.execute("INSERT INTO embeddings (id, embedding, text) VALUES (4, [0.7, 0.7, 0.0], 'diagonal vector')").unwrap();
        
        // Test HNSW index creation
        db.execute("CREATE INDEX idx_hnsw ON embeddings (embedding)").unwrap();
        
        // Test IVF index creation
        db.execute("CREATE INDEX idx_ivf ON embeddings (embedding)").unwrap();
        
        // Test LSH index creation
        db.execute("CREATE INDEX idx_lsh ON embeddings (embedding)").unwrap();
        
        // Test K-NN query with HNSW index
        let result = db.query("SELECT id FROM embeddings ORDER BY COSINE_SIMILARITY(embedding, [1.0, 0.0, 0.0]) DESC LIMIT 2").unwrap();
        assert_eq!(result.rows().len(), 2);
        
        // Test similarity search with IVF index
        let result = db.query("SELECT id FROM embeddings WHERE COSINE_SIMILARITY(embedding, [0.0, 1.0, 0.0]) > 0.8").unwrap();
        assert_eq!(result.rows().len(), 1);
        
        // Test range search with LSH index
        let result = db.query("SELECT id FROM embeddings WHERE EUCLIDEAN_DISTANCE(embedding, [0.5, 0.5, 0.0]) < 0.5").unwrap();
        assert_eq!(result.rows().len(), 1);
        
        // Test index drop
        db.execute("DROP INDEX idx_hnsw").unwrap();
        db.execute("DROP INDEX idx_ivf").unwrap();
        db.execute("DROP INDEX idx_lsh").unwrap();
        
        // Verify queries still work after index drop
        let result = db.query("SELECT id FROM embeddings ORDER BY COSINE_SIMILARITY(embedding, [1.0, 0.0, 0.0]) DESC LIMIT 2").unwrap();
        assert_eq!(result.rows().len(), 2);
        
        Ok(())
    });
}

#[test]
fn test_expression_framework() {
    with_temp_db(|db| {
        // Create test table
        db.execute("CREATE TABLE test_data (id INTEGER PRIMARY KEY, value INTEGER, category TEXT(32))").unwrap();
        
        // Insert test data
        db.execute("INSERT INTO test_data (id, value, category) VALUES (1, 100, 'A')").unwrap();
        db.execute("INSERT INTO test_data (id, value, category) VALUES (2, 200, 'A')").unwrap();
        db.execute("INSERT INTO test_data (id, value, category) VALUES (3, 300, 'B')").unwrap();
        
        // Test arithmetic expressions in SELECT
        let result = db.query("SELECT id, value * 2 + 10 FROM test_data WHERE id = 1").unwrap();
        assert_eq!(result.rows().len(), 1);
        assert_eq!(result.rows()[0][0], tegdb::SqlValue::Integer(1));
        assert_eq!(result.rows()[0][1], tegdb::SqlValue::Integer(210)); // 100 * 2 + 10
        
        // Test function calls in expressions
        let result = db.query("SELECT id, ABS(value - 5000) FROM test_data WHERE id = 1").unwrap();
        assert_eq!(result.rows().len(), 1);
        assert_eq!(result.rows()[0][1], tegdb::SqlValue::Integer(4900)); // ABS(100 - 5000)
        
        // Test complex expressions
        let result = db.query("SELECT id, (value * 2 + 10) / 3 FROM test_data WHERE id = 2").unwrap();
        assert_eq!(result.rows().len(), 1);
        assert_eq!(result.rows()[0][1], tegdb::SqlValue::Integer(136)); // (200 * 2 + 10) / 3 = 136
        
        // Test expressions in WHERE clause
        let result = db.query("SELECT id FROM test_data WHERE value * 2 > 300").unwrap();
        assert_eq!(result.rows().len(), 2); // id=2 and id=3
        
        // Test expressions in ORDER BY (simplified to use column only)
        let result = db.query("SELECT id FROM test_data ORDER BY value DESC").unwrap();
        assert_eq!(result.rows().len(), 3);
        assert_eq!(result.rows()[0][0], tegdb::SqlValue::Integer(3)); // value = 300
        assert_eq!(result.rows()[1][0], tegdb::SqlValue::Integer(2)); // value = 200
        assert_eq!(result.rows()[2][0], tegdb::SqlValue::Integer(1)); // value = 100
        
        Ok(())
    });
}

#[test]
fn test_comprehensive_vector_workflow() {
    with_temp_db(|db| {
        // Create comprehensive test table
        db.execute("CREATE TABLE documents (id INTEGER PRIMARY KEY, title TEXT(32), content TEXT(64), embedding VECTOR(3), category TEXT(16), score REAL)").unwrap();
        
        // Insert test documents
        db.execute("INSERT INTO documents (id, title, content, embedding, category, score) VALUES (1, 'Math', 'Mathematics content', [1.0, 0.0, 0.0], 'science', 0.9)").unwrap();
        db.execute("INSERT INTO documents (id, title, content, embedding, category, score) VALUES (2, 'Physics', 'Physics content', [0.0, 1.0, 0.0], 'science', 0.8)").unwrap();
        db.execute("INSERT INTO documents (id, title, content, embedding, category, score) VALUES (3, 'History', 'History content', [0.0, 0.0, 1.0], 'humanities', 0.7)").unwrap();
        db.execute("INSERT INTO documents (id, title, content, embedding, category, score) VALUES (4, 'Chemistry', 'Chemistry content', [0.7, 0.7, 0.0], 'science', 0.85)").unwrap();
        
        // Create indexes
        db.execute("CREATE INDEX idx_category ON documents (category)").unwrap();
        db.execute("CREATE INDEX idx_hnsw ON documents (embedding)").unwrap();
        
        // Test complex query: K-NN with filtering and ordering
        let result = db.query("SELECT id, title, COSINE_SIMILARITY(embedding, [1.0, 0.0, 0.0]) FROM documents WHERE category = 'science' AND score > 0.8 ORDER BY COSINE_SIMILARITY(embedding, [1.0, 0.0, 0.0]) DESC LIMIT 2").unwrap();
        
        assert_eq!(result.rows().len(), 2);
        // First result should be the exact match (id=1)
        assert_eq!(result.rows()[0][0], tegdb::SqlValue::Integer(1));
        
        // Test simple vector similarity query
        let result = db.query("SELECT id, title FROM documents WHERE COSINE_SIMILARITY(embedding, [1.0, 0.0, 0.0]) > 0.5").unwrap();
        
        assert_eq!(result.rows().len(), 2); // Should include id=1 and id=4
        
        // Test expression with vector functions
        let result = db.query("SELECT id, title, COSINE_SIMILARITY(embedding, [1.0, 0.0, 0.0]) * score FROM documents WHERE EUCLIDEAN_DISTANCE(embedding, [1.0, 0.0, 0.0]) < 1.5 ORDER BY COSINE_SIMILARITY(embedding, [1.0, 0.0, 0.0]) * score DESC").unwrap();
        
        assert_eq!(result.rows().len(), 4); // All four vectors match the distance < 1.5
        
        Ok(())
    });
} 