#![allow(clippy::uninlined_format_args)]

mod test_helpers {
    include!("../common/test_helpers.rs");
}
use test_helpers::run_with_both_backends;

use tegdb::{Database, Expression, Result, SqlValue};

#[test]
fn test_commit_marker_and_crash_recovery() -> Result<()> {
    run_with_both_backends("test_commit_marker_and_crash_recovery", |db_path| {
        let mut db = Database::open(db_path)?;

        // Create a test table
        db.execute("CREATE TABLE test_data (key TEXT(32) PRIMARY KEY, value TEXT(32))")?;

        // Begin a transaction and commit it
        {
            let mut tx = db.begin_transaction()?;
            tx.execute("INSERT INTO test_data (key, value) VALUES ('key1', 'value1')")?;
            tx.execute("INSERT INTO test_data (key, value) VALUES ('key2', 'value2')")?;
            tx.commit()?;
        }

        // Begin another transaction but don't commit (simulate crash)
        {
            let mut tx = db.begin_transaction()?;
            tx.execute("INSERT INTO test_data (key, value) VALUES ('key3', 'value3')")?;
            // Don't commit - this should be rolled back on recovery
        }

        // Drop the first database instance to release the file lock
        drop(db);

        // Reopen the database to simulate crash recovery
        let mut db2 = Database::open(db_path)?;

        // Check that committed data is still there
        let result1 = db2.query("SELECT value FROM test_data WHERE key = 'key1'")?;
        assert_eq!(result1.rows().len(), 1);
        assert_eq!(result1.rows()[0][0], SqlValue::Text("value1".to_string()));

        let result2 = db2.query("SELECT value FROM test_data WHERE key = 'key2'")?;
        assert_eq!(result2.rows().len(), 1);
        assert_eq!(result2.rows()[0][0], SqlValue::Text("value2".to_string()));

        // Check that uncommitted data was rolled back
        let result3 = db2.query("SELECT value FROM test_data WHERE key = 'key3'")?;
        assert_eq!(result3.rows().len(), 0); // Should be empty

        Ok(())
    })
}

#[test]
fn test_multiple_transactions_with_commit_markers() -> Result<()> {
    run_with_both_backends(
        "test_multiple_transactions_with_commit_markers",
        |db_path| {
            let mut db = Database::open(db_path)?;

            // Create a test table
            db.execute("CREATE TABLE test_data (key TEXT(32) PRIMARY KEY, value TEXT(32))")?;

            // Transaction 1: committed
            {
                let mut tx = db.begin_transaction()?;
                tx.execute("INSERT INTO test_data (key, value) VALUES ('tx1_key', 'tx1_value')")?;
                tx.commit()?;
            }

            // Transaction 2: committed
            {
                let mut tx = db.begin_transaction()?;
                tx.execute("INSERT INTO test_data (key, value) VALUES ('tx2_key', 'tx2_value')")?;
                tx.commit()?;
            }

            // Verify that both transactions were committed by checking their data
            let result1 = db.query("SELECT value FROM test_data WHERE key = 'tx1_key'")?;
            assert_eq!(result1.rows().len(), 1);
            assert_eq!(
                result1.rows()[0][0],
                SqlValue::Text("tx1_value".to_string())
            );

            let result2 = db.query("SELECT value FROM test_data WHERE key = 'tx2_key'")?;
            assert_eq!(result2.rows().len(), 1);
            assert_eq!(
                result2.rows()[0][0],
                SqlValue::Text("tx2_value".to_string())
            );

            Ok(())
        },
    )
}

#[test]
fn test_create_and_drop_index() {
    use tegdb::Database;
    let db_path = "file:///tmp/test_create_and_drop_index.teg";
    let _ = std::fs::remove_file("/tmp/test_create_and_drop_index.teg");
    let mut db = Database::open(db_path).unwrap();

    db.execute("CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT(50))")
        .unwrap();
    db.execute("CREATE INDEX idx_name ON users(name)").unwrap();
    // Duplicate index name should fail
    assert!(db.execute("CREATE INDEX idx_name ON users(name)").is_err());
    // Non-existent table should fail
    assert!(db.execute("CREATE INDEX idx2 ON no_table(name)").is_err());
    // Non-existent column should fail
    assert!(db.execute("CREATE INDEX idx3 ON users(no_col)").is_err());
    // Drop index
    db.execute("DROP INDEX idx_name").unwrap();
    // Drop non-existent index should fail
    assert!(db.execute("DROP INDEX idx_name").is_err());
    // Index should persist after reopen
    db.execute("CREATE INDEX idx_persist ON users(name)")
        .unwrap();
    drop(db);
    let mut db = Database::open(db_path).unwrap();
    // Drop after reload
    db.execute("DROP INDEX idx_persist").unwrap();
    let _ = std::fs::remove_file("/tmp/test_create_and_drop_index.teg");
}

#[test]
fn test_index_scan_usage() {
    use tegdb::Database;
    let db_path = "file:///tmp/test_index_scan_usage.teg";
    let _ = std::fs::remove_file("/tmp/test_index_scan_usage.teg");
    let mut db = Database::open(db_path).unwrap();

    // Create table and index
    println!("Creating table and index...");
    db.execute("CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT(50), email TEXT(100))")
        .unwrap();
    db.execute("CREATE INDEX idx_name ON users(name)").unwrap();
    println!("Table and index created successfully");

    // Insert test data
    println!("Inserting test data...");
    db.execute("INSERT INTO users (id, name, email) VALUES (1, 'alice', 'alice@example.com')")
        .unwrap();
    db.execute("INSERT INTO users (id, name, email) VALUES (2, 'bob', 'bob@example.com')")
        .unwrap();
    db.execute("INSERT INTO users (id, name, email) VALUES (3, 'alice', 'alice2@example.com')")
        .unwrap();
    println!("Test data inserted successfully");

    // Query that should use the index
    println!("Executing query: SELECT * FROM users WHERE name = 'alice'");
    let result = db
        .query("SELECT * FROM users WHERE name = 'alice'")
        .unwrap();
    println!("Query returned {} rows", result.rows().len());
    assert_eq!(result.rows().len(), 2); // Should find both alice entries

    // Query that should not use the index (no WHERE clause)
    let result = db.query("SELECT * FROM users").unwrap();
    assert_eq!(result.rows().len(), 3); // Should find all entries

    // Query on non-indexed column should not use index
    let result = db
        .query("SELECT * FROM users WHERE email = 'bob@example.com'")
        .unwrap();
    assert_eq!(result.rows().len(), 1); // Should find bob

    let _ = std::fs::remove_file("/tmp/test_index_scan_usage.teg");
}

#[test]
fn test_basic_table_operations() {
    use tegdb::Database;
    let db_path = "file:///tmp/test_basic_table_operations.teg";
    let _ = std::fs::remove_file("/tmp/test_basic_table_operations.teg");
    let mut db = Database::open(db_path).unwrap();

    // Create table
    db.execute("CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT(50), email TEXT(100))")
        .unwrap();

    // Insert test data
    db.execute("INSERT INTO users (id, name, email) VALUES (1, 'alice', 'alice@example.com')")
        .unwrap();
    db.execute("INSERT INTO users (id, name, email) VALUES (2, 'bob', 'bob@example.com')")
        .unwrap();

    // Query without WHERE clause
    let result = db.query("SELECT * FROM users").unwrap();
    assert_eq!(result.rows().len(), 2);

    // Query with WHERE clause
    let result = db
        .query("SELECT * FROM users WHERE name = 'alice'")
        .unwrap();
    assert_eq!(result.rows().len(), 1);

    let _ = std::fs::remove_file("/tmp/test_basic_table_operations.teg");
}

#[test]
fn test_integer_only_table() {
    use tegdb::Database;
    let db_path = "file:///tmp/test_integer_only_table.teg";
    let _ = std::fs::remove_file("/tmp/test_integer_only_table.teg");
    let mut db = Database::open(db_path).unwrap();

    // Create table with only INTEGER columns
    db.execute("CREATE TABLE numbers (id INTEGER PRIMARY KEY, value INTEGER)")
        .unwrap();

    // Insert test data
    db.execute("INSERT INTO numbers (id, value) VALUES (1, 10)")
        .unwrap();
    db.execute("INSERT INTO numbers (id, value) VALUES (2, 20)")
        .unwrap();

    // Query without WHERE clause
    let result = db.query("SELECT * FROM numbers").unwrap();
    assert_eq!(result.rows().len(), 2);

    // Query with WHERE clause
    let result = db.query("SELECT * FROM numbers WHERE value = 10").unwrap();
    assert_eq!(result.rows().len(), 1);

    let _ = std::fs::remove_file("/tmp/test_integer_only_table.teg");
}

#[test]
fn test_order_by_functionality() {
    use tegdb::Database;
    let db_path = "file:///tmp/test_order_by_functionality.teg";
    let _ = std::fs::remove_file("/tmp/test_order_by_functionality.teg");
    let mut db = Database::open(db_path).unwrap();

    // Create table
    db.execute("CREATE TABLE numbers (id INTEGER PRIMARY KEY, value INTEGER, name TEXT(50))")
        .unwrap();

    // Insert test data
    db.execute("INSERT INTO numbers (id, value, name) VALUES (1, 30, 'c')")
        .unwrap();
    db.execute("INSERT INTO numbers (id, value, name) VALUES (2, 10, 'a')")
        .unwrap();
    db.execute("INSERT INTO numbers (id, value, name) VALUES (3, 20, 'b')")
        .unwrap();

    // Test ORDER BY ascending
    let result = db
        .query("SELECT * FROM numbers ORDER BY value ASC")
        .unwrap();
    let rows = result.rows();
    assert_eq!(rows.len(), 3);
    // Should be ordered by value: 10, 20, 30
    assert_eq!(rows[0][1], tegdb::parser::SqlValue::Integer(10));
    assert_eq!(rows[1][1], tegdb::parser::SqlValue::Integer(20));
    assert_eq!(rows[2][1], tegdb::parser::SqlValue::Integer(30));

    // Test ORDER BY descending
    let result = db
        .query("SELECT * FROM numbers ORDER BY value DESC")
        .unwrap();
    let rows = result.rows();
    assert_eq!(rows.len(), 3);
    // Should be ordered by value: 30, 20, 10
    assert_eq!(rows[0][1], tegdb::parser::SqlValue::Integer(30));
    assert_eq!(rows[1][1], tegdb::parser::SqlValue::Integer(20));
    assert_eq!(rows[2][1], tegdb::parser::SqlValue::Integer(10));

    // Test ORDER BY text column
    let result = db.query("SELECT * FROM numbers ORDER BY name ASC").unwrap();
    let rows = result.rows();
    assert_eq!(rows.len(), 3);
    // Should be ordered by name: 'a', 'b', 'c'
    assert_eq!(rows[0][2], tegdb::parser::SqlValue::Text("a".to_string()));
    assert_eq!(rows[1][2], tegdb::parser::SqlValue::Text("b".to_string()));
    assert_eq!(rows[2][2], tegdb::parser::SqlValue::Text("c".to_string()));

    let _ = std::fs::remove_file("/tmp/test_order_by_functionality.teg");
}

#[test]
fn test_vector_basic() {
    use tegdb::Database;
    let db_path = "file:///tmp/test_vector_basic.teg";
    let _ = std::fs::remove_file("/tmp/test_vector_basic.teg");
    let mut db = Database::open(db_path).unwrap();

    // Test 1: Create table with vector column
    println!("Creating table...");
    let result = db.execute("CREATE TABLE vectors (id INTEGER PRIMARY KEY, embedding VECTOR(3))");
    match result {
        Ok(_) => println!("Table created successfully"),
        Err(e) => {
            println!("Failed to create table: {:?}", e);
            return;
        }
    }

    // Test 2: Insert vector data
    println!("Inserting data...");
    let result = db.execute("INSERT INTO vectors (id, embedding) VALUES (1, [1.0, 0.0, 0.0])");
    match result {
        Ok(_) => println!("Data inserted successfully"),
        Err(e) => {
            println!("Failed to insert data: {:?}", e);
            return;
        }
    }

    // Test 3: Query the data
    println!("Querying data...");
    let result = db.query("SELECT * FROM vectors WHERE id = 1");
    match result {
        Ok(result) => {
            println!("Query successful, got {} rows", result.len());
            let rows = result.rows();
            if !rows.is_empty() {
                println!("First row: {:?}", rows[0]);
            }
        }
        Err(e) => {
            println!("Failed to query data: {:?}", e);
        }
    }

    let _ = std::fs::remove_file("/tmp/test_vector_basic.teg");
}

#[test]
fn test_vector_similarity_functions() {
    use tegdb::parser::{parse_sql, SqlValue, Statement};
    use tegdb::Database;

    let db_path = "file:///tmp/test_vector_similarity_functions.teg";
    let _ = std::fs::remove_file("/tmp/test_vector_similarity_functions.teg");
    let mut db = Database::open(db_path).unwrap();

    // Create table with vector column
    db.execute("CREATE TABLE vectors (id INTEGER PRIMARY KEY, embedding VECTOR(3))")
        .unwrap();

    // Debug: Check what the INSERT statement parses to
    let insert_sql = "INSERT INTO vectors (id, embedding) VALUES (1, [1.0, 0.0, 0.0])";
    println!("Parsing INSERT statement: {}", insert_sql);
    let parse_result = parse_sql(insert_sql);
    match parse_result {
        Ok(Statement::Insert(insert)) => {
            println!("INSERT parsed successfully");
            if let Some(row) = insert.values.first() {
                println!("First row values: {:?}", row);
                if let Some(vec_value) = row.get(1) {
                    println!("Vector value: {:?}", vec_value);
                    match vec_value {
                        Expression::Value(SqlValue::Vector(v)) => {
                            println!("Vector parsed correctly: {:?}", v);
                        }
                        Expression::Value(SqlValue::Text(s)) => {
                            println!("Vector parsed as text: '{}'", s);
                        }
                        _ => {
                            println!("Vector parsed as other type: {:?}", vec_value);
                        }
                    }
                }
            }
        }
        Ok(other) => {
            println!("Expected INSERT, got: {:?}", other);
        }
        Err(e) => {
            println!("Parse error: {:?}", e);
        }
    }

    // Insert test data
    db.execute("INSERT INTO vectors (id, embedding) VALUES (1, [1.0, 0.0, 0.0])")
        .unwrap();
    db.execute("INSERT INTO vectors (id, embedding) VALUES (2, [0.0, 1.0, 0.0])")
        .unwrap();
    db.execute("INSERT INTO vectors (id, embedding) VALUES (3, [1.0, 1.0, 0.0])")
        .unwrap();

    // Test COSINE_SIMILARITY
    let result = db
        .query("SELECT COSINE_SIMILARITY(embedding, [1.0, 0.0, 0.0]) FROM vectors WHERE id = 1")
        .unwrap();
    let rows = result.rows();
    assert!(!rows.is_empty());

    if let SqlValue::Real(similarity) = rows[0][0] {
        assert!((similarity - 1.0).abs() < 0.001); // Should be very close to 1.0
    } else {
        panic!("Expected Real value for similarity");
    }

    // Test EUCLIDEAN_DISTANCE
    let result = db
        .query("SELECT EUCLIDEAN_DISTANCE(embedding, [0.0, 1.0, 0.0]) FROM vectors WHERE id = 1")
        .unwrap();
    let rows = result.rows();
    assert!(!rows.is_empty());

    if let SqlValue::Real(distance) = rows[0][0] {
        assert!((distance - std::f64::consts::SQRT_2).abs() < 0.001); // sqrt(2)
    } else {
        panic!("Expected Real value for distance");
    }

    // Test DOT_PRODUCT
    let result = db
        .query("SELECT DOT_PRODUCT(embedding, [1.0, 1.0, 0.0]) FROM vectors WHERE id = 1")
        .unwrap();
    let rows = result.rows();
    assert!(!rows.is_empty());

    if let SqlValue::Real(dot_product) = rows[0][0] {
        assert!((dot_product - 1.0).abs() < 0.001); // 1*1 + 0*1 + 0*0 = 1
    } else {
        panic!("Expected Real value for dot product");
    }

    // Test L2_NORMALIZE
    let result = db
        .query("SELECT L2_NORMALIZE(embedding) FROM vectors WHERE id = 3")
        .unwrap();
    let rows = result.rows();
    assert!(!rows.is_empty());

    if let SqlValue::Vector(normalized) = &rows[0][0] {
        assert_eq!(normalized.len(), 3);
        // [1.0, 1.0, 0.0] normalized should be [0.7071067811865475, 0.7071067811865475, 0.0]
        assert!((normalized[0] - 0.7071067811865475).abs() < 0.001);
        assert!((normalized[1] - 0.7071067811865475).abs() < 0.001);
        assert!((normalized[2] - 0.0).abs() < 0.001);
    } else {
        panic!("Expected Vector value for normalized");
    }

    let _ = std::fs::remove_file("/tmp/test_vector_similarity_functions.teg");
}

#[test]
fn test_vector_debug() {
    use tegdb::Database;
    let db_path = "file:///tmp/test_vector_debug.teg";
    let _ = std::fs::remove_file("/tmp/test_vector_debug.teg");
    let mut db = Database::open(db_path).unwrap();

    // Test 1: Create table with vector column
    println!("Creating table...");
    let result = db.execute("CREATE TABLE vectors (id INTEGER PRIMARY KEY, embedding VECTOR(3))");
    match result {
        Ok(_) => println!("Table created successfully"),
        Err(e) => {
            println!("Failed to create table: {:?}", e);
            return;
        }
    }

    // Test 2: Check the schema
    println!("Checking schema...");
    let schemas = db.get_table_schemas();
    if let Some(schema) = schemas.get("vectors") {
        println!("Schema found: {:?}", schema);
        for (i, col) in schema.columns.iter().enumerate() {
            println!("Column {}: name={}, data_type={:?}, storage_offset={}, storage_size={}, storage_type_code={}", 
                i, col.name, col.data_type, col.storage_offset, col.storage_size, col.storage_type_code);
        }
    } else {
        println!("Schema not found!");
        return;
    }

    // Test 3: Try to insert vector data
    println!("Inserting data...");
    let result = db.execute("INSERT INTO vectors (id, embedding) VALUES (1, '[1.0, 0.0, 0.0]')");
    match result {
        Ok(_) => println!("Data inserted successfully"),
        Err(e) => {
            println!("Failed to insert data: {:?}", e);
            return;
        }
    }

    let _ = std::fs::remove_file("/tmp/test_vector_debug.teg");
}

#[test]
fn test_vector_parsing() {
    use tegdb::parser::{parse_sql, SqlValue, Statement};

    // Test vector parsing in an INSERT statement
    let result = parse_sql("INSERT INTO test (id, vec) VALUES (1, [1.0, 0.0, 0.0])");
    match result {
        Ok(Statement::Insert(insert)) => {
            println!("INSERT parsing successful");
            if let Some(row) = insert.values.first() {
                if let Some(vec_value) = row.get(1) {
                    println!("Vector value: {:?}", vec_value);
                    match vec_value {
                        Expression::Value(SqlValue::Vector(v)) => {
                            println!("Vector has {} elements: {:?}", v.len(), v);
                            assert_eq!(v.len(), 3);
                            assert_eq!(v[0], 1.0);
                            assert_eq!(v[1], 0.0);
                            assert_eq!(v[2], 0.0);
                        }
                        _ => {
                            println!("Expected Vector, got: {:?}", vec_value);
                            panic!("Expected Vector value");
                        }
                    }
                } else {
                    panic!("No vector value found");
                }
            } else {
                panic!("No values found");
            }
        }
        Ok(other) => {
            println!("Expected INSERT statement, got: {:?}", other);
            panic!("Expected INSERT statement");
        }
        Err(e) => {
            println!("INSERT parsing failed: {:?}", e);
            panic!("INSERT parsing should work");
        }
    }
}

#[test]
fn test_function_call_parsing() {
    use tegdb::parser::{parse_sql, Expression, Statement};

    // Test function call parsing
    let result = parse_sql("SELECT COSINE_SIMILARITY(embedding, [1.0, 0.0, 0.0]) FROM test");
    match result {
        Ok(Statement::Select(select)) => {
            println!("SELECT parsed successfully");
            if let Some(expr) = select.columns.first() {
                println!("First column expression: {:?}", expr);
                match expr {
                    Expression::FunctionCall { name, args } => {
                        println!("Function call: name={}, args={:?}", name, args);
                        assert_eq!(name, "COSINE_SIMILARITY");
                        assert_eq!(args.len(), 2);
                    }
                    _ => {
                        println!("Expected FunctionCall, got: {:?}", expr);
                        panic!("Expected FunctionCall");
                    }
                }
            } else {
                panic!("No columns found");
            }
        }
        Ok(other) => {
            println!("Expected SELECT, got: {:?}", other);
            panic!("Expected SELECT statement");
        }
        Err(e) => {
            println!("Parse error: {:?}", e);
            panic!("Parse should work");
        }
    }
}

#[test]
fn test_select_with_where_parsing() {
    use tegdb::parser::{parse_sql, Statement};

    // Test the exact SELECT statement that's failing
    let result =
        parse_sql("SELECT COSINE_SIMILARITY(embedding, [1.0, 0.0, 0.0]) FROM vectors WHERE id = 1");
    match result {
        Ok(Statement::Select(select)) => {
            println!("SELECT parsed successfully");
            println!("Columns: {:?}", select.columns);
            println!("Table: {}", select.table);
            println!("Where clause: {:?}", select.where_clause);
        }
        Ok(other) => {
            println!("Expected SELECT, got: {:?}", other);
            panic!("Expected SELECT statement");
        }
        Err(e) => {
            println!("Parse error: {:?}", e);
            panic!("Parse should work");
        }
    }
}

#[test]
fn test_vector_search_operations() {
    let db_path = "file:///tmp/test_vector_search_operations.teg";
    let _ = std::fs::remove_file("/tmp/test_vector_search_operations.teg");
    let mut db = Database::open(db_path).unwrap();

    // Create a table for embeddings
    db.execute(
        "CREATE TABLE embeddings (id INTEGER PRIMARY KEY, embedding VECTOR(3), text TEXT(50))",
    )
    .unwrap();

    // Insert some test embeddings
    println!("Inserting test embeddings...");
    db.execute(
        "INSERT INTO embeddings (id, embedding, text) VALUES (1, [1.0, 0.0, 0.0], 'unit vector x')",
    )
    .unwrap();
    println!("Inserted embedding 1");
    db.execute(
        "INSERT INTO embeddings (id, embedding, text) VALUES (2, [0.0, 1.0, 0.0], 'unit vector y')",
    )
    .unwrap();
    println!("Inserted embedding 2");
    db.execute(
        "INSERT INTO embeddings (id, embedding, text) VALUES (3, [0.0, 0.0, 1.0], 'unit vector z')",
    )
    .unwrap();
    println!("Inserted embedding 3");
    db.execute("INSERT INTO embeddings (id, embedding, text) VALUES (4, [0.5, 0.5, 0.0], 'diagonal vector')").unwrap();
    println!("Inserted embedding 4");

    // Test 1: K-NN query with cosine similarity
    println!("Testing K-NN query with cosine similarity...");
    let query_vector = "[0.8, 0.2, 0.0]";
    let result = db.query(&format!(
        "SELECT id, text, COSINE_SIMILARITY(embedding, {}) FROM embeddings ORDER BY COSINE_SIMILARITY(embedding, {}) DESC LIMIT 2",
        query_vector, query_vector
    )).unwrap();

    println!("K-NN query result: {} rows", result.rows().len());
    for row in result.rows() {
        println!("Row: {:?}", row);
    }

    // Test 2: Similarity threshold in WHERE clause
    println!("\nTesting similarity threshold in WHERE clause...");
    let result = db
        .query(&format!(
            "SELECT id, text FROM embeddings WHERE COSINE_SIMILARITY(embedding, {}) > 0.5",
            query_vector
        ))
        .unwrap();

    println!(
        "Similarity threshold query result: {} rows",
        result.rows().len()
    );
    for row in result.rows() {
        println!("Row: {:?}", row);
    }

    // Test 3: Range query with Euclidean distance
    println!("\nTesting range query with Euclidean distance...");
    let result = db
        .query(&format!(
            "SELECT id, text FROM embeddings WHERE EUCLIDEAN_DISTANCE(embedding, {}) < 1.5",
            query_vector
        ))
        .unwrap();

    println!("Range query result: {} rows", result.rows().len());
    for row in result.rows() {
        println!("Row: {:?}", row);
    }

    // Test 4: Combined query with multiple conditions
    println!("\nTesting combined query with multiple conditions...");
    let result = db.query(&format!(
        "SELECT id, text, COSINE_SIMILARITY(embedding, {}) FROM embeddings WHERE COSINE_SIMILARITY(embedding, {}) > 0.3 ORDER BY COSINE_SIMILARITY(embedding, {}) DESC LIMIT 3",
        query_vector, query_vector, query_vector
    )).unwrap();

    println!("Combined query result: {} rows", result.rows().len());
    for row in result.rows() {
        println!("Row: {:?}", row);
    }
}

#[test]
fn test_vector_indexing() {
    use tegdb::vector_index::{HNSWIndex, IVFIndex, LSHIndex};

    // Test HNSW Index
    println!("Testing HNSW Index...");
    let mut hnsw = HNSWIndex::new(16, 32);

    // Insert test vectors
    hnsw.insert(1, vec![1.0, 0.0, 0.0]).unwrap();
    hnsw.insert(2, vec![0.0, 1.0, 0.0]).unwrap();
    hnsw.insert(3, vec![0.0, 0.0, 1.0]).unwrap();
    hnsw.insert(4, vec![0.5, 0.5, 0.0]).unwrap();
    hnsw.insert(5, vec![0.7, 0.3, 0.0]).unwrap();

    // Search for similar vectors
    let query = vec![0.8, 0.2, 0.0];
    let results = hnsw.search(&query, 3).unwrap();

    println!("HNSW search results: {:?}", results);
    assert_eq!(results.len(), 3);
    assert_eq!(results[0].0, 5); // Should find vector 5 first (most similar)

    // Test IVF Index
    println!("Testing IVF Index...");
    let mut ivf = IVFIndex::new(2);

    let vectors = vec![
        (1, vec![1.0, 0.0]),
        (2, vec![0.0, 1.0]),
        (3, vec![0.9, 0.1]),
        (4, vec![0.1, 0.9]),
        (5, vec![0.8, 0.2]),
    ];

    ivf.build(vectors).unwrap();

    // Search
    let query = vec![0.7, 0.3];
    let results = ivf.search(&query, 2).unwrap();

    println!("IVF search results: {:?}", results);
    assert_eq!(results.len(), 2);

    // Test LSH Index
    println!("Testing LSH Index...");
    let mut lsh = LSHIndex::new(4, 8, 3); // 4 tables, 8 functions per table, 3D vectors

    // Insert test vectors
    lsh.insert(1, vec![1.0, 0.0, 0.0]).unwrap();
    lsh.insert(2, vec![0.0, 1.0, 0.0]).unwrap();
    lsh.insert(3, vec![0.0, 0.0, 1.0]).unwrap();
    lsh.insert(4, vec![0.5, 0.5, 0.0]).unwrap();
    lsh.insert(5, vec![0.7, 0.3, 0.0]).unwrap();

    // Search
    let query = vec![0.8, 0.2, 0.0];
    let results = lsh.search(&query, 3).unwrap();

    println!("LSH search results: {:?}", results);
    assert!(!results.is_empty()); // LSH should find some candidates

    // Test that all three indexes work correctly
    println!("Vector indexing tests passed!");
}
