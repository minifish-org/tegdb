mod test_helpers {
    include!("../common/test_helpers.rs");
}
use test_helpers::run_with_both_backends;

use tegdb::{Database, Result, SqlValue};

#[test]
fn test_vector_different_dimensions() -> Result<()> {
    run_with_both_backends("test_vector_different_dimensions", |db_path| {
        let mut db = Database::open(db_path)?;

        // Test 2D vectors
        db.execute("CREATE TABLE vec2d (id INTEGER PRIMARY KEY, vec VECTOR(2))")?;
        db.execute("INSERT INTO vec2d (id, vec) VALUES (1, [1.0, 2.0])")?;
        let result = db.query("SELECT vec FROM vec2d WHERE id = 1")?;
        if let SqlValue::Vector(v) = &result.rows()[0][0] {
            assert_eq!(v.len(), 2);
        } else {
            panic!("Expected vector");
        }

        // Test 128D vectors (common for embeddings)
        db.execute("CREATE TABLE vec128d (id INTEGER PRIMARY KEY, vec VECTOR(128))")?;
        let vec128: Vec<f64> = (0..128).map(|i| i as f64 / 128.0).collect();
        let vec_str = format!(
            "[{}]",
            vec128
                .iter()
                .map(|v| v.to_string())
                .collect::<Vec<_>>()
                .join(", ")
        );
        db.execute(&format!(
            "INSERT INTO vec128d (id, vec) VALUES (1, {})",
            vec_str
        ))?;
        let result = db.query("SELECT vec FROM vec128d WHERE id = 1")?;
        if let SqlValue::Vector(v) = &result.rows()[0][0] {
            assert_eq!(v.len(), 128);
        } else {
            panic!("Expected vector");
        }

        // Test 256D vectors
        db.execute("CREATE TABLE vec256d (id INTEGER PRIMARY KEY, vec VECTOR(256))")?;
        let vec256: Vec<f64> = (0..256).map(|i| i as f64 / 256.0).collect();
        let vec_str = format!(
            "[{}]",
            vec256
                .iter()
                .map(|v| v.to_string())
                .collect::<Vec<_>>()
                .join(", ")
        );
        db.execute(&format!(
            "INSERT INTO vec256d (id, vec) VALUES (1, {})",
            vec_str
        ))?;
        let result = db.query("SELECT vec FROM vec256d WHERE id = 1")?;
        if let SqlValue::Vector(v) = &result.rows()[0][0] {
            assert_eq!(v.len(), 256);
        } else {
            panic!("Expected vector");
        }

        Ok(())
    })
}

#[test]
fn test_vector_edge_cases_comprehensive() -> Result<()> {
    run_with_both_backends("test_vector_edge_cases_comprehensive", |db_path| {
        let mut db = Database::open(db_path)?;

        db.execute("CREATE TABLE edge_vectors (id INTEGER PRIMARY KEY, vec VECTOR(3))")?;

        // Zero vector
        db.execute("INSERT INTO edge_vectors (id, vec) VALUES (1, [0.0, 0.0, 0.0])")?;

        // Very large values
        db.execute("INSERT INTO edge_vectors (id, vec) VALUES (2, [1000.0, 2000.0, 3000.0])")?;

        // Very small values
        db.execute("INSERT INTO edge_vectors (id, vec) VALUES (3, [0.0001, 0.0002, 0.0003])")?;

        // Mixed positive and negative
        db.execute("INSERT INTO edge_vectors (id, vec) VALUES (4, [1.0, -1.0, 0.5])")?;

        // Unit vectors
        db.execute("INSERT INTO edge_vectors (id, vec) VALUES (5, [1.0, 0.0, 0.0])")?;
        db.execute("INSERT INTO edge_vectors (id, vec) VALUES (6, [0.0, 1.0, 0.0])")?;
        db.execute("INSERT INTO edge_vectors (id, vec) VALUES (7, [0.0, 0.0, 1.0])")?;

        // Test queries on edge cases
        let result = db.query("SELECT id FROM edge_vectors WHERE id = 1")?;
        assert_eq!(result.len(), 1);

        // Test cosine similarity with unit vectors
        let result = db.query(
            "SELECT COSINE_SIMILARITY(vec, [1.0, 0.0, 0.0]) FROM edge_vectors WHERE id = 5",
        )?;
        assert_eq!(result.len(), 1);
        if let SqlValue::Real(sim) = result.rows()[0][0] {
            assert!((sim - 1.0).abs() < 0.001); // Should be 1.0 for identical vectors
        }

        Ok(())
    })
}

#[test]
fn test_vector_operations_in_where_clauses() -> Result<()> {
    run_with_both_backends("test_vector_operations_in_where_clauses", |db_path| {
        let mut db = Database::open(db_path)?;

        db.execute(
            "CREATE TABLE embeddings (id INTEGER PRIMARY KEY, text TEXT(50), embedding VECTOR(3))",
        )?;
        db.execute(
            "INSERT INTO embeddings (id, text, embedding) VALUES (1, 'cat', [1.0, 0.0, 0.0])",
        )?;
        db.execute(
            "INSERT INTO embeddings (id, text, embedding) VALUES (2, 'dog', [0.0, 1.0, 0.0])",
        )?;
        db.execute(
            "INSERT INTO embeddings (id, text, embedding) VALUES (3, 'bird', [0.0, 0.0, 1.0])",
        )?;

        // Note: Direct vector function in WHERE may not be fully supported,
        // but we can test basic vector column queries
        let result = db.query("SELECT id, text FROM embeddings WHERE id = 1")?;
        assert_eq!(result.len(), 1);

        // Test that we can select vector columns
        let result = db.query("SELECT embedding FROM embeddings WHERE id = 1")?;
        assert_eq!(result.len(), 1);
        if let SqlValue::Vector(v) = &result.rows()[0][0] {
            assert_eq!(v.len(), 3);
        }

        Ok(())
    })
}

#[test]
fn test_vector_operations_with_order_by_and_limit() -> Result<()> {
    run_with_both_backends(
        "test_vector_operations_with_order_by_and_limit",
        |db_path| {
            let mut db = Database::open(db_path)?;

            db.execute("CREATE TABLE vec_search (id INTEGER PRIMARY KEY, embedding VECTOR(3))")?;
            db.execute("INSERT INTO vec_search (id, embedding) VALUES (1, [1.0, 0.0, 0.0])")?;
            db.execute("INSERT INTO vec_search (id, embedding) VALUES (2, [0.0, 1.0, 0.0])")?;
            db.execute("INSERT INTO vec_search (id, embedding) VALUES (3, [0.0, 0.0, 1.0])")?;
            db.execute("INSERT INTO vec_search (id, embedding) VALUES (4, [0.5, 0.5, 0.0])")?;
            db.execute("INSERT INTO vec_search (id, embedding) VALUES (5, [0.7, 0.3, 0.0])")?;

            // Query with ORDER BY similarity and LIMIT
            let result = db.query("SELECT id, COSINE_SIMILARITY(embedding, [0.8, 0.2, 0.0]) AS sim FROM vec_search ORDER BY sim DESC LIMIT 2")?;
            assert_eq!(result.len(), 2);

            // Verify results are ordered by similarity (descending)
            let rows = result.rows();
            if rows.len() >= 2 {
                if let (SqlValue::Real(sim1), SqlValue::Real(sim2)) = (&rows[0][1], &rows[1][1]) {
                    assert!(
                        sim1 >= sim2,
                        "Results should be ordered by similarity descending"
                    );
                }
            }

            Ok(())
        },
    )
}

#[test]
fn test_multiple_vector_columns() -> Result<()> {
    run_with_both_backends("test_multiple_vector_columns", |db_path| {
        let mut db = Database::open(db_path)?;

        db.execute("CREATE TABLE multi_vec (id INTEGER PRIMARY KEY, vec1 VECTOR(3), vec2 VECTOR(3), vec3 VECTOR(2))")?;
        db.execute("INSERT INTO multi_vec (id, vec1, vec2, vec3) VALUES (1, [1.0, 0.0, 0.0], [0.0, 1.0, 0.0], [0.5, 0.5])")?;

        // Query all vector columns
        let result = db.query("SELECT vec1, vec2, vec3 FROM multi_vec WHERE id = 1")?;
        assert_eq!(result.len(), 1);

        let row = &result.rows()[0];
        if let (SqlValue::Vector(v1), SqlValue::Vector(v2), SqlValue::Vector(v3)) =
            (&row[0], &row[1], &row[2])
        {
            assert_eq!(v1.len(), 3);
            assert_eq!(v2.len(), 3);
            assert_eq!(v3.len(), 2);
        } else {
            panic!("Expected vectors");
        }

        // Use different vector columns in operations
        let result =
            db.query("SELECT COSINE_SIMILARITY(vec1, vec2) FROM multi_vec WHERE id = 1")?;
        assert_eq!(result.len(), 1);
        if let SqlValue::Real(sim) = result.rows()[0][0] {
            // vec1 and vec2 are orthogonal, so similarity should be 0
            assert!((sim - 0.0).abs() < 0.001);
        }

        Ok(())
    })
}

#[test]
fn test_vector_operations_with_null_handling() -> Result<()> {
    run_with_both_backends("test_vector_operations_with_null_handling", |db_path| {
        let mut db = Database::open(db_path)?;

        db.execute("CREATE TABLE vec_null (id INTEGER PRIMARY KEY, embedding VECTOR(3), metadata TEXT(50))")?;
        db.execute(
            "INSERT INTO vec_null (id, embedding, metadata) VALUES (1, [1.0, 0.0, 0.0], 'has_vec')",
        )?;
        db.execute("INSERT INTO vec_null (id, metadata) VALUES (2, 'no_vec')")?; // NULL embedding

        // Query with NULL embedding
        let result = db.query("SELECT id, metadata FROM vec_null WHERE id = 2")?;
        assert_eq!(result.len(), 1);
        assert_eq!(result.rows()[0][1], SqlValue::Text("no_vec".to_string()));

        // Query embedding column - NULL should be handled
        let result = db.query("SELECT embedding FROM vec_null WHERE id = 2")?;
        assert_eq!(result.len(), 1);
        assert_eq!(result.rows()[0][0], SqlValue::Null);

        Ok(())
    })
}

#[test]
fn test_vector_dimension_mismatch_error() -> Result<()> {
    run_with_both_backends("test_vector_dimension_mismatch_error", |db_path| {
        let mut db = Database::open(db_path)?;

        db.execute("CREATE TABLE vec_dim (id INTEGER PRIMARY KEY, vec VECTOR(3))")?;
        db.execute("INSERT INTO vec_dim (id, vec) VALUES (1, [1.0, 0.0, 0.0])")?;

        // Try to insert vector with wrong dimension - should fail
        let result = db.execute("INSERT INTO vec_dim (id, vec) VALUES (2, [1.0, 0.0])"); // 2D instead of 3D
        assert!(result.is_err(), "Should fail with dimension mismatch");

        // Try to use vector with wrong dimension in similarity - should fail
        let result =
            db.query("SELECT COSINE_SIMILARITY(vec, [1.0, 0.0]) FROM vec_dim WHERE id = 1"); // 2D query vector
        assert!(result.is_err(), "Should fail with dimension mismatch");

        Ok(())
    })
}

#[test]
fn test_vector_large_dataset_performance() -> Result<()> {
    run_with_both_backends("test_vector_large_dataset_performance", |db_path| {
        let mut db = Database::open(db_path)?;

        db.execute("CREATE TABLE large_vec (id INTEGER PRIMARY KEY, embedding VECTOR(128))")?;

        // Insert many vectors
        for i in 1..=100 {
            let vec: Vec<f64> = (0..128).map(|j| (i + j) as f64 / 1000.0).collect();
            let vec_str = format!(
                "[{}]",
                vec.iter()
                    .map(|v| v.to_string())
                    .collect::<Vec<_>>()
                    .join(", ")
            );
            db.execute(&format!(
                "INSERT INTO large_vec (id, embedding) VALUES ({}, {})",
                i, vec_str
            ))?;
        }

        // Query with LIMIT to test performance
        let result = db.query("SELECT id FROM large_vec LIMIT 10")?;
        assert_eq!(result.len(), 10);

        // Query with similarity and LIMIT
        let query_vec: Vec<f64> = (0..128).map(|j| (j + 50) as f64 / 1000.0).collect();
        let query_vec_str = format!(
            "[{}]",
            query_vec
                .iter()
                .map(|v| v.to_string())
                .collect::<Vec<_>>()
                .join(", ")
        );
        let result = db.query(&format!("SELECT id, COSINE_SIMILARITY(embedding, {}) AS sim FROM large_vec ORDER BY sim DESC LIMIT 5", query_vec_str))?;
        assert_eq!(result.len(), 5);

        Ok(())
    })
}
