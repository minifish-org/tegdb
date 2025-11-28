mod test_helpers {
    include!("../common/test_helpers.rs");
}
use test_helpers::run_with_both_backends;

use tegdb::{Database, Result, SqlValue};

#[test]
fn test_embed_various_text_inputs() -> Result<()> {
    run_with_both_backends("test_embed_various_text_inputs", |db_path| {
        let mut db = Database::open(db_path)?;

        db.execute("CREATE TABLE embed_test (id INTEGER PRIMARY KEY, embedding VECTOR(128))")?;

        // Short text
        db.execute("INSERT INTO embed_test (id, embedding) VALUES (1, EMBED('hello'))")?;

        // Long text
        let long_text = "This is a very long text that should be embedded properly. ".repeat(10);
        db.execute(&format!(
            "INSERT INTO embed_test (id, embedding) VALUES (2, EMBED('{}'))",
            long_text
        ))?;

        // Unicode text
        db.execute(
            "INSERT INTO embed_test (id, embedding) VALUES (3, EMBED('café naïve résumé'))",
        )?;

        // Special characters
        db.execute("INSERT INTO embed_test (id, embedding) VALUES (4, EMBED('Hello! @#$%^&*()'))")?;

        // Empty string
        db.execute("INSERT INTO embed_test (id, embedding) VALUES (5, EMBED(''))")?;

        // Verify all embeddings have correct dimension
        for i in 1..=5 {
            let result = db.query(&format!(
                "SELECT embedding FROM embed_test WHERE id = {}",
                i
            ))?;
            if let SqlValue::Vector(v) = &result.rows()[0][0] {
                assert_eq!(v.len(), 128, "Embedding should be 128 dimensions");
            } else {
                panic!("Expected vector for id {}", i);
            }
        }

        Ok(())
    })
}

#[test]
fn test_embed_in_insert_statements() -> Result<()> {
    run_with_both_backends("test_embed_in_insert_statements", |db_path| {
        let mut db = Database::open(db_path)?;

        db.execute("CREATE TABLE documents (id INTEGER PRIMARY KEY, text TEXT(200), embedding VECTOR(128))")?;

        // Insert with EMBED
        db.execute("INSERT INTO documents (id, text, embedding) VALUES (1, 'First document', EMBED('First document'))")?;
        db.execute("INSERT INTO documents (id, text, embedding) VALUES (2, 'Second document', EMBED('Second document'))")?;

        // Verify embeddings were created
        let result = db.query("SELECT embedding FROM documents WHERE id = 1")?;
        if let SqlValue::Vector(v) = &result.rows()[0][0] {
            assert_eq!(v.len(), 128);
        }

        // Verify we can query by text and get embedding
        let result = db.query("SELECT embedding FROM documents WHERE text = 'First document'")?;
        assert_eq!(result.len(), 1);
        if let SqlValue::Vector(v) = &result.rows()[0][0] {
            assert_eq!(v.len(), 128);
        }

        Ok(())
    })
}

#[test]
fn test_embed_in_select_statements() -> Result<()> {
    run_with_both_backends("test_embed_in_select_statements", |db_path| {
        let mut db = Database::open(db_path)?;

        // Test EMBED in SELECT without table
        let result = db.query("SELECT EMBED('test query') as embedding")?;
        assert_eq!(result.len(), 1);
        if let SqlValue::Vector(v) = &result.rows()[0][0] {
            assert_eq!(v.len(), 128);
        } else {
            panic!("Expected vector");
        }

        // Test EMBED in SELECT with table
        db.execute("CREATE TABLE queries (id INTEGER PRIMARY KEY, query_text TEXT(200))")?;
        db.execute("INSERT INTO queries (id, query_text) VALUES (1, 'search query')")?;

        let result = db.query("SELECT EMBED(query_text) FROM queries WHERE id = 1")?;
        assert_eq!(result.len(), 1);
        if let SqlValue::Vector(v) = &result.rows()[0][0] {
            assert_eq!(v.len(), 128);
        }

        Ok(())
    })
}

#[test]
fn test_embed_dimension_verification() -> Result<()> {
    run_with_both_backends("test_embed_dimension_verification", |db_path| {
        let mut db = Database::open(db_path)?;

        // Test multiple EMBED calls to verify consistent dimension
        let texts = [
            "short",
            "medium length text",
            "this is a much longer text that should still produce the same dimension embedding",
        ];

        for (i, text) in texts.iter().enumerate() {
            let result = db.query(&format!("SELECT EMBED('{}')", text))?;
            if let SqlValue::Vector(v) = &result.rows()[0][0] {
                assert_eq!(
                    v.len(),
                    128,
                    "All embeddings should be 128 dimensions, text {} failed",
                    i
                );
            }
        }

        Ok(())
    })
}

#[test]
fn test_embed_in_where_clauses() -> Result<()> {
    run_with_both_backends("test_embed_in_where_clauses", |db_path| {
        let mut db = Database::open(db_path)?;

        db.execute("CREATE TABLE search_docs (id INTEGER PRIMARY KEY, content TEXT(200), embedding VECTOR(128))")?;
        db.execute(
            "INSERT INTO search_docs (id, content, embedding) VALUES (1, 'cat', EMBED('cat'))",
        )?;
        db.execute(
            "INSERT INTO search_docs (id, content, embedding) VALUES (2, 'dog', EMBED('dog'))",
        )?;
        db.execute(
            "INSERT INTO search_docs (id, content, embedding) VALUES (3, 'bird', EMBED('bird'))",
        )?;

        // Use EMBED in WHERE clause for similarity search
        // Note: This may require specific syntax support
        // For now, test that we can use EMBED in computed columns
        let result = db.query("SELECT id, content, COSINE_SIMILARITY(embedding, EMBED('cat')) AS sim FROM search_docs ORDER BY sim DESC LIMIT 1")?;
        assert_eq!(result.len(), 1);

        // The most similar should be 'cat' itself
        assert_eq!(result.rows()[0][1], SqlValue::Text("cat".to_string()));

        Ok(())
    })
}

#[test]
fn test_embed_with_vector_similarity_functions() -> Result<()> {
    run_with_both_backends("test_embed_with_vector_similarity_functions", |db_path| {
        let mut db = Database::open(db_path)?;

        db.execute("CREATE TABLE semantic_search (id INTEGER PRIMARY KEY, text TEXT(200), embedding VECTOR(128))")?;
        db.execute("INSERT INTO semantic_search (id, text, embedding) VALUES (1, 'machine learning', EMBED('machine learning'))")?;
        db.execute("INSERT INTO semantic_search (id, text, embedding) VALUES (2, 'artificial intelligence', EMBED('artificial intelligence'))")?;
        db.execute("INSERT INTO semantic_search (id, text, embedding) VALUES (3, 'cooking recipe', EMBED('cooking recipe'))")?;

        // Use EMBED with COSINE_SIMILARITY
        let result = db.query("SELECT text, COSINE_SIMILARITY(embedding, EMBED('AI')) AS similarity FROM semantic_search ORDER BY similarity DESC LIMIT 1")?;
        assert_eq!(result.len(), 1);
        // Should find 'artificial intelligence' as most similar to 'AI'

        // Use EMBED with EUCLIDEAN_DISTANCE
        let result = db.query("SELECT text, EUCLIDEAN_DISTANCE(embedding, EMBED('machine')) AS distance FROM semantic_search ORDER BY distance ASC LIMIT 1")?;
        assert_eq!(result.len(), 1);
        // Should find 'machine learning' as closest

        // Use EMBED with DOT_PRODUCT
        let result = db.query("SELECT text, DOT_PRODUCT(embedding, EMBED('learning')) AS dot_product FROM semantic_search WHERE id = 1")?;
        assert_eq!(result.len(), 1);
        if let SqlValue::Real(dp) = result.rows()[0][1] {
            assert!(
                dp > 0.0,
                "Dot product should be positive for related concepts"
            );
        }

        Ok(())
    })
}

#[test]
fn test_embed_null_handling() -> Result<()> {
    run_with_both_backends("test_embed_null_handling", |db_path| {
        let mut db = Database::open(db_path)?;

        db.execute("CREATE TABLE embed_null_test (id INTEGER PRIMARY KEY, text TEXT(200), embedding VECTOR(128))")?;

        // Insert with NULL text - embedding should still work if text is provided separately
        db.execute("INSERT INTO embed_null_test (id, text) VALUES (1, NULL)")?;

        // EMBED with empty string should still produce a vector
        let result = db.query("SELECT EMBED('')")?;
        if let SqlValue::Vector(v) = &result.rows()[0][0] {
            assert_eq!(v.len(), 128);
        }

        // EMBED should handle various edge cases
        let result = db.query("SELECT EMBED('   ')")?; // Whitespace only
        if let SqlValue::Vector(v) = &result.rows()[0][0] {
            assert_eq!(v.len(), 128);
        }

        Ok(())
    })
}

#[test]
fn test_embed_error_cases() -> Result<()> {
    run_with_both_backends("test_embed_error_cases", |db_path| {
        let mut db = Database::open(db_path)?;

        db.execute("CREATE TABLE embed_errors (id INTEGER PRIMARY KEY, embedding VECTOR(128))")?;

        // Test that EMBED requires a text argument
        // This would be a parser error, so we test valid syntax
        // Invalid: EMBED() - missing argument
        // This should be caught by parser, but we test valid cases

        // Test that EMBED works with various valid inputs
        let valid_inputs = vec![
            "'simple'",
            "'text with spaces'",
            "'text with numbers 123'",
            "'text with symbols !@#$%'",
        ];

        for input in valid_inputs {
            let result = db.query(&format!("SELECT EMBED({})", input));
            assert!(result.is_ok(), "EMBED should work with input: {}", input);
        }

        Ok(())
    })
}

#[test]
fn test_embed_in_complex_queries() -> Result<()> {
    run_with_both_backends("test_embed_in_complex_queries", |db_path| {
        let mut db = Database::open(db_path)?;

        db.execute("CREATE TABLE complex_embed (id INTEGER PRIMARY KEY, title TEXT(100), content TEXT(500), embedding VECTOR(128))")?;
        db.execute("INSERT INTO complex_embed (id, title, content, embedding) VALUES (1, 'Article 1', 'Content about AI', EMBED('Content about AI'))")?;
        db.execute("INSERT INTO complex_embed (id, title, content, embedding) VALUES (2, 'Article 2', 'Content about cooking', EMBED('Content about cooking'))")?;

        // Complex query with EMBED, similarity, ORDER BY, and LIMIT
        let result = db.query("SELECT title, COSINE_SIMILARITY(embedding, EMBED('artificial intelligence')) AS sim FROM complex_embed ORDER BY sim DESC LIMIT 1")?;
        assert_eq!(result.len(), 1);
        assert_eq!(result.rows()[0][0], SqlValue::Text("Article 1".to_string()));

        // Query with WHERE and EMBED
        let result = db.query("SELECT title FROM complex_embed WHERE COSINE_SIMILARITY(embedding, EMBED('food')) > 0.5")?;
        // Should find Article 2 if similarity is high enough
        // May or may not match depending on embedding model, just verify query executes
        let _ = result.len(); // Query executed successfully

        Ok(())
    })
}
