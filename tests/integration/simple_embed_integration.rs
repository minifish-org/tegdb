//! Simple integration test for EMBED - matching working test patterns

use tegdb::{Database, Result, SqlValue};

#[test]
fn test_embed_simple() -> Result<()> {
    let db_path = std::env::temp_dir().join("test_embed_simple.db");
    let _ = std::fs::remove_file(&db_path);
    
    let mut db = Database::open(db_path.to_string_lossy())?;
    
    // Create table
    db.execute("CREATE TABLE test (id INTEGER PRIMARY KEY, embedding VECTOR(128))")?;
    
    // Insert with EMBED - exact same pattern as vector_search_tests
    db.execute("INSERT INTO test (id, embedding) VALUES (1, EMBED('hello world'))")?;
    
    // Query it back
    let result = db.query("SELECT id, embedding FROM test WHERE id = 1")?;
    let rows = result.rows();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0][0], SqlValue::Integer(1));
    
    // Check embedding dimension
    match &rows[0][1] {
        SqlValue::Vector(v) => {
            assert_eq!(v.len(), 128);
        }
        _ => panic!("Expected vector"),
    }
    
    Ok(())
}

#[test]
fn test_embed_in_select_direct() -> Result<()> {
    let db_path = std::env::temp_dir().join("test_embed_select.db");
    let _ = std::fs::remove_file(&db_path);
    
    let mut db = Database::open(db_path.to_string_lossy())?;
    
    // Test EMBED in SELECT - similar to how COSINE_SIMILARITY is tested
    let result = db.query("SELECT EMBED('test query') as embedding")?;
    let rows = result.rows();
    assert_eq!(rows.len(), 1);
    
    match &rows[0][0] {
        SqlValue::Vector(v) => {
            assert_eq!(v.len(), 128);
        }
        _ => panic!("Expected vector"),
    }
    
    Ok(())
}
