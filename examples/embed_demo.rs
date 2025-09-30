//! Simple demo of the EMBED function

use tegdb::{Database, Result};

fn main() -> Result<()> {
    let temp_dir = tempfile::tempdir().unwrap();
    let db_path = temp_dir.path().join("embed_demo.db");
    let mut db = Database::open(format!("file://{}", db_path.display()))?;

    println!("=== EMBED Function Demo ===\n");

    // Test 1: Simple EMBED in SELECT
    println!("1. Testing EMBED in SELECT...");
    let result = db.query("SELECT EMBED('hello world') as embedding")?;
    let rows = result.rows();
    println!("   ✓ Query succeeded, got {} row(s)", rows.len());
    match &rows[0][0] {
        tegdb::SqlValue::Vector(v) => {
            println!("   ✓ Embedding dimension: {}", v.len());
            println!("   ✓ First few values: {:?}", &v[0..5.min(v.len())]);
        }
        _ => println!("   ✗ Expected vector"),
    }

    // Test 2: Create table with embedding column
    println!("\n2. Creating table with vector column...");
    db.execute(
        "CREATE TABLE documents (id INTEGER PRIMARY KEY, text TEXT(128), embedding VECTOR(128))",
    )?;
    println!("   ✓ Table created");

    // Test 3: Insert with EMBED
    println!("\n3. Inserting documents with embeddings...");
    db.execute("INSERT INTO documents (id, text, embedding) VALUES (1, 'machine learning', EMBED('machine learning'))")?;
    db.execute("INSERT INTO documents (id, text, embedding) VALUES (2, 'deep learning', EMBED('deep learning'))")?;
    db.execute("INSERT INTO documents (id, text, embedding) VALUES (3, 'database systems', EMBED('database systems'))")?;
    println!("   ✓ Inserted 3 documents");

    // Test 4: Query with similarity
    println!("\n4. Semantic search...");
    let result = db.query(
        "SELECT id, text, COSINE_SIMILARITY(embedding, EMBED('artificial intelligence')) as similarity \
         FROM documents \
         ORDER BY similarity DESC \
         LIMIT 2"
    )?;
    let rows = result.rows();
    println!("   ✓ Found {} similar documents:", rows.len());
    for row in rows {
        let id = &row[0];
        let text = &row[1];
        let sim = &row[2];
        println!("      - {id:?}: {text:?} (similarity: {sim:?})");
    }

    // Test 5: Different models
    println!("\n5. Testing different embedding models...");
    let _result = db.query("SELECT EMBED('test', 'simple') as simple_embed")?;
    println!("   ✓ Simple model works");

    println!("\n=== Demo Complete! ===");

    Ok(())
}
