use tegdb::Database;
use tempfile::NamedTempFile;

fn main() -> tegdb::Result<()> {
    println!("=== TegDB Vector Search Integration Demo ===");

    // Create a temporary file for the database
    let temp_file = NamedTempFile::new().expect("Failed to create temp file");
    let db_path = temp_file.path();

    // Create a new database
    let mut db = Database::open(format!("file://{}", db_path.display()))?;

    // Create a table with vector column
    println!("\n1. Creating table with vector column...");
    db.execute(
        "CREATE TABLE embeddings (
            id INTEGER PRIMARY KEY,
            text TEXT(32),
            embedding VECTOR(3)
        )",
    )?;

    // Insert some sample data with vectors
    println!("\n2. Inserting sample data with vectors...");
    db.execute(
        "INSERT INTO embeddings (id, text, embedding) VALUES 
         (1, 'cat', [1.0, 0.0, 0.0])",
    )?;
    db.execute(
        "INSERT INTO embeddings (id, text, embedding) VALUES 
         (2, 'dog', [0.0, 1.0, 0.0])",
    )?;
    db.execute(
        "INSERT INTO embeddings (id, text, embedding) VALUES 
         (3, 'bird', [0.0, 0.0, 1.0])",
    )?;
    db.execute(
        "INSERT INTO embeddings (id, text, embedding) VALUES 
         (4, 'fish', [0.5, 0.5, 0.0])",
    )?;

    // Create a vector index
    println!("\n3. Creating vector index...");
    db.execute("CREATE INDEX idx_hnsw ON embeddings (embedding) USING HNSW")?;

    // Test vector similarity functions
    println!("\n4. Testing vector similarity functions...");
    let result = db.query(
        "SELECT id, text, COSINE_SIMILARITY(embedding, [1.0, 0.0, 0.0]) 
         FROM embeddings WHERE id = 1"
    )?;
    
    println!("Cosine similarity test:");
    for row in result.rows() {
        println!("  {row:?}");
    }

    // Test K-NN query (this should now use the vector index)
    println!("\n5. Testing K-NN query with vector index...");
    let result = db.query(
        "SELECT id, text, COSINE_SIMILARITY(embedding, [0.8, 0.2, 0.0]) 
         FROM embeddings 
         ORDER BY COSINE_SIMILARITY(embedding, [0.8, 0.2, 0.0]) DESC 
         LIMIT 3"
    )?;
    
    println!("K-NN search results (should be ordered by similarity):");
    for row in result.rows() {
        println!("  {row:?}");
    }

    // Test similarity threshold query
    println!("\n6. Testing similarity threshold query...");
    let result = db.query(
        "SELECT id, text FROM embeddings 
         WHERE COSINE_SIMILARITY(embedding, [1.0, 0.0, 0.0]) > 0.5"
    )?;
    
    println!("Similarity threshold results (> 0.5):");
    for row in result.rows() {
        println!("  {row:?}");
    }

    // Test different similarity functions
    println!("\n7. Testing different similarity functions...");
    
    // Euclidean distance
    let result = db.query(
        "SELECT id, text, EUCLIDEAN_DISTANCE(embedding, [1.0, 0.0, 0.0]) 
         FROM embeddings 
         ORDER BY EUCLIDEAN_DISTANCE(embedding, [1.0, 0.0, 0.0]) ASC 
         LIMIT 2"
    )?;
    
    println!("Euclidean distance results (closest first):");
    for row in result.rows() {
        println!("  {row:?}");
    }

    // Dot product
    let result = db.query(
        "SELECT id, text, DOT_PRODUCT(embedding, [1.0, 0.0, 0.0]) 
         FROM embeddings 
         ORDER BY DOT_PRODUCT(embedding, [1.0, 0.0, 0.0]) DESC 
         LIMIT 2"
    )?;
    
    println!("Dot product results (highest first):");
    for row in result.rows() {
        println!("  {row:?}");
    }

    println!("\n8. Vector search integration is working!");
    println!("   ✅ Vector similarity functions work in SQL");
    println!("   ✅ Vector indexes can be created");
    println!("   ✅ K-NN queries are supported");
    println!("   ✅ Similarity thresholds work");
    println!("   ✅ Multiple similarity functions available");

    // Clean up
    let _ = std::fs::remove_file(db_path);

    Ok(())
}
