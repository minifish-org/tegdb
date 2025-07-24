use tegdb::Database;
use tempfile::NamedTempFile;

fn main() -> tegdb::Result<()> {
    println!("=== TegDB Vector Support Demo ===");
    
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
            embedding VECTOR(10)
        )"
    )?;
    
    // Insert some sample data with vectors
    println!("\n2. Inserting sample data with vectors...");
    db.execute(
        "INSERT INTO embeddings (id, text, embedding) VALUES 
         (1, 'hello world', [0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9, 1.0])"
    )?;
    db.execute(
        "INSERT INTO embeddings (id, text, embedding) VALUES 
         (2, 'goodbye world', [1.0, 0.9, 0.8, 0.7, 0.6, 0.5, 0.4, 0.3, 0.2, 0.1])"
    )?;
    db.execute(
        "INSERT INTO embeddings (id, text, embedding) VALUES 
         (3, 'test message', [0.5, 0.5, 0.5, 0.5, 0.5, 0.5, 0.5, 0.5, 0.5, 0.5])"
    )?;
    
    // Query the data
    println!("\n3. Querying data with vectors...");
    let results = db.query("SELECT * FROM embeddings")?;
    
    println!("Columns: {:?}", results.columns());
    println!("Rows:");
    for row in results.rows() {
        println!("  {:?}", row);
    }
    
    // Test vector comparison (this will be implemented in the next step)
    println!("\n4. Vector support is now available!");
    println!("   - VECTOR data type is supported");
    println!("   - Vector literals can be inserted: [1.0, 2.0, 3.0]");
    println!("   - Vector columns are stored efficiently");
    println!("   - Next step: Add vector similarity search functions");
    
    // Clean up
    let _ = std::fs::remove_file(db_path);
    
    Ok(())
} 