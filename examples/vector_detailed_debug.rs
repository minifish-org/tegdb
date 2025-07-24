use tegdb::{Database, Result};
use tempfile::NamedTempFile;

fn main() -> Result<()> {
    println!("=== Vector Detailed Debug Test ===");
    
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
            text TEXT,
            embedding VECTOR(10)
        )"
    )?;
    
    // Check the schema
    println!("\n2. Checking schema...");
    let schemas = db.get_table_schemas();
    for (table_name, schema) in schemas {
        println!("   Table: {}", table_name);
        for col in &schema.columns {
            println!("     Column: {} - Type: {:?} - Storage: offset={}, size={}, type_code={}", 
                     col.name, col.data_type, col.storage_offset, col.storage_size, col.storage_type_code);
        }
    }
    
    // Try to insert vectors one by one
    println!("\n3. Inserting vectors one by one...");
    
    // First vector
    println!("   Inserting vector 1...");
    match db.execute(
        "INSERT INTO embeddings (id, text, embedding) VALUES (1, 'hello world', [0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9, 1.0])"
    ) {
        Ok(_) => println!("   ✓ Vector 1 inserted successfully"),
        Err(e) => println!("   ✗ Vector 1 failed: {}", e),
    }
    
    // Second vector
    println!("   Inserting vector 2...");
    match db.execute(
        "INSERT INTO embeddings (id, text, embedding) VALUES (2, 'goodbye world', [1.0, 0.9, 0.8, 0.7, 0.6, 0.5, 0.4, 0.3, 0.2, 0.1])"
    ) {
        Ok(_) => println!("   ✓ Vector 2 inserted successfully"),
        Err(e) => println!("   ✗ Vector 2 failed: {}", e),
    }
    
    // Third vector
    println!("   Inserting vector 3...");
    match db.execute(
        "INSERT INTO embeddings (id, text, embedding) VALUES (3, 'test message', [0.5, 0.5, 0.5, 0.5, 0.5, 0.5, 0.5, 0.5, 0.5, 0.5])"
    ) {
        Ok(_) => println!("   ✓ Vector 3 inserted successfully"),
        Err(e) => println!("   ✗ Vector 3 failed: {}", e),
    }
    
    // Query the data
    println!("\n4. Querying data...");
    match db.query("SELECT * FROM embeddings") {
        Ok(results) => {
            println!("   Columns: {:?}", results.columns());
            println!("   Rows:");
            for row in results.rows() {
                println!("     {:?}", row);
            }
        }
        Err(e) => println!("   ✗ Query failed: {}", e),
    }
    
    // Clean up
    let _ = std::fs::remove_file(db_path);
    
    Ok(())
} 