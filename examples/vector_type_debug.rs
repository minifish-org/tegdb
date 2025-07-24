use tegdb::{Database, Result};
use tempfile::NamedTempFile;

fn main() -> Result<()> {
    println!("=== Vector Type Debug Test ===");
    
    // Create a temporary file for the database
    let temp_file = NamedTempFile::new().expect("Failed to create temp file");
    let db_path = temp_file.path();
    
    // Create a new database
    let mut db = Database::open(format!("file://{}", db_path.display()))?;
    
    // Create a table with vector column
    println!("\n1. Creating table with vector column...");
    db.execute(
        "CREATE TABLE test (id INTEGER PRIMARY KEY, vec VECTOR(3))"
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
    
    // Try to insert a vector
    println!("\n3. Inserting vector...");
    match db.execute("INSERT INTO test (id, vec) VALUES (1, [1.0, 2.0, 3.0])") {
        Ok(_) => println!("   ✓ Insert successful"),
        Err(e) => println!("   ✗ Insert failed: {}", e),
    }
    
    // Clean up
    let _ = std::fs::remove_file(db_path);
    
    Ok(())
} 