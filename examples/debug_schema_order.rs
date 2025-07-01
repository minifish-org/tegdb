use tegdb::{Database, Result};
use tempfile::NamedTempFile;

fn main() -> Result<()> {
    let temp_file = NamedTempFile::new().expect("Failed to create temp file");
    let db_path = temp_file.path();
    
    let mut db = Database::open(db_path)?;
    
    println!("Creating table...");
    db.execute("CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT NOT NULL, age INTEGER)")?;
    
    // Check the debug information about schemas
    println!("Table schemas loaded in database:");
    
    // We need to access the database internals to debug this
    // Let's check by running some operations and seeing the results
    
    println!("Inserting test data...");
    db.execute("INSERT INTO users (id, name, age) VALUES (1, 'Alice', 30)")?;
    
    println!("Testing explicit column selection...");
    let result1 = db.query("SELECT id FROM users")?.into_query_result()?;
    println!("SELECT id: columns={:?}, rows={:?}", result1.columns(), result1.rows());
    
    let result2 = db.query("SELECT name FROM users")?.into_query_result()?;
    println!("SELECT name: columns={:?}, rows={:?}", result2.columns(), result2.rows());
    
    let result3 = db.query("SELECT age FROM users")?.into_query_result()?;
    println!("SELECT age: columns={:?}, rows={:?}", result3.columns(), result3.rows());
    
    println!("Testing SELECT * ...");
    let result_star = db.query("SELECT * FROM users")?.into_query_result()?;
    println!("SELECT *: columns={:?}, rows={:?}", result_star.columns(), result_star.rows());
    
    Ok(())
}
