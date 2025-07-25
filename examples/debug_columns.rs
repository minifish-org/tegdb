use tegdb::{Database, Result};
use tempfile::NamedTempFile;

fn main() -> Result<()> {
    let temp_file = NamedTempFile::new().expect("Failed to create temp file");
    let db_path = temp_file.path();

    let mut db = Database::open(format!("file://{}", db_path.display()))?;

    println!("Creating table...");
    db.execute("CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT(32) NOT NULL, age INTEGER)")?;

    println!("Inserting data...");
    db.execute("INSERT INTO users (id, name, age) VALUES (1, 'Alice', 30)")?;

    println!("Querying with explicit columns...");
    let result = db.query("SELECT id, name, age FROM users").unwrap();
    println!("Columns: {:?}", result.columns());
    for row in result.rows().iter() {
        println!("Row: {row:?}");
    }

    println!("Querying with SELECT *...");
    let result = db.query("SELECT * FROM users").unwrap();
    println!("Columns: {:?}", result.columns());
    for row in result.rows().iter() {
        println!("Row: {row:?}");
    }

    Ok(())
}
