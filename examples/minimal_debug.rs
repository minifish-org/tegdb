use tegdb::{Database, Result};
use tempfile::NamedTempFile;

fn main() -> Result<()> {
    let temp_file = NamedTempFile::new().expect("Failed to create temp file");
    let db_path = temp_file.path();

    let mut db = Database::open(format!("file://{}", db_path.display()))?;

    // Create table
    println!("Creating table...");
    db.execute("CREATE TABLE test (id INTEGER PRIMARY KEY, name TEXT)")?;

    // Insert one row
    println!("Inserting first row...");
    let affected = db.execute("INSERT INTO test (id, name) VALUES (1, 'Alice')")?;
    println!("Insert affected: {affected}");

    // Check count
    let result = db.query("SELECT * FROM test").unwrap();
    println!("After first insert - Rows found: {}", result.rows().len());
    for (i, row) in result.rows().iter().enumerate() {
        println!("  Row {i}: {row:?}");
    }

    // Insert second row
    println!("Inserting second row...");
    let affected = db.execute("INSERT INTO test (id, name) VALUES (2, 'Bob')")?;
    println!("Insert affected: {affected}");

    // Check count
    let result = db.query("SELECT * FROM test").unwrap();
    println!("After second insert - Rows found: {}", result.rows().len());
    for (i, row) in result.rows().iter().enumerate() {
        println!("  Row {i}: {row:?}");
    }

    // Update one row
    println!("Updating Alice...");
    let affected = db.execute("UPDATE test SET name = 'Alice Updated' WHERE id = 1")?;
    println!("Update affected: {affected}");

    // Check count and content
    let result = db.query("SELECT * FROM test").unwrap();
    println!("After update - Rows found: {}", result.rows().len());
    for (i, row) in result.rows().iter().enumerate() {
        println!("  Row {i}: {row:?}");
    }

    // Delete one row
    println!("Deleting Bob...");
    let affected = db.execute("DELETE FROM test WHERE id = 2")?;
    println!("Delete affected: {affected}");

    // Final check
    let result = db.query("SELECT * FROM test").unwrap();
    println!("Final - Rows found: {}", result.rows().len());
    for (i, row) in result.rows().iter().enumerate() {
        println!("  Row {i}: {row:?}");
    }

    Ok(())
}
