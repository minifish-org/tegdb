use tegdb::{Database, Result};

#[cfg(not(target_arch = "wasm32"))]
use tempfile::NamedTempFile;

fn main() -> Result<()> {
    #[cfg(not(target_arch = "wasm32"))]
    let temp_file = NamedTempFile::new().expect("Failed to create temp file");
    #[cfg(not(target_arch = "wasm32"))]
    let db_path = temp_file.path();

    #[cfg(not(target_arch = "wasm32"))]
    let mut db = Database::open(&format!("file://{}", db_path.display()))?;

    #[cfg(not(target_arch = "wasm32"))]
    println!("Creating table...");
    #[cfg(not(target_arch = "wasm32"))]
    db.execute("CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT NOT NULL, age INTEGER)")?;

    // Check the debug information about schemas
    #[cfg(not(target_arch = "wasm32"))]
    println!("Table schemas loaded in database:");

    // We need to access the database internals to debug this
    // Let's check by running some operations and seeing the results

    #[cfg(not(target_arch = "wasm32"))]
    println!("Inserting test data...");
    #[cfg(not(target_arch = "wasm32"))]
    db.execute("INSERT INTO users (id, name, age) VALUES (1, 'Alice', 30)")?;

    #[cfg(not(target_arch = "wasm32"))]
    println!("Testing explicit column selection...");
    #[cfg(not(target_arch = "wasm32"))]
    let result1 = db.query("SELECT id FROM users")?;
    #[cfg(not(target_arch = "wasm32"))]
    println!(
        "SELECT id: columns={:?}, rows={:?}",
        result1.columns(),
        result1.rows()
    );

    #[cfg(not(target_arch = "wasm32"))]
    let result2 = db.query("SELECT name FROM users")?;
    #[cfg(not(target_arch = "wasm32"))]
    println!(
        "SELECT name: columns={:?}, rows={:?}",
        result2.columns(),
        result2.rows()
    );

    #[cfg(not(target_arch = "wasm32"))]
    let result3 = db.query("SELECT age FROM users")?;
    #[cfg(not(target_arch = "wasm32"))]
    println!(
        "SELECT age: columns={:?}, rows={:?}",
        result3.columns(),
        result3.rows()
    );

    #[cfg(not(target_arch = "wasm32"))]
    println!("Testing SELECT * ...");
    #[cfg(not(target_arch = "wasm32"))]
    let result_star = db.query("SELECT * FROM users")?;
    #[cfg(not(target_arch = "wasm32"))]
    println!(
        "SELECT *: columns={:?}, rows={:?}",
        result_star.columns(),
        result_star.rows()
    );

    Ok(())
}
