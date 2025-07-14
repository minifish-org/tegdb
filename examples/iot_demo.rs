use tegdb::{Database, Result};
#[cfg(not(target_arch = "wasm32"))]
use tempfile::NamedTempFile;

fn main() -> Result<()> {
    // Create a temporary database
    #[cfg(not(target_arch = "wasm32"))]
    let temp_file = NamedTempFile::new().expect("Failed to create temp file");
    #[cfg(not(target_arch = "wasm32"))]
    let db_path = temp_file.path();

    #[cfg(not(target_arch = "wasm32"))]
    let mut db = Database::open(format!("file://{}", db_path.display()))?;

    println!("=== IOT (Index-Organized Table) Demo ===");

    // Create table with primary key (required for IOT)
    println!("1. Creating table with PRIMARY KEY constraint...");
    #[cfg(not(target_arch = "wasm32"))]
    db.execute("CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT(32) NOT NULL, age INTEGER)")?;

    // Insert data - primary key becomes the row identifier
    println!("2. Inserting data (primary key becomes row identifier)...");
    #[cfg(not(target_arch = "wasm32"))]
    db.execute("INSERT INTO users (id, name, age) VALUES (1, 'Alice', 30)")?;
    #[cfg(not(target_arch = "wasm32"))]
    db.execute("INSERT INTO users (id, name, age) VALUES (2, 'Bob', 25)")?;
    #[cfg(not(target_arch = "wasm32"))]
    db.execute("INSERT INTO users (id, name, age) VALUES (3, 'Carol', 35)")?;

    // Try to insert duplicate primary key (should fail)
    println!("3. Testing primary key constraint...");
    #[cfg(not(target_arch = "wasm32"))]
    match db.execute("INSERT INTO users (id, name, age) VALUES (1, 'Duplicate', 40)") {
        Ok(_) => println!("ERROR: Duplicate primary key was allowed!"),
        Err(e) => println!("✓ Primary key constraint working: {e}"),
    }

    // Query data (should be efficiently organized by primary key)
    println!("4. Querying data (organized by primary key)...");
    #[cfg(not(target_arch = "wasm32"))]
    {
        let result = db.query("SELECT * FROM users").unwrap();
        println!("Found {} rows:", result.rows().len());

        for row in result.rows() {
            let id = match &row[0] {
                tegdb::SqlValue::Integer(i) => i,
                _ => &0,
            };
            let name = match &row[1] {
                tegdb::SqlValue::Text(s) => s,
                _ => &"Unknown".to_string(),
            };
            let age = match &row[2] {
                tegdb::SqlValue::Integer(i) => i,
                _ => &0,
            };

            println!("  ID: {id}, Name: {name}, Age: {age}");
        }
    }

    // Test table without primary key (should fail)
    println!("5. Testing table creation without PRIMARY KEY...");
    #[cfg(not(target_arch = "wasm32"))]
    match db.execute("CREATE TABLE invalid_table (name TEXT(32), value TEXT(32))") {
        Ok(_) => println!("ERROR: Table without primary key was allowed!"),
        Err(e) => println!("✓ Primary key requirement enforced: {e}"),
    }

    println!("\n=== IOT Benefits ===");
    println!("• Data is physically organized by primary key");
    println!("• No artificial row IDs needed");
    println!("• Primary key uniqueness automatically enforced");
    println!("• Efficient range scans by primary key");
    println!("• SQL standard compliance");

    Ok(())
}
