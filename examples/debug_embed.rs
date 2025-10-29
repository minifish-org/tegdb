//! Debug EMBED parsing issues

use tegdb::{Database, Result};

fn main() -> Result<()> {
    let temp_dir = tempfile::tempdir().unwrap();
    let db_path = temp_dir.path().join("debug").with_extension("teg");
    let db_file = format!("file://{}", db_path.display());

    let mut db = Database::open(&db_file)?;

    // Create table
    println!("Creating table...");
    db.execute("CREATE TABLE test (id INTEGER PRIMARY KEY, text TEXT(128), vec VECTOR(128))")?;
    println!("✓ Table created\n");

    // Test 1: Simple INSERT without EMBED
    println!("Test 1: INSERT without functions");
    let sql1 = "INSERT INTO test (id, text) VALUES (1, 'hello')";
    println!("SQL: {sql1}");
    match db.execute(sql1) {
        Ok(n) => println!("✓ Success: {n} rows\n"),
        Err(e) => println!("✗ Error: {e:?}\n"),
    }

    // Test 2: INSERT with a simple function (ABS)
    println!("Test 2: INSERT with ABS function");
    let sql2 = "INSERT INTO test (id, text) VALUES (ABS(-2), 'test')";
    println!("SQL: {sql2}");
    match db.execute(sql2) {
        Ok(n) => println!("✓ Success: {n} rows\n"),
        Err(e) => println!("✗ Error: {e:?}\n"),
    }

    // Test 3: SELECT with ABS function
    println!("Test 3: SELECT with ABS function");
    let sql3 = "SELECT ABS(-5) as result";
    println!("SQL: {sql3}");
    match db.query(sql3) {
        Ok(r) => println!("✓ Success: {} rows\n", r.rows().len()),
        Err(e) => println!("✗ Error: {e:?}\n"),
    }

    // Test 4: Now try EMBED in SELECT
    println!("Test 4: SELECT with EMBED function");
    let sql4 = "SELECT EMBED('hello world') as embedding";
    println!("SQL: {sql4}");
    println!("SQL length: {}", sql4.len());
    println!("SQL bytes: {:?}", sql4.as_bytes());
    match db.query(sql4) {
        Ok(r) => println!("✓ Success: {} rows\n", r.rows().len()),
        Err(e) => println!("✗ Error: {e:?}\n"),
    }

    Ok(())
}
