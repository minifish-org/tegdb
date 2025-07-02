use tegdb::{Database, Result};

fn main() -> Result<()> {
    let db_path = "minimal_test.db";
    let _ = std::fs::remove_file(db_path);

    let mut db = Database::open(db_path)?;

    // Create table
    println!("Creating table...");
    db.execute("CREATE TABLE test (id INTEGER PRIMARY KEY, name TEXT)")?;

    // Insert one row
    println!("Inserting first row...");
    let affected = db.execute("INSERT INTO test (id, name) VALUES (1, 'Alice')")?;
    println!("Insert affected: {affected}");

    // Check count
    let result = db
        .query("SELECT * FROM test")
        .unwrap()
        .into_query_result()
        .unwrap();
    println!("After first insert - Rows found: {}", result.rows().len());
    for (i, row) in result.rows().iter().enumerate() {
        println!("  Row {i}: {row:?}");
    }

    // Insert second row
    println!("Inserting second row...");
    let affected = db.execute("INSERT INTO test (id, name) VALUES (2, 'Bob')")?;
    println!("Insert affected: {affected}");

    // Check count
    let result = db
        .query("SELECT * FROM test")
        .unwrap()
        .into_query_result()
        .unwrap();
    println!("After second insert - Rows found: {}", result.rows().len());
    for (i, row) in result.rows().iter().enumerate() {
        println!("  Row {i}: {row:?}");
    }

    // Update one row
    println!("Updating Alice...");
    let affected = db.execute("UPDATE test SET name = 'Alice Updated' WHERE id = 1")?;
    println!("Update affected: {affected}");

    // Check count and content
    let result = db
        .query("SELECT * FROM test ORDER BY id")
        .unwrap()
        .into_query_result()
        .unwrap();
    println!("After update - Rows found: {}", result.rows().len());
    for (i, row) in result.rows().iter().enumerate() {
        println!("  Row {i}: {row:?}");
    }

    // Delete one row
    println!("Deleting Bob...");
    let affected = db.execute("DELETE FROM test WHERE id = 2")?;
    println!("Delete affected: {affected}");

    // Final check
    let result = db
        .query("SELECT * FROM test")
        .unwrap()
        .into_query_result()
        .unwrap();
    println!("Final - Rows found: {}", result.rows().len());
    for (i, row) in result.rows().iter().enumerate() {
        println!("  Row {i}: {row:?}");
    }

    let _ = std::fs::remove_file(db_path);
    Ok(())
}
