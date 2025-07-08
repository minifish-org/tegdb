use tegdb::Database;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Test the improved executor with validation
    let mut path = std::env::temp_dir();
    path.push("executor_test.db");

    // Remove existing file
    let _ = std::fs::remove_file(&path);

    let mut db = Database::open(&format!("file://{}", path.display()))?;

    println!("Testing improved executor with validation...");

    // Test 1: Create table (Database handles transactions automatically)
    let result = db.execute(
        "CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT NOT NULL, email TEXT UNIQUE)",
    )?;
    println!("Create table result: {result} rows affected");

    // Test 2: Insert with validation
    let result =
        db.execute("INSERT INTO users (id, name, email) VALUES (1, 'Alice', 'alice@example.com')")?;
    println!("Insert result: {result} rows affected");

    // Test 3: Try to insert duplicate primary key (should fail)
    match db.execute("INSERT INTO users (id, name, email) VALUES (1, 'Bob', 'bob@example.com')") {
        Ok(_) => println!("ERROR: Duplicate primary key should have failed!"),
        Err(e) => println!("✓ Primary key constraint validation works: {e}"),
    }

    // Test 4: Try to insert missing NOT NULL field (should fail)
    match db.execute("INSERT INTO users (id, email) VALUES (2, 'charlie@example.com')") {
        Ok(_) => println!("ERROR: Missing NOT NULL field should have failed!"),
        Err(e) => println!("✓ NOT NULL constraint validation works: {e}"),
    }

    // Test 5: Try to insert duplicate UNIQUE field (should fail)
    match db.execute("INSERT INTO users (id, name, email) VALUES (3, 'David', 'alice@example.com')")
    {
        Ok(_) => println!("ERROR: Duplicate UNIQUE field should have failed!"),
        Err(e) => println!("✓ UNIQUE constraint validation works: {e}"),
    }

    // Test 6: Valid insert
    let result =
        db.execute("INSERT INTO users (id, name, email) VALUES (2, 'Bob', 'bob@example.com')")?;
    println!("Valid insert result: {result} rows affected");

    // Test 7: Query to verify data
    let result = db.query("SELECT * FROM users").unwrap();
    println!("Query result: {} rows returned", result.rows().len());
    for (i, row_data) in result.rows().iter().enumerate() {
        println!("  Row {}: {:?}", i + 1, row_data);
    }

    // Test 8: Select with LIMIT (memory optimization)
    let result = db.query("SELECT * FROM users LIMIT 1").unwrap();
    println!(
        "Select with LIMIT result: {} rows returned",
        result.rows().len()
    );

    println!("All tests completed successfully!");

    // Cleanup
    let _ = std::fs::remove_file(&path);

    Ok(())
}
