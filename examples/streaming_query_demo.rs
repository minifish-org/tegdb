use tegdb::Database;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Create a temporary database
    let mut db = Database::open("streaming_demo.db")?;

    // Setup test data
    db.execute("CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT, age INTEGER)")?;
    db.execute("INSERT INTO users (id, name, age) VALUES (1, 'Alice', 30)")?;
    db.execute("INSERT INTO users (id, name, age) VALUES (2, 'Bob', 25)")?;
    db.execute("INSERT INTO users (id, name, age) VALUES (3, 'Charlie', 35)")?;
    db.execute("INSERT INTO users (id, name, age) VALUES (4, 'Diana', 28)")?;

    println!("=== Streaming Query Demo ===");

    // Query with iterator API - this is now the default behavior
    let query_iter = db.query("SELECT * FROM users WHERE age > 25").unwrap();

    println!("Columns: {:?}", query_iter.columns());
    println!("Processing rows one by one (streaming):");

    // Iterate through results without loading all into memory
    for (i, row_result) in query_iter.enumerate() {
        let row = row_result?;
        println!("Row {}: {:?}", i + 1, row);

        // Early termination example - stop after 2 rows
        if i >= 1 {
            println!("Early termination after 2 rows!");
            break;
        }
    }

    println!("\n=== Collecting All Results (if needed) ===");

    // If you need all results at once, you can still collect them
    let query_iter = db
        .query("SELECT name, age FROM users ORDER BY age")
        .unwrap();
    let all_rows = query_iter.collect_rows()?;

    println!("All rows collected: {:?}", all_rows);

    println!("\n=== Backward Compatibility ===");

    // For backward compatibility, convert to old QueryResult format
    let query_iter = db
        .query("SELECT * FROM users WHERE name LIKE '%a%'")
        .unwrap();
    let query_result = query_iter.into_query_result()?;

    println!("Using old QueryResult format:");
    println!("Columns: {:?}", query_result.columns());
    println!("Rows: {:?}", query_result.rows());
    println!("Row count: {}", query_result.len());

    // Cleanup
    std::fs::remove_file("streaming_demo.db").ok();

    Ok(())
}
