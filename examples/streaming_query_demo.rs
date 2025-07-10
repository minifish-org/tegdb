use tegdb::Database;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Create a temporary database
    let mut db = Database::open("file://streaming_demo.db")?;

    // Setup test data
    db.execute("CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT, age INTEGER)")?;
    db.execute("INSERT INTO users (id, name, age) VALUES (1, 'Alice', 30)")?;
    db.execute("INSERT INTO users (id, name, age) VALUES (2, 'Bob', 25)")?;
    db.execute("INSERT INTO users (id, name, age) VALUES (3, 'Charlie', 35)")?;
    db.execute("INSERT INTO users (id, name, age) VALUES (4, 'Diana', 28)")?;

    println!("=== Streaming Query Demo ===");

    // Query and process rows one by one
    let qr = db.query("SELECT * FROM users WHERE age > 25").unwrap();
    println!("Columns: {:?}", qr.columns());
    println!("Processing rows one by one (streaming simulation):");
    for (i, row) in qr.rows().iter().enumerate() {
        println!("Row {}: {:?}", i + 1, row);
        // Early termination example - stop after 2 rows
        if i >= 1 {
            println!("Early termination after 2 rows!");
            break;
        }
    }

    println!("\n=== Collecting All Results (if needed) ===");

    // If you need all results at once, you can use rows()
    let qr_all = db
        .query("SELECT name, age FROM users")
        .unwrap();
    let all_rows = qr_all.rows().to_vec();

    println!("All rows collected: {all_rows:?}");

    println!("\n=== Backward Compatibility ===");

    // The QueryResult itself still provides columns() and rows()
    let query_result = db
        .query("SELECT * FROM users WHERE name LIKE '%a%'")
        .unwrap();

    println!("Using QueryResult format:");
    println!("Columns: {:?}", query_result.columns());
    println!("Rows: {:?}", query_result.rows());
    println!("Row count: {}", query_result.len());

    // Cleanup
    std::fs::remove_file("streaming_demo.db").ok();

    Ok(())
}
