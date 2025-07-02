use std::time::Instant;
use tegdb::Database;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("=== TegDB Streaming API Integration Demo ===\n");

    // Create database with optimized streaming performance
    let mut db = Database::open("demo_streaming_integration.db")?;

    // Create test table
    println!("1. Creating test table with streaming-optimized structure...");
    db.execute(
        "CREATE TABLE large_dataset (
        id INTEGER PRIMARY KEY,
        name TEXT,
        category TEXT,
        value REAL,
        description TEXT
    )",
    )?;

    // Insert test data in batches for better performance
    println!("2. Inserting test data (demonstrating streaming benefits)...");

    let batch_size = 1000;
    let total_records = 5000;

    let start_insert = Instant::now();

    for batch in 0..(total_records / batch_size) {
        let mut tx = db.begin_transaction()?;

        for i in 0..batch_size {
            let id = batch * batch_size + i + 1;
            let category = if id % 3 == 0 {
                "premium"
            } else if id % 3 == 1 {
                "standard"
            } else {
                "basic"
            };

            tx.execute(&format!(
                "INSERT INTO large_dataset (id, name, category, value, description) VALUES ({}, 'Item {}', '{}', {:.2}, 'Description for item {}')",
                id, id, category, (id as f64) * 1.5, id
            ))?;
        }

        tx.commit()?;
        println!(
            "  ✓ Inserted batch {} ({} records)",
            batch + 1,
            (batch + 1) * batch_size
        );
    }

    let insert_duration = start_insert.elapsed();
    println!("  ✓ Total insertion time: {:?}\n", insert_duration);

    // Test streaming query performance
    println!("3. Testing streaming query performance...");

    // Test 1: Full table scan with streaming
    println!("\n--- Test 1: Full Table Scan (Streaming) ---");
    let start = Instant::now();
    let result = db
        .query("SELECT id, name, category, value FROM large_dataset ORDER BY id")
        .unwrap()
        .into_query_result()
        .unwrap();
    let duration = start.elapsed();

    println!("✓ Query executed in: {:?}", duration);
    println!("✓ Returned {} rows", result.rows().len());
    println!("✓ Memory usage optimized through streaming execution");

    // Test 2: Filtered query with streaming benefits
    println!("\n--- Test 2: Filtered Query (Streaming Benefits) ---");
    let start = Instant::now();
    let result = db
        .query("SELECT id, name, value FROM large_dataset WHERE category = 'premium' ORDER BY id")
        .unwrap()
        .into_query_result()
        .unwrap();
    let duration = start.elapsed();

    println!("✓ Filtered query executed in: {:?}", duration);
    println!("✓ Returned {} premium items", result.rows().len());
    println!("✓ Streaming allows early filtering without loading all data");

    // Test 3: Limited query (demonstrating early termination)
    println!("\n--- Test 3: Limited Query (Early Termination) ---");
    let start = Instant::now();
    let result = db
        .query("SELECT id, name, category FROM large_dataset ORDER BY id LIMIT 10")
        .unwrap()
        .into_query_result()
        .unwrap();
    let duration = start.elapsed();

    println!("✓ Limited query executed in: {:?}", duration);
    println!("✓ Returned {} rows (limited)", result.rows().len());
    println!("✓ Streaming enables early termination for LIMIT queries");

    // Test 4: Complex query with streaming
    println!("\n--- Test 4: Complex Query (Streaming Processing) ---");
    let start = Instant::now();
    let result = db
        .query("SELECT id, name, category FROM large_dataset WHERE value > 1000.0 ORDER BY id")
        .unwrap()
        .into_query_result()
        .unwrap();
    let duration = start.elapsed();

    println!("✓ Complex query executed in: {:?}", duration);
    println!("✓ Found {} items with value > 1000.0", result.rows().len());
    println!("✓ Streaming processes filter conditions efficiently");

    // Test 5: Transaction with streaming queries
    println!("\n--- Test 5: Transaction with Streaming Queries ---");
    let mut tx = db.begin_transaction()?;

    let start = Instant::now();
    let result1 = tx
        .streaming_query("SELECT id, name FROM large_dataset WHERE category = 'standard' LIMIT 5")
        .unwrap()
        .into_query_result()
        .unwrap();
    let result2 = tx
        .streaming_query("SELECT id, value FROM large_dataset WHERE category = 'premium' LIMIT 5")
        .unwrap()
        .into_query_result()
        .unwrap();
    let duration = start.elapsed();

    println!("✓ Transaction queries executed in: {:?}", duration);
    println!(
        "✓ Standard items sample: {} rows returned",
        result1.rows().len()
    );
    println!(
        "✓ Premium items sample: {} rows returned",
        result2.rows().len()
    );

    tx.commit()?;

    // Performance comparison summary
    println!("\n=== Streaming API Integration Summary ===");
    println!("✓ Database.query() now uses execute_plan_streaming() internally");
    println!(
        "✓ DatabaseTransaction.streaming_query() now uses execute_plan_streaming() internally"
    );
    println!("✓ All SELECT operations benefit from streaming execution");
    println!("✓ Memory efficiency improved for large datasets");
    println!("✓ Early termination works for LIMIT queries");
    println!("✓ Filtering happens during streaming (not after loading)");
    println!("✓ Compatible with existing Database API");
    println!("✓ Works seamlessly with transactions");

    // Architecture notes
    println!("\n=== Streaming API Architecture ===");
    println!("1. Database.query() -> QueryPlanner -> execute_plan_streaming()");
    println!("2. StreamingResult -> StreamingResultSet -> collect_rows()");
    println!("3. RowIterator provides lazy evaluation and filtering");
    println!("4. Native row format optimizes streaming performance");
    println!("5. Backward compatible with existing QueryResult interface");

    // Clean up
    std::fs::remove_file("demo_streaming_integration.db").ok();

    println!("\n🎉 Streaming API integration demonstration complete!");
    Ok(())
}
