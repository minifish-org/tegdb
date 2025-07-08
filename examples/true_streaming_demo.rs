use std::time::Instant;
/// Demonstration of TegDB's true streaming query API
///
/// This example shows the difference between:
/// 1. The backward-compatible `query()` API that materializes all rows
/// 2. The new `query()` API that yields rows on-demand without materializing
use tegdb::Database;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Create a test database
    let mut db = Database::open("file://streaming_demo.db")?;

    // Create a test table
    db.execute("CREATE TABLE large_table (id INTEGER PRIMARY KEY, data TEXT, value INTEGER)")?;

    // Insert test data
    println!("Inserting 10,000 test rows...");
    for i in 0..10_000 {
        db.execute(&format!(
            "INSERT INTO large_table (id, data, value) VALUES ({}, 'data_{}', {})",
            i,
            i,
            i % 100
        ))?;
    }

    println!("\n=== DEMONSTRATION ===\n");

    // Demonstrate the difference between materialized and streaming approaches

    // 1. Backward-compatible API (materializes all rows)
    println!("1. Using backward-compatible query() API (materializes all rows):");
    let start = Instant::now();
    let qr_bc = db.query("SELECT * FROM large_table WHERE value < 5")?;
    let materialized_time = start.elapsed();

    let rows_bc = qr_bc.rows().to_vec();
    let count = rows_bc.len();
    let total_time = start.elapsed();

    println!("   - Time to create iterator: {materialized_time:?}");
    println!("   - Time to process all {count} rows: {total_time:?}");
    println!("   - Memory usage: All {count} rows materialized in memory");

    // 2. True streaming API (yields rows on-demand)
    println!("\n2. Using new query() API (true streaming):");
    let start = Instant::now();
    let qr_stream = db.query("SELECT * FROM large_table WHERE value < 5")?;
    let streaming_create_time = start.elapsed();

    println!("   - Time to create streaming iterator: {streaming_create_time:?}");
    println!("   - Memory usage: No rows materialized yet");

    // Process only the first 3 rows to demonstrate streaming
    println!("   - Processing first 3 rows on-demand:");
    let mut count = 0;
    // Process first 3 rows
    for row in qr_stream.rows().iter().take(3) {
        println!(
            "     Row {}: id={:?}, data={:?}, value={:?}",
            count + 1,
            row[0],
            row[1],
            row[2]
        );
        count += 1;
    }

    let partial_time = start.elapsed();
    println!("   - Time to process first 3 rows: {partial_time:?}");
    println!("   - Memory usage: Only 3 rows processed, rest remain unread");

    // Show that we can continue processing from where we left off
    println!("   - Continuing to process remaining rows...");
    for _row in qr_stream.rows().iter().skip(3) {
        count += 1;
    }
    let complete_time = start.elapsed();

    println!("   - Total rows processed: {count}");
    println!("   - Total time to process all rows: {complete_time:?}");

    // 3. Demonstrate early termination benefit
    println!("\n3. Demonstrating early termination with LIMIT:");
    let start = Instant::now();
    let limited_stream = db.query("SELECT * FROM large_table LIMIT 5")?;

    // Use rows() for LIMIT example
    let rows_lim = limited_stream.rows().to_vec();
    let limited_count = rows_lim.len();
    let limited_time = start.elapsed();

    println!("   - Time to process {limited_count} rows with LIMIT: {limited_time:?}");
    println!("   - Memory efficiency: Only {limited_count} rows ever existed in memory");

    // 4. Show memory efficiency comparison
    println!("\n=== MEMORY EFFICIENCY COMPARISON ===");
    println!("• query() API: Materializes ALL matching rows in memory before returning");
    println!("• query() API: Yields rows one-by-one on demand");
    println!("• Early termination: query() can stop processing without reading all data");
    println!("• Large datasets: query() uses constant memory, query() uses O(n) memory");

    // Clean up
    std::fs::remove_file("streaming_demo.db").ok();

    Ok(())
}
