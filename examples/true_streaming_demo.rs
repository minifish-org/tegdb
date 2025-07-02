use std::time::Instant;
/// Demonstration of TegDB's true streaming query API
///
/// This example shows the difference between:
/// 1. The backward-compatible `query()` API that materializes all rows
/// 2. The new `query()` API that yields rows on-demand without materializing
use tegdb::Database;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Create a test database
    let mut db = Database::open("streaming_demo.db")?;

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
    let result_iter = db
        .query("SELECT * FROM large_table WHERE value < 5")
        .unwrap();
    let materialized_time = start.elapsed();

    let count = result_iter.collect::<Result<Vec<_>, _>>()?.len();
    let total_time = start.elapsed();

    println!("   - Time to create iterator: {:?}", materialized_time);
    println!("   - Time to process all {} rows: {:?}", count, total_time);
    println!(
        "   - Memory usage: All {} rows materialized in memory",
        count
    );

    // 2. True streaming API (yields rows on-demand)
    println!("\n2. Using new query() API (true streaming):");
    let start = Instant::now();
    let mut streaming_result = db.query("SELECT * FROM large_table WHERE value < 5")?;
    let streaming_create_time = start.elapsed();

    println!(
        "   - Time to create streaming iterator: {:?}",
        streaming_create_time
    );
    println!("   - Memory usage: No rows materialized yet");

    // Process only the first 3 rows to demonstrate streaming
    println!("   - Processing first 3 rows on-demand:");
    let mut count = 0;
    for row in streaming_result.by_ref() {
        let row = row?;
        println!(
            "     Row {}: id={:?}, data={:?}, value={:?}",
            count + 1,
            row[0],
            row[1],
            row[2]
        );
        count += 1;
        if count >= 3 {
            break;
        }
    }

    let partial_time = start.elapsed();
    println!("   - Time to process first 3 rows: {:?}", partial_time);
    println!("   - Memory usage: Only 3 rows processed, rest remain unread");

    // Show that we can continue processing from where we left off
    println!("   - Continuing to process remaining rows...");
    for _row in streaming_result {
        count += 1;
    }
    let complete_time = start.elapsed();

    println!("   - Total rows processed: {}", count);
    println!("   - Total time to process all rows: {:?}", complete_time);

    // 3. Demonstrate early termination benefit
    println!("\n3. Demonstrating early termination with LIMIT:");
    let start = Instant::now();
    let limited_stream = db.query("SELECT * FROM large_table LIMIT 5")?;

    let mut limited_count = 0;
    for row in limited_stream {
        let row = row?;
        println!("   Limited Row {}: id={:?}", limited_count + 1, row[0]);
        limited_count += 1;
    }
    let limited_time = start.elapsed();

    println!(
        "   - Time to process {} rows with LIMIT: {:?}",
        limited_count, limited_time
    );
    println!(
        "   - Memory efficiency: Only {} rows ever existed in memory",
        limited_count
    );

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
