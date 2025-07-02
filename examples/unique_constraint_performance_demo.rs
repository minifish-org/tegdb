use std::time::Instant;
use tegdb::{Database, Result};

fn main() -> Result<()> {
    println!("=== Unique Constraint Performance Demo ===\n");

    // Create a temporary database
    let temp_dir = std::env::temp_dir();
    let db_path = temp_dir.join("unique_performance_demo.db");

    // Clean up any existing file
    let _ = std::fs::remove_file(&db_path);

    let mut db = Database::open(&db_path)?;

    // Create table with a unique column
    println!("1. Creating table with UNIQUE constraint...");
    db.execute("CREATE TABLE users (id INTEGER PRIMARY KEY, email TEXT UNIQUE, name TEXT)")?;

    // Insert test data
    println!("2. Inserting test data...");
    let start_time = Instant::now();

    for i in 1..=1000 {
        let insert_sql = format!(
            "INSERT INTO users (id, email, name) VALUES ({}, 'user{}@example.com', 'User {}')",
            i, i, i
        );
        db.execute(&insert_sql)?;
    }

    let insert_time = start_time.elapsed();
    println!("   ✅ Inserted 1000 rows in {:?}", insert_time);

    // Test unique constraint violation detection speed
    println!("3. Testing unique constraint violation detection...");
    let start_time = Instant::now();

    // Try to insert a duplicate unique value
    match db.execute(
        "INSERT INTO users (id, email, name) VALUES (1001, 'user500@example.com', 'Duplicate')",
    ) {
        Ok(_) => println!("   ❌ ERROR: Unique constraint violation not detected!"),
        Err(e) => {
            let violation_check_time = start_time.elapsed();
            println!(
                "   ✅ Unique constraint violation detected in {:?}",
                violation_check_time
            );
            println!("   ✅ Error: {}", e);
        }
    }

    // Test update with unique constraint
    println!("4. Testing update with unique constraint...");
    let start_time = Instant::now();

    match db.execute("UPDATE users SET email = 'user999@example.com' WHERE id = 1") {
        Ok(_) => println!("   ❌ ERROR: Update with unique constraint violation not detected!"),
        Err(e) => {
            let update_violation_time = start_time.elapsed();
            println!(
                "   ✅ Update unique constraint violation detected in {:?}",
                update_violation_time
            );
            println!("   ✅ Error: {}", e);
        }
    }

    // Test successful update
    println!("5. Testing successful update...");
    let start_time = Instant::now();

    match db.execute("UPDATE users SET email = 'updated@example.com' WHERE id = 1") {
        Ok(_) => {
            let successful_update_time = start_time.elapsed();
            println!(
                "   ✅ Successful update completed in {:?}",
                successful_update_time
            );
        }
        Err(e) => println!("   ❌ ERROR: Successful update failed: {}", e),
    }

    // Verify the update worked
    let result = db
        .query("SELECT id, email FROM users WHERE id = 1")?
        .into_query_result()?;
    if let Some(row) = result.rows().first() {
        println!("   ✅ Verified: User 1 email is now: {:?}", &row[1]);
    }

    // Clean up
    let _ = std::fs::remove_file(&db_path);

    println!("\n🚀 Performance Benefits:");
    println!("   ✅ O(1) unique constraint checking using secondary indexes");
    println!("   ✅ No more O(n) table scans for unique validation");
    println!("   ✅ Efficient unique constraint maintenance during updates");
    println!("   ✅ Fast constraint violation detection");

    Ok(())
}
