use tegdb::Database;
use tegdb::SqlValue;

/// Demonstrates the new fixed-length storage format
/// 
/// This example shows how the new format provides:
/// 1. Predictable record sizes
/// 2. Ultra-fast column access
/// 3. Zero-copy deserialization
/// 4. Maximum cache efficiency
fn main() -> tegdb::Result<()> {
    println!("=== TegDB Fixed-Length Storage Format Demo ===\n");

    // Create a database with fixed-length columns
    let mut db = Database::open("file:///tmp/fixed_length_demo.db")?;
    
    // Clean up any existing data
    let _ = db.execute("DROP TABLE IF EXISTS users");
    let _ = db.execute("DROP TABLE IF EXISTS products");

    // Create tables with fixed-length columns
    println!("1. Creating tables with fixed-length columns:");
    println!("   - TEXT(50) for names (50 bytes fixed)");
    println!("   - TEXT(100) for emails (100 bytes fixed)");
    println!("   - TEXT(256) for avatars (256 bytes fixed)");
    println!();

    db.execute(
        "CREATE TABLE users (
            id INTEGER PRIMARY KEY,
            name TEXT(50),
            email TEXT(100),
            age INTEGER,
            score REAL,
            avatar TEXT(256)
        )"
    )?;

    db.execute(
        "CREATE TABLE products (
            id INTEGER PRIMARY KEY,
            name TEXT(100),
            description TEXT(200),
            price REAL,
            category TEXT(50),
            image_data TEXT(512)
        )"
    )?;

    println!("2. Inserting sample data...");
    
    // Insert users with fixed-length data
    for i in 1..=5 {
        let name = format!("User{}", i);
        let email = format!("user{}@example.com", i);
        let avatar = format!("avatar_data_for_user_{}", i);
        
        db.execute(&format!(
            "INSERT INTO users (id, name, email, age, score, avatar) 
             VALUES ({}, '{}', '{}', {}, {}, '{}')",
            i,
            name,
            email,
            20 + (i * 5),
            50.0 + (i as f64 * 10.5),
            avatar
        ))?;
    }

    // Insert products with fixed-length data
    for i in 1..=3 {
        let name = format!("Product{}", i);
        let description = format!("This is a detailed description for product number {}", i);
        let category = format!("Category{}", i);
        let image_data = format!("binary_image_data_for_product_{}_with_some_padding", i);
        
        db.execute(&format!(
            "INSERT INTO products (id, name, description, price, category, image_data) 
             VALUES ({}, '{}', '{}', {}, '{}', '{}')",
            i,
            name,
            description,
            10.0 + (i as f64 * 25.5),
            category,
            image_data
        ))?;
    }

    println!("3. Querying data with the new format...\n");

    // Query all users
    let users_result = db.query("SELECT * FROM users")?;
    println!("Users table:");
    println!("{:<5} {:<10} {:<25} {:<5} {:<8} {:<20}", "ID", "Name", "Email", "Age", "Score", "Avatar");
    println!("{:-<80}", "");
    
    for row in users_result.rows() {
        // Access columns by index: id=0, name=1, email=2, age=3, score=4, avatar=5
        let id = row.get(0).unwrap_or(&SqlValue::Null);
        let name = row.get(1).unwrap_or(&SqlValue::Null);
        let email = row.get(2).unwrap_or(&SqlValue::Null);
        let age = row.get(3).unwrap_or(&SqlValue::Null);
        let score = row.get(4).unwrap_or(&SqlValue::Null);
        let avatar = row.get(5).unwrap_or(&SqlValue::Null);
        
        println!(
            "{:<5} {:<10} {:<25} {:<5} {:<8} {:<20}",
            format!("{:?}", id),
            format!("{:?}", name),
            format!("{:?}", email),
            format!("{:?}", age),
            format!("{:?}", score),
            format!("{:?}", avatar)
        );
    }

    println!("\n4. Demonstrating partial column access (LIMIT optimization)...");
    
    // Query only specific columns - this is much faster with fixed-length format
    let partial_result = db.query("SELECT id, name, score FROM users WHERE score > 60")?;
    println!("Users with score > 60 (partial columns):");
    for row in partial_result.rows() {
        // Access columns by index: id=0, name=1, score=2 (for SELECT id, name, score)
        let id = row.get(0).unwrap_or(&SqlValue::Null);
        let name = row.get(1).unwrap_or(&SqlValue::Null);
        let score = row.get(2).unwrap_or(&SqlValue::Null);
        println!("  ID: {:?}, Name: {:?}, Score: {:?}", id, name, score);
    }

    println!("\n5. Performance comparison...");
    
    // Benchmark the new format
    let start = std::time::Instant::now();
    
    // Query all data multiple times to simulate real usage
    for _ in 0..100 {
        let _result = db.query("SELECT * FROM users")?;
    }
    
    let duration = start.elapsed();
    println!("   Query time for 100 full table scans: {:?}", duration);
    println!("   Average per query: {:?}", duration / 100);

    // Benchmark partial column access
    let start = std::time::Instant::now();
    
    for _ in 0..100 {
        let _result = db.query("SELECT id, name FROM users")?;
    }
    
    let duration = start.elapsed();
    println!("   Query time for 100 partial column scans: {:?}", duration);
    println!("   Average per query: {:?}", duration / 100);

    println!("\n6. Storage format benefits:");
    println!("   ✓ Predictable record sizes (no variable-length encoding)");
    println!("   ✓ Direct offset-based column access");
    println!("   ✓ Zero-copy deserialization for fixed-length types");
    println!("   ✓ Maximum cache efficiency");
    println!("   ✓ No header parsing overhead");
    println!("   ✓ No varint decoding for text/blob columns");

    println!("\n7. Record layout example:");
    println!("   Users table record size: 430 bytes");
    println!("   - id (INTEGER): 8 bytes at offset 0");
    println!("   - name (TEXT(50)): 50 bytes at offset 8");
    println!("   - email (TEXT(100)): 100 bytes at offset 58");
    println!("   - age (INTEGER): 8 bytes at offset 158");
    println!("   - score (REAL): 8 bytes at offset 166");
    println!("   - avatar (TEXT(256)): 256 bytes at offset 174");
    println!("   Total: 430 bytes (always the same!)");

    println!("\n=== Demo Complete ===");
    println!("The new fixed-length format provides:");
    println!("• Nanosecond-level column access");
    println!("• Predictable memory usage");
    println!("• Maximum performance for analytical queries");
    println!("• Cache-friendly data layout");

    Ok(())
} 