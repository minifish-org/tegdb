use std::time::Instant;
use tegdb::{Database, Result};

fn main() -> Result<()> {
    let mut db = Database::open("query_optimizer_demo.db")?;

    // Create a test table with composite primary key
    db.execute("DROP TABLE IF EXISTS products")?;
    db.execute(
        "CREATE TABLE products (
            category TEXT PRIMARY KEY,
            product_id INTEGER PRIMARY KEY,
            name TEXT NOT NULL,
            price REAL,
            description TEXT
        )",
    )?;

    println!("🏗️  Created products table with composite primary key (category, product_id)");

    // Insert test data
    let categories = ["electronics", "books", "clothing"];
    let mut total_inserted = 0;

    for category in &categories {
        for product_id in 1..=100 {
            let name = format!("{category} Product {product_id}");
            let price = 10.0 + (product_id as f64 * 0.5);
            let description = format!("Description for {category} product {product_id}");

            db.execute(&format!(
                "INSERT INTO products (category, product_id, name, price, description) 
                 VALUES ('{category}', {product_id}, '{name}', {price}, '{description}')"
            ))?;
            total_inserted += 1;
        }
    }

    println!(
        "📦 Inserted {} products across {} categories",
        total_inserted,
        categories.len()
    );

    // Test 1: Optimized query with full primary key equality
    println!("\n🚀 Test 1: Query with complete PK equality (should use PK lookup)");
    let start = Instant::now();
    let result = db
        .query("SELECT * FROM products WHERE category = 'electronics' AND product_id = 42")
        .unwrap();
    let duration = start.elapsed();

    println!("   Query executed in {duration:?}");
    println!(
        "   Found {} rows with {} columns",
        result.rows().len(),
        result.columns().len()
    );
    if !result.rows().is_empty() {
        println!("   Sample row: {:?}", result.rows()[0]);
    }
    assert_eq!(result.rows().len(), 1, "Should find exactly one product");

    // Test 2: Query with partial primary key (should fall back to scan)
    println!("\n📊 Test 2: Query with partial PK (should fall back to table scan)");
    let start = Instant::now();
    let result = db
        .query("SELECT name, price FROM products WHERE category = 'books'")
        .unwrap();
    let duration = start.elapsed();

    println!("   Query executed in {duration:?}");
    println!(
        "   Found {} rows with {} columns",
        result.rows().len(),
        result.columns().len()
    );
    assert_eq!(result.rows().len(), 100, "Should find all books");

    // Test 3: Query with non-PK column (should fall back to scan)
    println!("\n🔍 Test 3: Query with non-PK column (should fall back to table scan)");
    let start = Instant::now();
    let result = db
        .query("SELECT category, product_id, name FROM products WHERE price > 50.0")
        .unwrap();
    let duration = start.elapsed();

    println!("   Query executed in {duration:?}");
    println!("   Found {} expensive products", result.rows().len());

    // Test 4: Complex AND condition with complete PK
    println!("\n⚡ Test 4: Complex AND with complete PK (should use PK lookup)");
    let start = Instant::now();
    let result = db
        .query(
            "SELECT name, price FROM products 
         WHERE product_id = 50 AND category = 'clothing' AND price > 0",
        )
        .unwrap();
    let duration = start.elapsed();

    println!("   Query executed in {duration:?}");
    println!("   Found {} rows", result.rows().len());
    assert_eq!(
        result.rows().len(),
        1,
        "Should find exactly one clothing item"
    );

    println!("\n🎉 Query optimizer demonstration completed successfully!");
    println!("💡 The optimizer automatically chooses between PK lookup and table scan");
    println!("   based on the WHERE clause conditions.");

    // Clean up
    std::fs::remove_file("query_optimizer_demo.db").ok();

    Ok(())
}
