use tegdb::{Database, Result};
use tempfile::NamedTempFile;

fn main() -> Result<()> {
    // Create a temporary database
    let temp_file = NamedTempFile::new().expect("Failed to create temp file");
    let db_path = temp_file.path();

    let mut db = Database::open(db_path)?;

    println!("=== Composite Primary Key IOT Demo ===");

    // Create table with composite primary key
    println!("1. Creating table with composite PRIMARY KEY...");
    db.execute("CREATE TABLE order_items (order_id INTEGER PRIMARY KEY, product_id INTEGER PRIMARY KEY, quantity INTEGER, price REAL)")?;

    // Insert data with composite primary keys
    println!("2. Inserting data with composite primary keys...");
    db.execute(
        "INSERT INTO order_items (order_id, product_id, quantity, price) VALUES (100, 1, 2, 29.99)",
    )?;
    db.execute(
        "INSERT INTO order_items (order_id, product_id, quantity, price) VALUES (100, 2, 1, 15.50)",
    )?;
    db.execute(
        "INSERT INTO order_items (order_id, product_id, quantity, price) VALUES (101, 1, 3, 29.99)",
    )?;
    db.execute(
        "INSERT INTO order_items (order_id, product_id, quantity, price) VALUES (101, 3, 1, 45.00)",
    )?;

    // Try to insert duplicate composite primary key
    println!("3. Testing composite primary key constraint...");
    match db.execute(
        "INSERT INTO order_items (order_id, product_id, quantity, price) VALUES (100, 1, 5, 35.00)",
    ) {
        Ok(_) => println!("ERROR: Duplicate composite primary key was allowed!"),
        Err(e) => println!("✓ Composite primary key constraint working: {}", e),
    }

    // Query all data
    println!("4. Querying all order items (organized by composite PK)...");
    let result = db.query("SELECT * FROM order_items")?.into_query_result()?;
    println!("Found {} order items:", result.rows().len());

    for row in result.rows() {
        let order_id = match &row[0] {
            tegdb::SqlValue::Integer(i) => i,
            _ => &0,
        };
        let product_id = match &row[1] {
            tegdb::SqlValue::Integer(i) => i,
            _ => &0,
        };
        let quantity = match &row[2] {
            tegdb::SqlValue::Integer(i) => i,
            _ => &0,
        };
        let price = match &row[3] {
            tegdb::SqlValue::Real(r) => *r,
            _ => 0.0,
        };

        println!(
            "  Order: {}, Product: {}, Qty: {}, Price: ${:.2}",
            order_id, product_id, quantity, price
        );
    }

    // Test WHERE clause with partial primary key
    println!("5. Querying by partial primary key (order_id = 100)...");
    let result = db
        .query("SELECT * FROM order_items WHERE order_id = 100")
        .unwrap()
        .into_query_result()
        .unwrap();
    println!("Found {} items for order 100:", result.rows().len());

    for row in result.rows() {
        let product_id = match &row[1] {
            tegdb::SqlValue::Integer(i) => i,
            _ => &0,
        };
        let quantity = match &row[2] {
            tegdb::SqlValue::Integer(i) => i,
            _ => &0,
        };

        println!("  Product: {}, Qty: {}", product_id, quantity);
    }

    println!("\n=== Composite Primary Key Benefits ===");
    println!("• Natural clustering by (order_id, product_id)");
    println!("• Efficient range scans by order_id");
    println!("• Prevents duplicate (order, product) combinations");
    println!("• Storage key: 'order_items:000000000000000100:000000000000000001'");

    Ok(())
}
