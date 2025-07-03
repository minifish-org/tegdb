//! Demonstration of IOT (Index-Organized Table) optimization
//!
//! This example shows how the IOT implementation stores only non-primary key
//! columns in the value, eliminating redundancy.

use tegdb::{Database, Result};

fn main() -> Result<()> {
    // Create a temporary database
    let temp_dir = std::env::temp_dir();
    let db_path = temp_dir.join("iot_demo.db");

    // Clean up any existing file
    let _ = std::fs::remove_file(&db_path);

    let mut db = Database::open(&db_path)?;

    println!("=== IOT Optimization Demo ===\n");

    // Create a table with composite primary key
    println!("1. Creating table with composite primary key:");
    let create_sql = "CREATE TABLE orders (
        customer_id INTEGER,
        order_id INTEGER PRIMARY KEY,
        product_name TEXT,
        quantity INTEGER,
        price REAL
    )";
    println!("   {create_sql}");
    db.execute(create_sql)?;

    // Insert some test data
    println!("\n2. Inserting test data:");
    let insert_sql = "INSERT INTO orders (customer_id, order_id, product_name, quantity, price) 
                     VALUES (1, 101, 'Laptop', 2, 999.99)";
    println!("   {insert_sql}");
    db.execute(insert_sql)?;

    let insert_sql2 = "INSERT INTO orders (customer_id, order_id, product_name, quantity, price) 
                      VALUES (1, 102, 'Mouse', 5, 25.50)";
    println!("   {insert_sql2}");
    db.execute(insert_sql2)?;

    let insert_sql3 = "INSERT INTO orders (customer_id, order_id, product_name, quantity, price) 
                      VALUES (2, 201, 'Keyboard', 1, 89.99)";
    println!("   {insert_sql3}");
    db.execute(insert_sql3)?;

    // Query the data to verify IOT works correctly
    println!("\n3. Querying data (IOT reconstruction in action):");
    let result = db
        .query("SELECT * FROM orders ORDER BY customer_id, order_id")
        .unwrap();

    println!("   Columns: {:?}", result.columns());
    for (i, row) in result.rows().iter().enumerate() {
        let customer_id_pos = result
            .columns()
            .iter()
            .position(|c| c == "customer_id")
            .unwrap();
        let order_id_pos = result
            .columns()
            .iter()
            .position(|c| c == "order_id")
            .unwrap();
        let product_name_pos = result
            .columns()
            .iter()
            .position(|c| c == "product_name")
            .unwrap();
        let quantity_pos = result
            .columns()
            .iter()
            .position(|c| c == "quantity")
            .unwrap();
        let price_pos = result.columns().iter().position(|c| c == "price").unwrap();

        println!("   Row {}: customer_id={:?}, order_id={:?}, product_name={:?}, quantity={:?}, price={:?}",
            i,
            &row[customer_id_pos],
            &row[order_id_pos],
            &row[product_name_pos],
            &row[quantity_pos],
            &row[price_pos]
        );
    }

    // Demonstrate primary key lookup efficiency
    println!("\n4. Primary key lookup (efficient IOT access):");
    let pk_result = db
        .query("SELECT * FROM orders WHERE customer_id = 1 AND order_id = 101")
        .unwrap();
    println!(
        "   Found {} rows for customer_id=1, order_id=101",
        pk_result.rows().len()
    );
    if let Some(row) = pk_result.rows().first() {
        let product_name_pos = pk_result
            .columns()
            .iter()
            .position(|c| c == "product_name")
            .unwrap();
        let quantity_pos = pk_result
            .columns()
            .iter()
            .position(|c| c == "quantity")
            .unwrap();
        let price_pos = pk_result
            .columns()
            .iter()
            .position(|c| c == "price")
            .unwrap();

        println!(
            "   Product: {:?}, Quantity: {:?}, Price: {:?}",
            &row[product_name_pos], &row[quantity_pos], &row[price_pos]
        );
    }

    println!("\n=== IOT Storage Efficiency ===");
    println!("✅ Storage Key:   'orders:00000000000000000001:00000000000000000101'");
    println!("✅ Storage Value: Only non-PK columns (product_name, quantity, price)");
    println!("❌ OLD approach: Would store ALL columns including redundant PK values");
    println!("\n🚀 Benefits:");
    println!("   • Reduced storage space (no PK redundancy)");
    println!("   • Faster I/O (smaller values)");
    println!("   • Efficient primary key lookups");
    println!("   • Natural clustering by primary key");

    // Clean up
    let _ = std::fs::remove_file(&db_path);

    Ok(())
}
