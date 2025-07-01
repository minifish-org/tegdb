//! Example demonstrating arithmetic expressions in UPDATE statements
//! 
//! This example shows how TegDB now supports arithmetic operations
//! in UPDATE statements, allowing expressions like:
//! - UPDATE table SET column = column + 5
//! - UPDATE table SET price = price * 1.1
//! - UPDATE table SET total = quantity * price

use tegdb::{Database, SqlValue};

fn main() -> tegdb::Result<()> {
    println!("=== TegDB Arithmetic Expressions Example ===\n");
    
    // Clean up any existing database
    let _ = std::fs::remove_file("arithmetic_example.db");
    
    // Create database
    let mut db = Database::open("arithmetic_example.db")?;
    
    // Create a products table
    println!("1. Creating products table...");
    db.execute("CREATE TABLE products (id INTEGER PRIMARY KEY, name TEXT, price REAL, quantity INTEGER, discount REAL)")?;
    
    // Insert sample data
    println!("2. Inserting sample products...");
    db.execute("INSERT INTO products (id, name, price, quantity, discount) VALUES (1, 'Laptop', 999.99, 10, 0.0)")?;
    db.execute("INSERT INTO products (id, name, price, quantity, discount) VALUES (2, 'Mouse', 29.99, 50, 0.0)")?;
    db.execute("INSERT INTO products (id, name, price, quantity, discount) VALUES (3, 'Keyboard', 79.99, 25, 0.0)")?;
    
    // Show initial data
    println!("3. Initial product data:");
    print_products(&mut db)?;
    
    // Example 1: Increase all prices by 10% (multiplication)
    println!("4. Applying 10% price increase to all products...");
    db.execute("UPDATE products SET price = price * 1.1")?;
    print_products(&mut db)?;
    
    // Example 2: Add quantity (addition)
    println!("5. Restocking: adding 5 units to each product...");
    db.execute("UPDATE products SET quantity = quantity + 5")?;
    print_products(&mut db)?;
    
    // Example 3: Apply discount (subtraction)
    println!("6. Applying $5 discount to products over $50...");
    db.execute("UPDATE products SET discount = 5.0 WHERE price > 50.0")?;
    db.execute("UPDATE products SET price = price - discount WHERE discount > 0.0")?;
    print_products(&mut db)?;
    
    // Example 4: Complex expression with multiple operations
    println!("7. Final adjustment: quantity = quantity * 2 - 3...");
    db.execute("UPDATE products SET quantity = quantity * 2 - 3")?;
    print_products(&mut db)?;
    
    // Example 5: Show the parsing capabilities with different data types
    println!("8. Testing mixed type arithmetic (integer + real)...");
    db.execute("UPDATE products SET price = quantity + price WHERE id = 1")?;
    
    let result = db.query("SELECT name, price FROM products WHERE id = 1").unwrap().into_query_result().unwrap();
    if let Some(row) = result.rows().get(0) {
        println!("   Product '{}' now has price: {:?}", 
                 match &row[0] { SqlValue::Text(s) => s.as_str(), _ => "unknown" },
                 &row[1]);
    }
    
    println!("\n🎉 All arithmetic operations completed successfully!");
    
    println!("\n💡 Supported arithmetic operations:");
    println!("   + Addition       (works with integers, reals, and text concatenation)");
    println!("   - Subtraction    (integers and reals)");
    println!("   * Multiplication (integers and reals)");
    println!("   / Division       (integers and reals, with zero-division protection)");
    println!("   Operator precedence: * and / before + and -");
    println!("   Parentheses support: (expression)");
    
    // Clean up
    let _ = std::fs::remove_file("arithmetic_example.db");
    
    Ok(())
}

fn print_products(db: &mut Database) -> tegdb::Result<()> {
    let result = db.query("SELECT name, price, quantity, discount FROM products ORDER BY id").unwrap().into_query_result().unwrap();
    
    println!("   Products:");
    println!("   | Name     | Price    | Quantity | Discount |");
    println!("   |----------|----------|----------|----------|");
    
    for row in result.rows() {
        let name = match &row[0] { SqlValue::Text(s) => s.as_str(), _ => "?" };
        let price = match &row[1] { SqlValue::Real(f) => format!("{:.2}", f), SqlValue::Integer(i) => i.to_string(), _ => "?".to_string() };
        let quantity = match &row[2] { SqlValue::Integer(i) => i.to_string(), _ => "?".to_string() };
        let discount = match &row[3] { SqlValue::Real(f) => format!("{:.2}", f), SqlValue::Integer(i) => i.to_string(), _ => "?".to_string() };
        
        println!("   | {:<8} | ${:<7} | {:<8} | ${:<7} |", name, price, quantity, discount);
    }
    println!();
    
    Ok(())
}
