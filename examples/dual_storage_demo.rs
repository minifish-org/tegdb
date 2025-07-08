use tegdb::{Database, Result};

fn main() -> Result<()> {
    println!("=== TegDB Dual Storage Backend Demo ===\n");

    // Native file backend (default)
    println!("1. Testing file backend...");
    test_file_backend()?;
    
    // Browser storage backend (simulated - would work in WASM)
    println!("\n2. Testing browser storage backend simulation...");
    test_browser_backend()?;

    println!("\n🎉 Dual storage backend test completed successfully!");
    Ok(())
}

fn test_file_backend() -> Result<()> {
    println!("   Creating file-based database...");
    let mut db = Database::open("demo_file_backend.db")?;
    
    println!("   Creating table and inserting data...");
    db.execute("CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT, score REAL)")?;
    db.execute("INSERT INTO users (id, name, score) VALUES (1, 'Alice', 95.5)")?;
    db.execute("INSERT INTO users (id, name, score) VALUES (2, 'Bob', 87.2)")?;
    
    println!("   Querying data...");
    let results = db.query("SELECT name, score FROM users WHERE score > 90")?;
    for row_result in results {
        let row = row_result?;
        if let [name, score] = &row[..] {
            println!("     File: {:?} - {:?}", name, score);
        }
    }
    
    println!("   ✓ File backend test completed");
    Ok(())
}

fn test_browser_backend() -> Result<()> {
    // Note: This demonstrates how browser storage would work
    // In actual WASM, you'd use "browser://my-app-db" or "localstorage://my-app-db"
    
    println!("   Simulating browser storage with localstorage:// prefix...");
    
    // This will still use file backend on native, but demonstrates the interface
    let mut db = Database::open("localstorage://demo_browser_backend")?;
    
    println!("   Creating table and inserting data...");
    db.execute("CREATE TABLE products (id INTEGER PRIMARY KEY, name TEXT, price REAL)")?;
    db.execute("INSERT INTO products (id, name, price) VALUES (1, 'Widget', 19.99)")?;
    db.execute("INSERT INTO products (id, name, price) VALUES (2, 'Gadget', 29.99)")?;
    
    println!("   Querying data...");
    let results = db.query("SELECT name, price FROM products WHERE price < 25")?;
    for row_result in results {
        let row = row_result?;
        if let [name, price] = &row[..] {
            println!("     Browser: {:?} - {:?}", name, price);
        }
    }
    
    println!("   ✓ Browser backend simulation completed");
    println!("   💡 In WASM builds, this would use localStorage/IndexedDB");
    Ok(())
}
