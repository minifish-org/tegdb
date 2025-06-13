use tegdb::{Engine, executor::Executor, parser::parse_sql, executor::ResultSet};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("TegDB Comprehensive Explicit Transaction Demo");
    println!("===========================================");

    // Create database and executor
    let engine = Engine::new("explicit_transaction_comprehensive_demo.db".into())?;
    let mut executor = Executor::new(engine);

    // Test 1: Basic workflow with COMMIT
    println!("\nTest 1: Basic workflow with COMMIT");
    println!("=================================");
    
    demo_section(&mut executor, "BEGIN", |e| {
        let (_, stmt) = parse_sql("BEGIN").unwrap();
        e.execute(stmt)
    })?;

    demo_section(&mut executor, "CREATE TABLE", |e| {
        let (_, stmt) = parse_sql("CREATE TABLE products (id INTEGER PRIMARY KEY, name TEXT NOT NULL, price REAL)").unwrap();
        e.execute(stmt)
    })?;

    demo_section(&mut executor, "INSERT products", |e| {
        let (_, stmt) = parse_sql("INSERT INTO products (id, name, price) VALUES (1, 'Laptop', 999.99), (2, 'Mouse', 29.95)").unwrap();
        e.execute(stmt)
    })?;

    demo_section(&mut executor, "SELECT products", |e| {
        let (_, stmt) = parse_sql("SELECT * FROM products").unwrap();
        e.execute(stmt)
    })?;

    demo_section(&mut executor, "UPDATE products", |e| {
        let (_, stmt) = parse_sql("UPDATE products SET price = 899.99 WHERE name = 'Laptop'").unwrap();
        e.execute(stmt)
    })?;

    demo_section(&mut executor, "COMMIT", |e| {
        let (_, stmt) = parse_sql("COMMIT").unwrap();
        e.execute(stmt)
    })?;

    // Test 2: Rollback scenario
    println!("\nTest 2: ROLLBACK scenario");
    println!("========================");

    demo_section(&mut executor, "BEGIN", |e| {
        let (_, stmt) = parse_sql("BEGIN").unwrap();
        e.execute(stmt)
    })?;

    demo_section(&mut executor, "INSERT more products", |e| {
        let (_, stmt) = parse_sql("INSERT INTO products (id, name, price) VALUES (3, 'Keyboard', 79.99)").unwrap();
        e.execute(stmt)
    })?;

    demo_section(&mut executor, "SELECT with new data", |e| {
        let (_, stmt) = parse_sql("SELECT * FROM products").unwrap();
        e.execute(stmt)
    })?;

    demo_section(&mut executor, "DELETE product", |e| {
        let (_, stmt) = parse_sql("DELETE FROM products WHERE id = 1").unwrap();
        e.execute(stmt)
    })?;

    demo_section(&mut executor, "SELECT after delete", |e| {
        let (_, stmt) = parse_sql("SELECT * FROM products").unwrap();
        e.execute(stmt)
    })?;

    demo_section(&mut executor, "ROLLBACK", |e| {
        let (_, stmt) = parse_sql("ROLLBACK").unwrap();
        e.execute(stmt)
    })?;

    // Test 3: Verify rollback worked
    println!("\nTest 3: Verify rollback worked");
    println!("=============================");

    demo_section(&mut executor, "BEGIN", |e| {
        let (_, stmt) = parse_sql("BEGIN").unwrap();
        e.execute(stmt)
    })?;

    demo_section(&mut executor, "SELECT after rollback", |e| {
        let (_, stmt) = parse_sql("SELECT * FROM products").unwrap();
        e.execute(stmt)
    })?;

    demo_section(&mut executor, "COMMIT", |e| {
        let (_, stmt) = parse_sql("COMMIT").unwrap();
        e.execute(stmt)
    })?;

    // Test 4: Error handling
    println!("\nTest 4: Error handling");
    println!("====================");

    println!("Attempting operation without transaction:");
    match parse_sql("SELECT * FROM products") {
        Ok((_, stmt)) => {
            match executor.execute(stmt) {
                Ok(_) => println!("  ❌ Unexpected success"),
                Err(e) => println!("  ✓ Expected error: {}", e),
            }
        }
        Err(e) => println!("  ❌ Parse error: {}", e),
    }

    println!("\nAttempting COMMIT without BEGIN:");
    match parse_sql("COMMIT") {
        Ok((_, stmt)) => {
            match executor.execute(stmt) {
                Ok(_) => println!("  ❌ Unexpected success"),
                Err(e) => println!("  ✓ Expected error: {}", e),
            }
        }
        Err(e) => println!("  ❌ Parse error: {}", e),
    }

    println!("\nDemo completed successfully!");
    
    // Clean up
    std::fs::remove_file("explicit_transaction_comprehensive_demo.db").ok();
    
    Ok(())
}

fn demo_section<F>(executor: &mut Executor, operation: &str, f: F) -> Result<(), Box<dyn std::error::Error>>
where
    F: Fn(&mut Executor) -> Result<ResultSet, tegdb::Error>,
{
    print!("  {}: ", operation);
    match f(executor) {
        Ok(result) => {
            println!("✓ Success");
            match result {
                ResultSet::Begin { transaction_id } => {
                    println!("    Started transaction: {}", transaction_id);
                }
                ResultSet::Commit { transaction_id } => {
                    println!("    Committed transaction: {}", transaction_id);
                }
                ResultSet::Rollback { transaction_id } => {
                    println!("    Rolled back transaction: {}", transaction_id);
                }
                ResultSet::CreateTable { table_name } => {
                    println!("    Created table: {}", table_name);
                }
                ResultSet::Insert { rows_affected } => {
                    println!("    Inserted {} rows", rows_affected);
                }
                ResultSet::Update { rows_affected } => {
                    println!("    Updated {} rows", rows_affected);
                }
                ResultSet::Delete { rows_affected } => {
                    println!("    Deleted {} rows", rows_affected);
                }
                ResultSet::Select { columns, rows } => {
                    println!("    Query result: {} columns, {} rows", columns.len(), rows.len());
                    if !rows.is_empty() {
                        println!("    Columns: {:?}", columns);
                        for (i, row) in rows.iter().enumerate() {
                            println!("    Row {}: {:?}", i + 1, row);
                        }
                    }
                }
            }
        }
        Err(e) => {
            println!("❌ Error: {}", e);
            return Err(e.into());
        }
    }
    Ok(())
}
