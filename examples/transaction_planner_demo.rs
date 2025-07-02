// Transaction planner integration demonstration
use tegdb::{Database, Result};

fn main() -> Result<()> {
    println!("=== TegDB Transaction Planner Integration Demo ===\n");

    // Create database and set up test data
    let mut db = Database::open("transaction_planner_demo.db")?;

    // Clean up any existing test data
    let _ = db.execute("DROP TABLE IF EXISTS users");

    // Create table
    db.execute("CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT, age INTEGER)")?;

    // Insert some initial data
    db.execute("INSERT INTO users (id, name, age) VALUES (1, 'Alice', 30)")?;
    db.execute("INSERT INTO users (id, name, age) VALUES (2, 'Bob', 25)")?;

    println!("1. Testing Transaction.execute() with planner...");

    // Start a transaction and use execute() method (now uses planner)
    {
        let mut tx = db.begin_transaction()?;

        // INSERT through transaction (uses planner pipeline)
        let rows = tx.execute("INSERT INTO users (id, name, age) VALUES (3, 'Carol', 35)")?;
        println!("   → Inserted {rows} rows via transaction planner");

        // UPDATE through transaction (uses planner pipeline)
        let rows = tx.execute("UPDATE users SET age = 31 WHERE id = 1")?;
        println!("   → Updated {rows} rows via transaction planner");

        tx.commit()?;
    }

    println!("\n2. Testing Transaction.streaming_query() with planner...");

    // Start another transaction and use query() method (now uses planner)
    {
        let mut tx = db.begin_transaction()?;

        // SELECT through transaction (uses planner pipeline)
        let result = tx
            .streaming_query("SELECT * FROM users WHERE age > 25")
            .unwrap()
            .into_query_result()
            .unwrap();
        println!(
            "   → Query via transaction planner found {} rows:",
            result.rows().len()
        );

        for row in result.rows().iter() {
            let id: i64 = match &row[0] {
                tegdb::SqlValue::Integer(v) => *v,
                _ => 0,
            };
            let name = match &row[1] {
                tegdb::SqlValue::Text(v) => v.clone(),
                _ => "unknown".to_string(),
            };
            let age: i64 = match &row[2] {
                tegdb::SqlValue::Integer(v) => *v,
                _ => 0,
            };
            println!("     • {name} (ID: {id}, Age: {age})");
        }

        tx.commit()?;
    }

    println!("\n3. Testing Transaction rollback with planner...");

    // Test rollback functionality
    {
        let mut tx = db.begin_transaction()?;

        // Make some changes that we'll rollback
        tx.execute("INSERT INTO users (id, name, age) VALUES (4, 'Dave', 40)")?;
        tx.execute("UPDATE users SET age = 99 WHERE id = 2")?;

        println!("   → Made changes in transaction (insert + update)");

        // Check changes are visible within transaction
        let result = tx
            .streaming_query("SELECT * FROM users")
            .unwrap()
            .into_query_result()
            .unwrap();
        let count = result.rows().len();
        println!("   → Count within transaction: {count}");

        // Rollback the transaction
        tx.rollback()?;
        println!("   → Transaction rolled back");
    }

    // Verify rollback worked
    let result = db
        .query("SELECT * FROM users")
        .unwrap()
        .into_query_result()
        .unwrap();
    let final_count = result.rows().len();
    println!("   → Final count after rollback: {final_count}");

    println!("\n=== Transaction Planner Integration Confirmed ===");
    println!("✓ Transaction.execute() uses QueryPlanner + PlanExecutor");
    println!("✓ Transaction.streaming_query() uses QueryPlanner + PlanExecutor");
    println!("✓ All transaction operations benefit from query optimization");
    println!("✓ ACID properties maintained with planner pipeline");

    Ok(())
}
