//! Demonstration of the query planner architecture concept
//!
//! This example shows the conceptual flow of how a query planner would work:
//! SQL Text -> Parser -> Planner -> Execution Plan -> Plan Executor -> Results
//!
//! Run with: cargo run --example planner_demo

use tegdb::{Database, Result};

fn main() -> Result<()> {
    println!("=== TegDB Query Planner Architecture Concept Demo ===\n");

    // 1. Setup database and create test data
    setup_test_database()?;

    // 2. Demonstrate the conceptual planning pipeline
    demonstrate_planning_concept()?;

    // 3. Show different query optimization strategies
    demonstrate_optimization_strategies()?;

    // 4. Compare execution approaches
    demonstrate_execution_comparison()?;

    println!("\n=== Demo completed successfully! ===");
    Ok(())
}

fn setup_test_database() -> Result<()> {
    println!("1. Setting up test database with sample data...");

    let mut db = Database::open("planner_demo.db")?;

    // Clean up existing tables
    let _ = db.execute("DROP TABLE IF EXISTS users");
    let _ = db.execute("DROP TABLE IF EXISTS orders");

    // Create tables with different schemas
    db.execute(
        "CREATE TABLE users (
        id INTEGER PRIMARY KEY,
        name TEXT NOT NULL,
        email TEXT UNIQUE,
        age INTEGER
    )",
    )?;

    db.execute(
        "CREATE TABLE orders (
        order_id INTEGER PRIMARY KEY,
        user_id INTEGER NOT NULL,
        product TEXT NOT NULL,
        amount REAL,
        order_date TEXT
    )",
    )?;

    // Insert sample data
    for i in 1..=100 {
        db.execute(&format!(
            "INSERT INTO users (id, name, email, age) VALUES ({}, 'User{}', 'user{}@example.com', {})",
            i, i, i, 20 + (i % 50)
        ))?;
    }

    for i in 1..=500 {
        let user_id = 1 + (i % 100);
        db.execute(&format!(
            "INSERT INTO orders (order_id, user_id, product, amount, order_date) VALUES ({}, {}, 'Product{}', {:.2}, '2024-01-{:02}')",
            i, user_id, i % 50, (i as f64) * 9.99, 1 + (i % 28)
        ))?;
    }

    println!("   ✓ Created users table with 100 rows");
    println!("   ✓ Created orders table with 500 rows");
    println!();

    Ok(())
}

fn demonstrate_planning_concept() -> Result<()> {
    println!("2. Demonstrating the conceptual planning pipeline...");

    // Show how different queries would be planned
    let queries_and_strategies = vec![
        (
            "SELECT name, email FROM users WHERE id = 42",
            "Primary Key Lookup",
            "Direct key access - O(1) complexity, most efficient",
        ),
        (
            "SELECT * FROM users WHERE name = 'User50'",
            "Table Scan with Predicate Pushdown",
            "Sequential scan with early filtering - O(n) complexity",
        ),
        (
            "SELECT * FROM users WHERE id = 42 AND age > 25",
            "Primary Key Lookup + Additional Filter",
            "Key lookup followed by condition evaluation - O(1) + constant",
        ),
        (
            "SELECT * FROM users LIMIT 10",
            "Table Scan with Limit Pushdown",
            "Sequential scan with early termination - O(limit) complexity",
        ),
        (
            "UPDATE users SET age = 31 WHERE id = 42",
            "Primary Key Lookup + Update",
            "Key lookup for target row, then update operation",
        ),
        (
            "DELETE FROM users WHERE age < 21",
            "Table Scan + Bulk Delete",
            "Scan to find matching rows, then delete operations",
        ),
    ];

    for (sql, strategy, description) in queries_and_strategies {
        println!("   Query: {sql}");
        println!("   Strategy: {strategy}");
        println!("   Description: {description}");
        println!("   Pipeline: SQL → Parse → Plan({strategy}) → Execute → Result");
        println!();
    }

    Ok(())
}

fn demonstrate_optimization_strategies() -> Result<()> {
    println!("3. Demonstrating query optimization strategies...");

    // Show different optimization techniques
    println!("   Optimization Techniques in TegDB Planner:");
    println!();

    println!("   a) PRIMARY KEY OPTIMIZATION");
    println!("      - Detects equality conditions on primary key columns");
    println!("      - Uses direct key lookup instead of table scan");
    println!("      - Example: WHERE id = 123 → Direct key access");
    println!();

    println!("   b) PREDICATE PUSHDOWN");
    println!("      - Applies filters as early as possible during scan");
    println!("      - Reduces memory usage and processing time");
    println!("      - Example: WHERE age > 30 → Filter during scan, not after");
    println!();

    println!("   c) LIMIT PUSHDOWN");
    println!("      - Enables early termination when LIMIT is specified");
    println!("      - Stops scanning once enough rows are found");
    println!("      - Example: LIMIT 10 → Stop after finding 10 matching rows");
    println!();

    println!("   d) COST-BASED OPTIMIZATION");
    println!("      - Estimates cost of different execution strategies");
    println!("      - Chooses plan with lowest estimated cost");
    println!("      - Considers I/O, CPU, and memory costs");
    println!();

    println!("   e) STATISTICS-DRIVEN DECISIONS");
    println!("      - Uses table statistics for better cost estimation");
    println!("      - Row counts, column cardinality, data distribution");
    println!("      - Adapts plans based on actual data characteristics");
    println!();

    Ok(())
}

fn demonstrate_execution_comparison() -> Result<()> {
    println!("4. Comparing execution approaches...");

    let mut db = Database::open("planner_demo.db")?;

    // Test different query patterns
    let test_queries = vec![
        ("Primary Key Lookup", "SELECT * FROM users WHERE id = 50"),
        (
            "Sequential Scan",
            "SELECT * FROM users WHERE name = 'User50'",
        ),
        (
            "Range Query",
            "SELECT * FROM users WHERE age BETWEEN 25 AND 35",
        ),
        ("Limited Query", "SELECT * FROM users LIMIT 5"),
    ];

    for (query_type, sql) in test_queries {
        println!("   Testing {query_type}: {sql}");

        let start = std::time::Instant::now();
        let result = db.query(sql)?;
        let duration = start.elapsed();

        println!(
            "     → Found {} rows in {:?}",
            result.rows().len(),
            duration
        );

        // Show execution characteristics
        match query_type {
            "Primary Key Lookup" => {
                println!("     → Used direct key access (IOT optimization)");
                println!("     → No table scan required");
            }
            "Sequential Scan" => {
                println!("     → Scanned table sequentially");
                println!("     → Applied filter predicate during scan");
            }
            "Range Query" => {
                println!("     → Scanned table with range condition");
                println!("     → Could benefit from secondary index");
            }
            "Limited Query" => {
                println!("     → Used early termination optimization");
                println!("     → Stopped after finding required rows");
            }
            _ => {}
        }
        println!();
    }

    Ok(())
}

#[allow(dead_code)]
fn show_planner_architecture() {
    println!("5. Query Planner Architecture Overview:");
    println!();
    println!("   ┌─────────────┐    ┌─────────────┐    ┌─────────────┐    ┌─────────────┐");
    println!("   │   SQL Text  │ -> │   Parser    │ -> │   Planner   │ -> │  Executor   │");
    println!("   └─────────────┘    └─────────────┘    └─────────────┘    └─────────────┘");
    println!("          │                   │                   │                   │");
    println!("          │                   │                   │                   │");
    println!("    \"SELECT...\"         Statement         ExecutionPlan        ResultSet");
    println!();
    println!("   Components:");
    println!("   • Parser: Converts SQL text into AST (Abstract Syntax Tree)");
    println!("   • Planner: Analyzes AST and generates optimized execution plan");
    println!("   • Executor: Executes the plan against the storage engine");
    println!();
    println!("   Plan Types:");
    println!("   • PrimaryKeyLookup: Direct key access for equality on PK");
    println!("   • TableScan: Sequential table scan with optimizations");
    println!("   • Insert/Update/Delete: Modification plans");
    println!("   • DDL: Schema change operations");
    println!();
}
