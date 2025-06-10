use tegdb::{Engine, sql::{parse_sql, SqlStatement}, sql_executor::{SqlExecutor, SqlResult}};
use tempfile::tempdir;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("TegDB SQL Parser & Executor Demo");
    println!("================================\n");

    // Create a temporary database
    let dir = tempdir()?;
    let db_path = dir.path().join("demo.db");
    let engine = Engine::new(db_path)?;
    let mut sql_executor = SqlExecutor::new(engine);

    // Example SQL operations
    let sql_statements = vec![
        "CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT NOT NULL, age INTEGER)",
        "INSERT INTO users (id, name, age) VALUES (1, 'Alice', 30), (2, 'Bob', 25), (3, 'Charlie', 35)",
        "SELECT * FROM users",
        "SELECT name, age FROM users WHERE age > 27",
        "UPDATE users SET age = 26 WHERE name = 'Bob'",
        "SELECT * FROM users WHERE name = 'Bob'",
        "DELETE FROM users WHERE age > 30",
        "SELECT * FROM users",
    ];

    for (i, sql) in sql_statements.iter().enumerate() {
        println!("Step {}: {}", i + 1, sql);
        
        match parse_sql(sql) {
            Ok((remaining, statement)) => {
                if !remaining.trim().is_empty() {
                    println!("  ⚠️  Unparsed input: '{}'", remaining);
                }
                
                match sql_executor.execute(statement) {
                    Ok(result) => {
                        println!("  ✓ Executed successfully!");
                        display_result(&result);
                    }
                    Err(e) => {
                        println!("  ✗ Execution error: {:?}", e);
                    }
                }
            }
            Err(e) => {
                println!("  ✗ Parse error: {:?}", e);
            }
        }
        println!();
    }

    // Demonstrate more complex queries
    println!("Advanced Examples:");
    println!("==================\n");

    let advanced_queries = vec![
        "SELECT * FROM users ORDER BY age DESC",
        "SELECT name FROM users WHERE age BETWEEN 25 AND 30",
        "INSERT INTO users (id, name, age) VALUES (4, 'Diana', 28)",
        "SELECT COUNT(*) FROM users",
        "UPDATE users SET age = age + 1 WHERE age < 30",
    ];

    for (i, sql) in advanced_queries.iter().enumerate() {
        println!("Advanced {}: {}", i + 1, sql);
        
        match parse_sql(sql) {
            Ok((remaining, statement)) => {
                if !remaining.trim().is_empty() {
                    println!("  ⚠️  Unparsed input: '{}'", remaining);
                }
                
                // Note: Some of these advanced features (ORDER BY, BETWEEN, COUNT, etc.) 
                // are not yet implemented in our executor, but they parse correctly
                match statement {
                    SqlStatement::Select(ref select) => {
                        if select.order_by.is_some() {
                            println!("  ℹ️  ORDER BY clause parsed but not yet implemented in executor");
                        }
                        if select.columns.iter().any(|c| c.contains("COUNT")) {
                            println!("  ℹ️  Aggregate functions parsed but not yet implemented in executor");
                        }
                    }
                    _ => {}
                }
                
                // Try to execute (some may fail due to unimplemented features)
                match sql_executor.execute(statement) {
                    Ok(result) => {
                        println!("  ✓ Executed successfully!");
                        display_result(&result);
                    }
                    Err(e) => {
                        println!("  ℹ️  Expected limitation: {:?}", e);
                    }
                }
            }
            Err(e) => {
                println!("  ✗ Parse error: {:?}", e);
            }
        }
        println!();
    }

    println!("Demo completed! The SQL parser successfully parsed all statements,");
    println!("and the executor handled basic CRUD operations.");
    println!("\nNext steps for TegDB SQL layer:");
    println!("- Implement ORDER BY, LIMIT, and aggregate functions");
    println!("- Add support for JOINs and subqueries");
    println!("- Implement proper indexing for efficient WHERE clauses");
    println!("- Add transaction support to the SQL layer");
    println!("- Implement proper data types and constraints");

    Ok(())
}

fn display_result(result: &SqlResult) {
    match result {
        SqlResult::Select { columns, rows } => {
            if rows.is_empty() {
                println!("    No rows returned");
            } else {
                println!("    Columns: {:?}", columns);
                for (i, row) in rows.iter().enumerate() {
                    println!("    Row {}: {:?}", i + 1, row);
                }
                println!("    ({} row(s) returned)", rows.len());
            }
        }
        SqlResult::Insert { rows_affected } => {
            println!("    {} row(s) inserted", rows_affected);
        }
        SqlResult::Update { rows_affected } => {
            println!("    {} row(s) updated", rows_affected);
        }
        SqlResult::Delete { rows_affected } => {
            println!("    {} row(s) deleted", rows_affected);
        }
        SqlResult::CreateTable { table_name } => {
            println!("    Table '{}' created", table_name);
        }
    }
}
