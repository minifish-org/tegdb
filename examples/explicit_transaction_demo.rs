use tegdb::{Engine, parser::parse_sql, executor::{Executor, ResultSet}};
use tempfile::tempdir;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("TegDB Explicit Transaction Demo");
    println!("===============================\n");

    // Create a temporary database
    let dir = tempdir()?;
    let db_path = dir.path().join("demo.db");
    let mut engine = Engine::new(db_path)?;
    let transaction = engine.begin_transaction();
    let mut executor = Executor::new(transaction);

    // Demonstrate explicit transaction workflow
    let sql_statements = vec![
        "BEGIN",
        "CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT NOT NULL, age INTEGER)",
        "INSERT INTO users (id, name, age) VALUES (1, 'Alice', 30), (2, 'Bob', 25)",
        "SELECT * FROM users",
        "UPDATE users SET age = 26 WHERE name = 'Bob'",
        "COMMIT",
    ];

    for (i, sql) in sql_statements.iter().enumerate() {
        println!("Step {}: {}", i + 1, sql);
        
        match parse_sql(sql) {
            Ok((remaining, statement)) => {
                if !remaining.trim().is_empty() {
                    println!("  ⚠️  Unparsed input: '{}'", remaining);
                }
                
                match executor.execute(statement) {
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

    // Demonstrate error when trying to execute without transaction
    println!("Error Handling Demo:");
    println!("===================\n");

    let no_transaction_sql = "SELECT * FROM users";
    println!("Attempting: {}", no_transaction_sql);
    match parse_sql(no_transaction_sql) {
        Ok((_, statement)) => {
            match executor.execute(statement) {
                Ok(_) => println!("  ✗ Unexpected success"),
                Err(e) => println!("  ✓ Expected error: {:?}", e),
            }
        }
        Err(e) => println!("  ✗ Parse error: {:?}", e),
    }

    println!("\nDemo completed!");
    Ok(())
}

fn display_result(result: &ResultSet) {
    match result {
        ResultSet::Select { columns, rows } => {
            println!("    Query result: {} columns, {} rows", columns.len(), rows.len());
            if !rows.is_empty() {
                println!("    Columns: {:?}", columns);
                for (i, row) in rows.iter().enumerate() {
                    println!("    Row {}: {:?}", i + 1, row);
                }
            }
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
        ResultSet::CreateTable { table_name } => {
            println!("    Created table: {}", table_name);
        }
        ResultSet::Begin { transaction_id } => {
            println!("    Started transaction: {}", transaction_id);
        }
        ResultSet::Commit { transaction_id } => {
            println!("    Committed transaction: {}", transaction_id);
        }
        ResultSet::Rollback { transaction_id } => {
            println!("    Rolled back transaction: {}", transaction_id);
        }
    }
}