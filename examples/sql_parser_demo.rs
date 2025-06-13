use tegdb::sql::{parse_sql, SqlStatement};

fn main() {
    // Example SQL statements
    let queries = vec![
        "SELECT * FROM users",
        "SELECT name, age FROM users WHERE age > 18",
        "INSERT INTO users (name, age) VALUES ('John', 25), ('Jane', 30)",
        "UPDATE users SET name = 'Johnny' WHERE id = 1",
        "DELETE FROM users WHERE age < 18",
        "CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT NOT NULL, age INTEGER)",
    ];

    println!("TegDB SQL Parser Examples");
    println!("========================\n");

    for (i, query) in queries.iter().enumerate() {
        println!("Example {}: {}", i + 1, query);
        
        match parse_sql(query) {
            Ok((remaining, statement)) => {
                println!("✓ Successfully parsed!");
                println!("  Remaining input: '{}'", remaining);
                println!("  Parsed statement: {:#?}", statement);
                
                // Demonstrate pattern matching on the parsed statement
                match statement {
                    SqlStatement::Select(select) => {
                        println!("  → This is a SELECT statement for table: {}", select.table);
                        println!("  → Selected columns: {:?}", select.columns);
                    }
                    SqlStatement::Insert(insert) => {
                        println!("  → This is an INSERT statement for table: {}", insert.table);
                        println!("  → Number of value rows: {}", insert.values.len());
                    }
                    SqlStatement::Update(update) => {
                        println!("  → This is an UPDATE statement for table: {}", update.table);
                        println!("  → Number of assignments: {}", update.assignments.len());
                    }
                    SqlStatement::Delete(delete) => {
                        println!("  → This is a DELETE statement for table: {}", delete.table);
                    }
                    SqlStatement::CreateTable(create) => {
                        println!("  → This is a CREATE TABLE statement for table: {}", create.table);
                        println!("  → Number of columns: {}", create.columns.len());
                    }
                    SqlStatement::Begin => {
                        println!("  → This is a BEGIN transaction statement");
                    }
                    SqlStatement::Commit => {
                        println!("  → This is a COMMIT transaction statement");
                    }
                    SqlStatement::Rollback => {
                        println!("  → This is a ROLLBACK transaction statement");
                    }
                }
            }
            Err(e) => {
                println!("✗ Parse error: {:?}", e);
            }
        }
        
        println!();
    }

    // Example of error handling
    println!("Example with invalid SQL:");
    let invalid_sql = "INVALID SQL STATEMENT";
    println!("{}", invalid_sql);
    match parse_sql(invalid_sql) {
        Ok((remaining, statement)) => {
            println!("✓ Unexpectedly parsed: {:#?}", statement);
            println!("  Remaining: '{}'", remaining);
        }
        Err(e) => {
            println!("✗ Parse error (as expected): {:?}", e);
        }
    }
}
