use tegdb::{Database, parser::{parse_sql, Statement, Expression}};

fn main() {
    // Test aggregate function parsing
    let sql = "SELECT COUNT(*) FROM test_table";
    match parse_sql(sql) {
        Ok(Statement::Select(select)) => {
            println!("✅ Successfully parsed SELECT statement");
            println!("Columns: {:?}", select.columns);
            println!("Table: {}", select.table);
            
            if let Some(Expression::AggregateFunction { name, arg }) = select.columns.first() {
                println!("✅ Found aggregate function: {} with arg: {:?}", name, arg);
            } else {
                println!("❌ Expected aggregate function, got: {:?}", select.columns.first());
            }
        }
        Ok(other) => {
            println!("❌ Expected SELECT statement, got: {:?}", other);
        }
        Err(e) => {
            println!("❌ Parse error: {}", e);
        }
    }
    
    // Test database execution
    let path = std::env::temp_dir().join("test_aggregate.db");
    if path.exists() {
        std::fs::remove_file(&path).unwrap();
    }
    
    let mut db = Database::open(&path.to_string_lossy()).expect("Failed to create database");
    
    // Create a simple table
    db.execute("CREATE TABLE test_table (id INTEGER PRIMARY KEY, value INTEGER)").unwrap();
    db.execute("INSERT INTO test_table (id, value) VALUES (1, 100)").unwrap();
    db.execute("INSERT INTO test_table (id, value) VALUES (2, 200)").unwrap();
    db.execute("INSERT INTO test_table (id, value) VALUES (3, 300)").unwrap();
    
    // Test aggregate query
    match db.query("SELECT COUNT(*) FROM test_table") {
        Ok(result) => {
            println!("✅ Aggregate query succeeded");
            println!("Columns: {:?}", result.columns());
            println!("Rows: {:?}", result.rows());
        }
        Err(e) => {
            println!("❌ Aggregate query failed: {}", e);
        }
    }
    
    // Clean up
    std::fs::remove_file(&path).unwrap();
} 