use tegdb::parser::{parse_sql, SqlValue, Statement};

fn main() {
    println!("=== Vector Debug Test ===");
    
    // Test vector parsing
    println!("\n1. Testing vector parsing...");
    let sql = "INSERT INTO test (id, vec) VALUES (1, [1.0, 2.0, 3.0])";
    match parse_sql(sql) {
        Ok(Statement::Insert(insert)) => {
            println!("   ✓ SQL parsed successfully");
            println!("   Table: {}", insert.table);
            println!("   Columns: {:?}", insert.columns);
            println!("   Values: {:?}", insert.values);
            
            // Check if the vector was parsed correctly
            if let Some(row) = insert.values.first() {
                if let Some(SqlValue::Vector(v)) = row.get(1) {
                    println!("   ✓ Vector parsed: {:?}", v);
                } else {
                    println!("   ✗ Vector not parsed correctly: {:?}", row.get(1));
                }
            }
        }
        Ok(other) => println!("   ✗ Unexpected statement type: {:?}", other),
        Err(e) => println!("   ✗ Parse error: {}", e),
    }
    
    // Test CREATE TABLE with vector
    println!("\n2. Testing CREATE TABLE with vector...");
    let create_sql = "CREATE TABLE test (id INTEGER, vec VECTOR(3))";
    match parse_sql(create_sql) {
        Ok(Statement::CreateTable(create)) => {
            println!("   ✓ CREATE TABLE parsed successfully");
            println!("   Table: {}", create.table);
            for col in &create.columns {
                println!("   Column: {} - {:?}", col.name, col.data_type);
            }
        }
        Ok(other) => println!("   ✗ Unexpected statement type: {:?}", other),
        Err(e) => println!("   ✗ Parse error: {}", e),
    }
} 