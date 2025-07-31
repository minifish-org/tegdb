use tegdb::parser::{parse_sql, Statement, Expression};

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
    
    // Test SUM function
    let sql = "SELECT SUM(value) FROM test_table";
    match parse_sql(sql) {
        Ok(Statement::Select(select)) => {
            println!("✅ Successfully parsed SUM statement");
            if let Some(Expression::AggregateFunction { name, arg }) = select.columns.first() {
                println!("✅ Found aggregate function: {} with arg: {:?}", name, arg);
            }
        }
        Err(e) => {
            println!("❌ Parse error: {}", e);
        }
        _ => {}
    }
} 