use tegdb::parser::{parse_sql, Expression, Statement};

fn main() {
    // Test aggregate function parsing
    let sql = "SELECT COUNT(*) FROM test_table";
    match parse_sql(sql) {
        Ok(Statement::Select(select)) => {
            println!("✅ Successfully parsed SELECT statement");
            let columns = &select.columns;
            println!("Columns: {columns:?}");
            let table = &select.table;
            println!("Table: {table}");

            if let Some(Expression::AggregateFunction { name, arg }) = select.columns.first() {
                println!("✅ Found aggregate function: {name} with arg: {arg:?}");
            } else {
                let first = select.columns.first();
                println!("❌ Expected aggregate function, got: {first:?}");
            }
        }
        Ok(other) => {
            println!("❌ Expected SELECT statement, got: {other:?}");
        }
        Err(e) => {
            println!("❌ Parse error: {e}");
        }
    }

    // Test SUM function
    let sql = "SELECT SUM(value) FROM test_table";
    match parse_sql(sql) {
        Ok(Statement::Select(select)) => {
            println!("✅ Successfully parsed SUM statement");
            if let Some(Expression::AggregateFunction { name, arg }) = select.columns.first() {
                println!("✅ Found aggregate function: {name} with arg: {arg:?}");
            }
        }
        Err(e) => {
            println!("❌ Parse error: {e}");
        }
        _ => {}
    }
}
