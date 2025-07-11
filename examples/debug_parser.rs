use tegdb::parse_sql;

fn main() {
    let test_queries = vec![
        "SELECT name, age FROM users",
        "SELECT * FROM users",
        "SELECT name FROM users",
        "SELECT name, age FROM users WHERE age > 25",
    ];

    for query in test_queries {
        println!("Testing: '{query}'");
        match parse_sql(query) {
            Ok(stmt) => {
                println!("  ✓ Success: {stmt:?}");
            }
            Err(e) => {
                println!("  ✗ Error: {e}");
            }
        }
        println!();
    }
}
