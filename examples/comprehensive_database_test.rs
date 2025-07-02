// examples/comprehensive_database_test.rs
use tegdb::{Database, Result, SqlValue};
use tempfile::NamedTempFile;

fn main() -> Result<()> {
    // Create a temporary database file that will be automatically cleaned up
    let temp_file = NamedTempFile::new().expect("Failed to create temp file");
    let db_path = temp_file.path();

    // Create/open database
    let mut db = Database::open(db_path)?;

    println!("=== Setting up database ===");

    // Create table
    db.execute("CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT NOT NULL, age INTEGER)")?;
    println!("✓ Table created");

    // Insert test data
    db.execute("INSERT INTO users (id, name, age) VALUES (1, 'Alice', 30)")?;
    db.execute("INSERT INTO users (id, name, age) VALUES (2, 'Bob', 25)")?;
    db.execute("INSERT INTO users (id, name, age) VALUES (3, 'Carol', 35)")?;
    println!("✓ Test data inserted");

    // Query all data
    let result = db
        .query("SELECT id, name, age FROM users")?
        .into_query_result()?;
    println!("\n=== Initial data ===");
    print_query_result(&result);

    println!("\n=== Testing explicit transaction ===");

    // Test explicit transaction
    {
        let mut tx = db.begin_transaction()?;

        // Try UPDATE within transaction
        let updated = tx.execute("UPDATE users SET age = 31 WHERE name = 'Alice'")?;
        println!("UPDATE affected {} rows", updated);

        // Try DELETE within transaction
        let deleted = tx.execute("DELETE FROM users WHERE name = 'Bob'")?;
        println!("DELETE affected {} rows", deleted);

        // Try SELECT within transaction to see changes
        let tx_result = tx
            .streaming_query("SELECT id, name, age FROM users")?
            .into_query_result()?;
        println!("Data within transaction:");
        print_query_result(&tx_result);

        // Commit transaction
        tx.commit()?;
        println!("✓ Transaction committed");
    }

    // Query again to see final state
    let final_result = db
        .query("SELECT id, name, age FROM users")?
        .into_query_result()?;
    println!("\n=== Final data after transaction ===");
    print_query_result(&final_result);

    // Test simple operations
    println!("\n=== Testing simple operations ===");
    db.execute("INSERT INTO users (id, name, age) VALUES (4, 'David', 28)")?;
    let updated = db.execute("UPDATE users SET age = 36 WHERE name = 'Carol'")?;
    println!("Simple UPDATE affected {} rows", updated);

    let simple_result = db
        .query("SELECT id, name, age FROM users")?
        .into_query_result()?;
    println!("Final state:");
    print_query_result(&simple_result);

    // Database file is automatically cleaned up when temp_file goes out of scope
    Ok(())
}

fn print_query_result(result: &tegdb::QueryResult) {
    println!("Columns: {:?}", result.columns());
    println!("Rows: {}", result.rows().len());

    for row in result.rows().iter() {
        let id_pos = result.columns().iter().position(|c| c == "id").unwrap();
        let name_pos = result.columns().iter().position(|c| c == "name").unwrap();
        let age_pos = result.columns().iter().position(|c| c == "age").unwrap();

        let id = match &row[id_pos] {
            SqlValue::Integer(i) => *i,
            _ => 0,
        };
        let name = match &row[name_pos] {
            SqlValue::Text(s) => s.clone(),
            _ => "Unknown".to_string(),
        };
        let age = match &row[age_pos] {
            SqlValue::Integer(i) => *i,
            _ => 0,
        };

        println!("  ID: {}, Name: {}, Age: {}", id, name, age);
    }
}
