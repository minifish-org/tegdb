use std::env;
use std::fs;
use std::path::PathBuf;
use std::time::Instant;

fn temp_db_path(prefix: &str) -> PathBuf {
    let mut path = env::temp_dir();
    path.push(format!(
        "tegdb_limit_profile_{}_{}",
        prefix,
        std::process::id()
    ));
    path
}

fn main() {
    let tegdb_path = temp_db_path("tegdb");
    let _ = fs::remove_file(&tegdb_path);

    // Setup TegDB
    let mut tegdb = tegdb::Database::open(format!("file://{}", tegdb_path.display()))
        .expect("Failed to create TegDB database");

    // Create table
    let create_table_sql = "CREATE TABLE items (
        id INTEGER PRIMARY KEY, 
        name TEXT(32) NOT NULL, 
        category TEXT(32) NOT NULL,
        value INTEGER NOT NULL
    )";
    tegdb
        .execute(create_table_sql)
        .expect("Failed to create TegDB table");

    // Insert test data
    for i in 1..=1000 {
        let insert_sql = format!(
            "INSERT INTO items (id, name, category, value) VALUES ({}, 'Item {}', '{}', {})",
            i,
            i,
            if i % 3 == 0 {
                "A"
            } else if i % 3 == 1 {
                "B"
            } else {
                "C"
            },
            (i * 7) % 100
        );
        tegdb.execute(&insert_sql).unwrap();
    }

    // Prepare statement
    let stmt = tegdb
        .prepare("SELECT id, name FROM items WHERE id = ?1")
        .unwrap();

    // Profile loop
    let start = Instant::now();
    for i in 1..=50_000_000 {
        let param = vec![tegdb::SqlValue::Integer(((i % 1000) + 1) as i64)];
        let result = tegdb.query_prepared(&stmt, &param).unwrap();
        std::hint::black_box(&result);
    }
    let elapsed = start.elapsed();
    println!("Profiled 10,000,000 PK lookup queries in {elapsed:?}");

    // Clean up
    drop(stmt);
    drop(tegdb);
    let _ = fs::remove_file(&tegdb_path);
}
