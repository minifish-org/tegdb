use criterion::{black_box, criterion_group, criterion_main, Criterion};
use rusqlite::{params, Connection};
use std::path::PathBuf;
use std::env;
use std::fs;

/// Creates a unique temporary file path for benchmarks
fn temp_db_path(prefix: &str) -> PathBuf {
    let mut path = env::temp_dir();
    path.push(format!("tegdb_simple_{}_{}", prefix, std::process::id()));
    path
}

/// Simple test to verify TegDB's query planner is working
fn simple_planner_test(c: &mut Criterion) {
    let tegdb_path = temp_db_path("tegdb");
    let sqlite_path = temp_db_path("sqlite");
    
    // Clean up existing files
    let _ = fs::remove_file(&tegdb_path);
    let _ = fs::remove_file(&sqlite_path);
    
    // Setup TegDB
    let mut tegdb = tegdb::Database::open(&tegdb_path).expect("Failed to create TegDB database");
    
    // Setup SQLite
    let sqlite = Connection::open(&sqlite_path).unwrap();
    
    // Create simple tables
    let create_table_sql = "CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)";
    
    tegdb.execute(create_table_sql).expect("Failed to create TegDB table");
    sqlite.execute(create_table_sql, []).unwrap();
    
    // Insert small amount of test data
    for i in 1..=100 {
        let insert_sql = format!("INSERT INTO users (id, name) VALUES ({}, 'User {}')", i, i);
        tegdb.execute(&insert_sql).unwrap();
        sqlite.execute("INSERT INTO users (id, name) VALUES (?, ?)", params![i, format!("User {}", i)]).unwrap();
    }
    
    println!("Setup complete. Running simple primary key lookup tests...");
    
    // Test primary key lookup - should show TegDB's planner optimization
    c.bench_function("tegdb_pk_lookup", |b| {
        b.iter(|| {
            let id = black_box(50);
            let result = tegdb.query(&format!("SELECT name FROM users WHERE id = {}", id)).unwrap();
            black_box(result);
        });
    });
    
    c.bench_function("sqlite_pk_lookup", |b| {
        b.iter(|| {
            let id = black_box(50);
            let mut stmt = sqlite.prepare("SELECT name FROM users WHERE id = ?").unwrap();
            let mut rows = stmt.query([id]).unwrap();
            let mut results = Vec::new();
            while let Some(row) = rows.next().unwrap() {
                results.push(row.get::<_, String>(0).unwrap());
            }
            black_box(results);
        });
    });
    
    // Clean up
    drop(tegdb);
    drop(sqlite);
    let _ = fs::remove_file(&tegdb_path);
    let _ = fs::remove_file(&sqlite_path);
    
    println!("Simple planner test completed!");
}

criterion_group!(simple_benches, simple_planner_test);
criterion_main!(simple_benches);
