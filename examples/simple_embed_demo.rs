//! Simple demonstration of EMBED functionality using the high-level Database API

use tegdb::{Database, Result};

fn main() -> Result<()> {
    let temp_dir = tempfile::tempdir().unwrap();
    let db_path = temp_dir.path().join("simple_embed.db");
    let db_file = format!("file://{}", db_path.display());

    println!("=== Simple EMBED Demo ===\n");

    let mut db = Database::open(&db_file)?;

    // Create a simple table
    println!("1. Creating table...");
    db.execute("CREATE TABLE test (id INTEGER PRIMARY KEY, vec VECTOR(128))")?;
    println!("   ✓ Table created\n");

    // Try inserting without EMBED first
    println!("2. Inserting a manual vector...");
    let manual_vec = vec![0.1; 128];
    let vec_str = format!(
        "[{}]",
        manual_vec
            .iter()
            .map(|v| v.to_string())
            .collect::<Vec<_>>()
            .join(",")
    );
    db.execute(&format!(
        "INSERT INTO test (id, vec) VALUES (1, {vec_str})"
    ))?;
    println!("   ✓ Manual vector inserted\n");

    // Now try with EMBED
    println!("3. Inserting with EMBED function...");
    let result = db.execute("INSERT INTO test (id, vec) VALUES (2, EMBED('hello world'))");
    match result {
        Ok(count) => println!("   ✓ EMBED insert succeeded, {count} rows affected\n"),
        Err(e) => {
            println!("   ✗ EMBED insert failed: {e:?}\n");
            println!("   Note: This is expected if EMBED evaluation has issues\n");
        }
    }

    // Try SELECT with EMBED
    println!("4. Testing EMBED in SELECT...");
    let result = db.query("SELECT EMBED('test query') as embedding");
    match result {
        Ok(rows) => println!(
            "   ✓ EMBED in SELECT succeeded, got {} rows\n",
            rows.rows().len()
        ),
        Err(e) => println!("   ✗ EMBED in SELECT failed: {e:?}\n"),
    }

    println!("=== Demo Complete ===");

    Ok(())
}
