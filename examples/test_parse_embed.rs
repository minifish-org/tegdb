//! Test parsing EMBED function

use tegdb::parser::parse_sql;
use tegdb::Result;

fn main() -> Result<()> {
    println!("Testing EMBED parsing...\n");

    // Test 1: Simple EMBED in SELECT
    println!("1. Parsing: SELECT EMBED('hello')");
    match parse_sql("SELECT EMBED('hello')") {
        Ok(stmt) => println!("   ✓ Parsed successfully: {stmt:?}\n"),
        Err(e) => println!("   ✗ Parse error: {e}\n"),
    }

    // Test 2: EMBED with model
    println!("2. Parsing: SELECT EMBED('hello', 'simple')");
    match parse_sql("SELECT EMBED('hello', 'simple')") {
        Ok(_stmt) => println!("   ✓ Parsed successfully\n"),
        Err(e) => println!("   ✗ Parse error: {e}\n"),
    }

    // Test 3: INSERT with EMBED
    println!("3. Parsing: INSERT INTO test (id, vec) VALUES (1, EMBED('hello'))");
    match parse_sql("INSERT INTO test (id, vec) VALUES (1, EMBED('hello'))") {
        Ok(_stmt) => println!("   ✓ Parsed successfully\n"),
        Err(e) => println!("   ✗ Parse error: {e}\n"),
    }

    // Test 4: Nested function
    println!("4. Parsing: SELECT COSINE_SIMILARITY(EMBED('a'), EMBED('b'))");
    match parse_sql("SELECT COSINE_SIMILARITY(EMBED('a'), EMBED('b'))") {
        Ok(_stmt) => println!("   ✓ Parsed successfully\n"),
        Err(e) => println!("   ✗ Parse error: {e}\n"),
    }

    Ok(())
}
