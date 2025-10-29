use criterion::{criterion_group, criterion_main, Criterion};
use std::fs;
use std::hint::black_box;
use tegdb::{Database, Result};

fn setup_test_database() -> Result<Database> {
    // Clean up any existing test database
    let _ = fs::remove_file("/tmp/bench_optimizer_test.teg");

    let mut db = Database::open("file:///tmp/bench_optimizer_test.teg")?;

    // Create a table with composite primary key
    db.execute("DROP TABLE IF EXISTS products")?;
    db.execute(
        "CREATE TABLE products (
            category TEXT(32) PRIMARY KEY,
            product_id INTEGER PRIMARY KEY,
            name TEXT(32) NOT NULL,
            price REAL,
            description TEXT(32)
        )",
    )?;

    // Insert test data (smaller dataset for faster benchmarking)
    let categories = ["electronics", "books", "clothing"];
    for category in &categories {
        for product_id in 1..=100 {
            let name = format!("{category} Product {product_id}");
            let price = 10.0 + (product_id as f64 * 0.1);
            let description = format!("Description for {category} product {product_id}");

            db.execute(&format!(
                "INSERT INTO products (category, product_id, name, price, description) 
                 VALUES ('{category}', {product_id}, '{name}', {price}, '{description}')"
            ))?;
        }
    }

    Ok(db)
}

fn bench_optimizer_comparison(c: &mut Criterion) {
    let mut group = c.benchmark_group("query_optimizer");

    // Setup database once and reuse
    let mut db = setup_test_database().expect("Failed to setup test database");

    // PK lookup (optimized) - should be very fast
    group.bench_function("pk_exact_match", |b| {
        b.iter(|| {
            let result = db
                .query(black_box(
                    "SELECT * FROM products WHERE category = 'electronics' AND product_id = 42",
                ))
                .unwrap();
            black_box(result);
        });
    });

    // Partial PK (not optimized) - should be slower
    group.bench_function("partial_pk_scan", |b| {
        b.iter(|| {
            let result = db
                .query(black_box(
                    "SELECT * FROM products WHERE category = 'electronics'",
                ))
                .unwrap();
            black_box(result);
        });
    });

    // Non-PK condition (not optimized) - should be slower
    group.bench_function("non_pk_scan", |b| {
        b.iter(|| {
            let result = db
                .query(black_box("SELECT * FROM products WHERE price > 15.0"))
                .unwrap();
            black_box(result);
        });
    });

    group.finish();

    // Clean up
    let _ = fs::remove_file("/tmp/bench_optimizer_test.teg");
}

criterion_group!(benches, bench_optimizer_comparison);
criterion_main!(benches);
