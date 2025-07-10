use criterion::{black_box, criterion_group, criterion_main, BenchmarkId, Criterion};
use tegdb::parser::*;

fn simple_sql_statements() -> Vec<(&'static str, &'static str)> {
    vec![
        ("simple_select", "SELECT id FROM users"),
        ("simple_insert", "INSERT INTO users (name) VALUES ('John')"),
        (
            "simple_update",
            "UPDATE users SET name = 'Jane' WHERE id = 1",
        ),
        ("simple_delete", "DELETE FROM users WHERE id = 1"),
        (
            "create_table",
            "CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)",
        ),
        ("drop_table", "DROP TABLE users"),
        ("begin_transaction", "BEGIN"),
        ("commit_transaction", "COMMIT"),
        ("rollback_transaction", "ROLLBACK"),
    ]
}

fn complex_sql_statements() -> Vec<(&'static str, &'static str)> {
    vec![
        (
            "complex_select",
            "SELECT u.id, u.name, p.title FROM users u WHERE u.age > 25 AND u.status = 'active' LIMIT 100"
        ),
        (
            "multi_insert",
            "INSERT INTO products (name, price, category) VALUES ('Product1', 29.99, 'Electronics'), ('Product2', 39.99, 'Books'), ('Product3', 19.99, 'Clothing')"
        ),
        (
            "complex_update",
            "UPDATE inventory SET quantity = quantity - 1, last_updated = '2023-01-01' WHERE product_id = 123 AND quantity > 0"
        ),
        (
            "complex_delete",
            "DELETE FROM audit_logs WHERE created_at < '2022-01-01' AND log_level = 'DEBUG'"
        ),
        (
            "complex_create_table",
            "CREATE TABLE orders (id INTEGER PRIMARY KEY, customer_id INTEGER, order_date TEXT, total_amount REAL, status TEXT, shipping_address TEXT)"
        ),
        (
            "select_with_like",
            "SELECT * FROM customers WHERE email LIKE '%@gmail.com' AND name LIKE 'John%'"
        ),
        (
            "select_multiple_columns",
            "SELECT id, first_name, last_name, email, phone, address, city, state, zip_code, country FROM customers"
        ),
    ]
}

fn large_sql_statements() -> Vec<(&'static str, String)> {
    vec![
        (
            "large_insert",
            format!(
                "INSERT INTO test_table (col1, col2, col3, col4, col5) VALUES {}",
                (0..50)
                    .map(|i| format!("({i}, 'text{i}', {i}.5, 'value{i}', 'data{i}')"))
                    .collect::<Vec<_>>()
                    .join(", ")
            )
        ),
        (
            "select_many_columns",
            format!(
                "SELECT {} FROM large_table WHERE id > 1000 LIMIT 1000",
                (1..=30).map(|i| format!("col{i}")).collect::<Vec<_>>().join(", ")
            )
        ),
        (
            "complex_where_clause",
            "SELECT * FROM events WHERE (event_type = 'click' OR event_type = 'view') AND timestamp > '2023-01-01' AND (user_id = 123 OR user_id = 456 OR user_id = 789) AND status != 'deleted'".to_string()
        ),
    ]
}

fn bench_simple_statements(c: &mut Criterion) {
    let mut group = c.benchmark_group("parser_simple");

    for (name, sql) in simple_sql_statements() {
        group.bench_with_input(BenchmarkId::new("parse", name), sql, |b, sql| {
            b.iter(|| {
                let result = parse_sql(black_box(sql));
                black_box(result)
            })
        });
    }
    group.finish();
}

fn bench_complex_statements(c: &mut Criterion) {
    let mut group = c.benchmark_group("parser_complex");

    for (name, sql) in complex_sql_statements() {
        group.bench_with_input(BenchmarkId::new("parse", name), sql, |b, sql| {
            b.iter(|| {
                let result = parse_sql(black_box(sql));
                black_box(result)
            })
        });
    }
    group.finish();
}

fn bench_large_statements(c: &mut Criterion) {
    let mut group = c.benchmark_group("parser_large");

    for (name, sql) in large_sql_statements() {
        group.bench_with_input(BenchmarkId::new("parse", name), &sql, |b, sql| {
            b.iter(|| {
                let result = parse_sql(black_box(sql));
                black_box(result)
            })
        });
    }
    group.finish();
}

fn bench_repeated_parsing(c: &mut Criterion) {
    let sql_statements = vec![
        "SELECT * FROM users WHERE id = 1",
        "INSERT INTO products (name, price) VALUES ('Test', 19.99)",
        "UPDATE inventory SET quantity = 100 WHERE product_id = 1",
        "DELETE FROM logs WHERE created_at < '2023-01-01'",
    ];

    let mut group = c.benchmark_group("parser_repeated");

    for count in [10, 100, 1000].iter() {
        group.bench_with_input(
            BenchmarkId::new("batch_parse", count),
            count,
            |b, &count| {
                b.iter(|| {
                    for _ in 0..count {
                        for sql in &sql_statements {
                            let result = parse_sql(black_box(sql));
                            let _ = black_box(result);
                        }
                    }
                })
            },
        );
    }
    group.finish();
}

fn bench_statement_validation(c: &mut Criterion) {
    let valid_statements = vec![
        "SELECT id FROM users",
        "INSERT INTO users (name) VALUES ('John')",
        "UPDATE users SET name = 'Jane'",
        "DELETE FROM users WHERE id = 1",
    ];

    let invalid_statements = vec![
        "INVALID SQL STATEMENT",
        "SELECT FROM WHERE",
        "INSERT INTO",
        "UPDATE SET",
        "DELETE FROM",
    ];

    let mut group = c.benchmark_group("parser_validation");

    group.bench_function("valid_statements", |b| {
        b.iter(|| {
            for sql in &valid_statements {
                let result = parse_sql(black_box(sql));
                let _ = black_box(result);
            }
        })
    });

    group.bench_function("invalid_statements", |b| {
        b.iter(|| {
            for sql in &invalid_statements {
                let result = parse_sql(black_box(sql));
                let _ = black_box(result);
            }
        })
    });

    group.finish();
}

fn bench_memory_usage(c: &mut Criterion) {
    // Test parsing with different data types and sizes
    let statements_by_size = vec![
        ("small_text", "SELECT 'small' FROM table".to_string()),
        (
            "medium_text",
            format!("SELECT '{}' FROM table", "a".repeat(100)),
        ),
        (
            "large_text",
            format!("SELECT '{}' FROM table", "a".repeat(1000)),
        ),
        (
            "many_integers",
            format!(
                "SELECT {} FROM table",
                (1..=50)
                    .map(|i| i.to_string())
                    .collect::<Vec<_>>()
                    .join(", ")
            ),
        ),
        (
            "many_floats",
            format!(
                "SELECT {} FROM table",
                (1..=50)
                    .map(|i| format!("{i}.5"))
                    .collect::<Vec<_>>()
                    .join(", ")
            ),
        ),
    ];

    let mut group = c.benchmark_group("parser_memory");

    for (name, sql) in statements_by_size {
        group.bench_with_input(BenchmarkId::new("parse", name), &sql, |b, sql| {
            b.iter(|| {
                let result = parse_sql(black_box(sql));
                // Ensure the parsed result is fully materialized
                if let Ok((_, statement)) = result {
                    black_box(statement);
                }
            })
        });
    }
    group.finish();
}

criterion_group!(
    benches,
    bench_simple_statements,
    bench_complex_statements,
    bench_large_statements,
    bench_repeated_parsing,
    bench_statement_validation,
    bench_memory_usage
);
criterion_main!(benches);
