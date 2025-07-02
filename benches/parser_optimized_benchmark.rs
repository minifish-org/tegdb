use criterion::{black_box, criterion_group, criterion_main, BenchmarkId, Criterion};
use tegdb::parser::*;

fn optimized_scenarios() -> Vec<(&'static str, &'static str)> {
    vec![
        // Fast path scenarios (transaction commands)
        ("begin_fast", "BEGIN"),
        ("commit_fast", "COMMIT"),
        ("rollback_fast", "ROLLBACK"),

        // Optimized identifier scenarios
        ("short_identifier", "SELECT id FROM users"),
        ("long_identifier", "SELECT very_long_column_name_that_exceeds_sixteen_characters FROM table_name"),

        // Optimized number parsing
        ("small_numbers", "SELECT * FROM users WHERE id = 1 AND age = 25"),
        ("large_numbers", "SELECT * FROM users WHERE id = 123456789 AND balance = -9876543210"),

        // String literal scenarios
        ("short_strings", "INSERT INTO users (name) VALUES ('Bob')"),
        ("long_strings", "INSERT INTO users (description) VALUES ('This is a very long string that should test the string literal parsing optimization paths')"),

        // Comparison operator optimization
        ("equal_operator", "SELECT * FROM users WHERE id = 1"),
        ("like_operator", "SELECT * FROM users WHERE name LIKE 'John%'"),
        ("complex_comparison", "SELECT * FROM users WHERE age >= 18 AND age <= 65"),

        // Column list optimization
        ("single_column", "SELECT id FROM users"),
        ("many_columns", "SELECT id, name, email, phone, address, city, state, zip, country, created_at, updated_at, status FROM users"),
    ]
}

fn bench_optimized_parsing(c: &mut Criterion) {
    let mut group = c.benchmark_group("parser_optimized");

    for (name, sql) in optimized_scenarios() {
        group.bench_with_input(BenchmarkId::new("parse", name), sql, |b, sql| {
            b.iter(|| {
                let result = parse_sql(black_box(sql));
                black_box(result)
            })
        });
    }
    group.finish();
}

fn bench_optimization_impact(c: &mut Criterion) {
    let test_cases = vec![
        ("transaction_batch", vec!["BEGIN", "COMMIT", "ROLLBACK"]),
        (
            "identifier_batch",
            vec![
                "SELECT id FROM users",
                "SELECT user_id FROM orders",
                "SELECT very_long_column_name FROM table",
            ],
        ),
        (
            "number_batch",
            vec![
                "SELECT * FROM t WHERE id = 1",
                "SELECT * FROM t WHERE id = 42",
                "SELECT * FROM t WHERE id = 999",
            ],
        ),
    ];

    let mut group = c.benchmark_group("optimization_impact");

    for (batch_name, statements) in test_cases {
        group.bench_function(batch_name, |b| {
            b.iter(|| {
                for sql in &statements {
                    let result = parse_sql(black_box(sql));
                    let _ = black_box(result);
                }
            })
        });
    }
    group.finish();
}

fn bench_memory_efficiency(c: &mut Criterion) {
    let repeated_identifier = "user_id"; // Should benefit from string interning
    let unique_identifiers = (0..100).map(|i| format!("column_{i}")).collect::<Vec<_>>();

    let mut group = c.benchmark_group("memory_efficiency");

    // Test string interning benefit
    group.bench_function("repeated_identifiers", |b| {
        let sql =
            format!("SELECT {repeated_identifier} FROM table WHERE {repeated_identifier} = 1");
        b.iter(|| {
            for _ in 0..10 {
                let result = parse_sql(black_box(&sql));
                let _ = black_box(result);
            }
        })
    });

    // Test unique identifier performance
    group.bench_function("unique_identifiers", |b| {
        b.iter(|| {
            for identifier in &unique_identifiers[..10] {
                let sql = format!("SELECT {identifier} FROM table");
                let result = parse_sql(black_box(&sql));
                let _ = black_box(result);
            }
        })
    });

    group.finish();
}

criterion_group!(
    optimized_benches,
    bench_optimized_parsing,
    bench_optimization_impact,
    bench_memory_efficiency
);
criterion_main!(optimized_benches);
