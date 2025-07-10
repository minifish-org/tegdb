//! Benchmark comparing streaming vs non-streaming approaches

use criterion::{criterion_group, criterion_main, Criterion};
use std::hint::black_box;
use tegdb::SqlValue;

// Simulate the performance difference between streaming and non-streaming approaches
fn benchmark_streaming_vs_traditional(c: &mut Criterion) {
    // Create test data
    let test_data: Vec<Vec<SqlValue>> = (0..10000)
        .map(|i| {
            vec![
                SqlValue::Integer(i),
                SqlValue::Text(format!("User{i}")),
                SqlValue::Integer(20 + (i % 50)),
            ]
        })
        .collect();

    let mut group = c.benchmark_group("streaming_vs_traditional");

    // Traditional approach: load all data into memory
    group.bench_function("traditional_load_all", |b| {
        b.iter(|| {
            let mut result = Vec::new();
            for row in &test_data {
                result.push(row.clone());
            }
            black_box(result)
        })
    });

    // Streaming approach: process one row at a time
    group.bench_function("streaming_process_one_by_one", |b| {
        b.iter(|| {
            let mut count = 0;
            for row in &test_data {
                // Simulate processing without storing
                let _processed = black_box(row);
                count += 1;
            }
            black_box(count)
        })
    });

    // Traditional with filtering: load all, then filter
    group.bench_function("traditional_load_then_filter", |b| {
        b.iter(|| {
            let mut all_rows = Vec::new();
            for row in &test_data {
                all_rows.push(row.clone());
            }

            let filtered: Vec<_> = all_rows
                .into_iter()
                .filter(|row| {
                    if let Some(SqlValue::Integer(age)) = row.get(2) {
                        *age > 30
                    } else {
                        false
                    }
                })
                .collect();

            black_box(filtered)
        })
    });

    // Streaming with filtering: filter during iteration
    group.bench_function("streaming_filter_during_iteration", |b| {
        b.iter(|| {
            let mut filtered_count = 0;
            for row in &test_data {
                if let Some(SqlValue::Integer(age)) = row.get(2) {
                    if *age > 30 {
                        let _processed = black_box(row);
                        filtered_count += 1;
                    }
                }
            }
            black_box(filtered_count)
        })
    });

    // Traditional with LIMIT: load all, then take first N
    group.bench_function("traditional_load_then_limit", |b| {
        b.iter(|| {
            let mut all_rows = Vec::new();
            for row in &test_data {
                all_rows.push(row.clone());
            }

            let limited: Vec<_> = all_rows.into_iter().take(100).collect();
            black_box(limited)
        })
    });

    // Streaming with LIMIT: stop after N rows
    group.bench_function("streaming_early_termination", |b| {
        b.iter(|| {
            let mut count = 0;
            for row in &test_data {
                if count >= 100 {
                    break;
                }
                let _processed = black_box(row);
                count += 1;
            }
            black_box(count)
        })
    });

    group.finish();
}

// Memory usage simulation
fn benchmark_memory_usage(c: &mut Criterion) {
    let test_data: Vec<Vec<SqlValue>> = (0..50000)
        .map(|i| {
            vec![
                SqlValue::Integer(i),
                SqlValue::Text(format!("Data{i}")),
                SqlValue::Real(i as f64 * 1.5),
            ]
        })
        .collect();

    let mut group = c.benchmark_group("memory_usage");

    // Traditional: calculate average by loading all data
    group.bench_function("traditional_average", |b| {
        b.iter(|| {
            let mut all_values = Vec::new();
            for row in &test_data {
                if let Some(SqlValue::Real(val)) = row.get(2) {
                    all_values.push(*val);
                }
            }

            let sum: f64 = all_values.iter().sum();
            let avg = sum / all_values.len() as f64;
            black_box(avg)
        })
    });

    // Streaming: calculate average without storing all values
    group.bench_function("streaming_average", |b| {
        b.iter(|| {
            let mut sum = 0.0;
            let mut count = 0;

            for row in &test_data {
                if let Some(SqlValue::Real(val)) = row.get(2) {
                    sum += val;
                    count += 1;
                }
            }

            let avg = sum / count as f64;
            black_box(avg)
        })
    });

    group.finish();
}

criterion_group!(
    benches,
    benchmark_streaming_vs_traditional,
    benchmark_memory_usage
);
criterion_main!(benches);
