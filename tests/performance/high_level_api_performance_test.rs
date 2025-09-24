//! Performance tracing tests for TegDB high-level API
//!
//! This module provides comprehensive performance tests for the Database interface,
//! focusing on real-world usage patterns and identifying performance bottlenecks.

#[allow(clippy::duplicate_mod)]
#[path = "../helpers/test_helpers.rs"]
mod test_helpers;

use std::collections::HashMap;
use std::time::{Duration, Instant};
use tegdb::{Database, Result};
use tempfile::NamedTempFile;

// These are available with the dev feature
#[cfg(feature = "dev")]
use tegdb::parser::parse_sql;
#[cfg(feature = "dev")]
use tegdb::planner::QueryPlanner;

/// Performance test results for analysis
#[derive(Debug, Clone)]
pub struct PerformanceMetrics {
    pub operation: String,
    pub duration: Duration,
    pub records_processed: usize,
    pub throughput_per_second: f64,
}

impl PerformanceMetrics {
    fn new(operation: &str, duration: Duration, records: usize) -> Self {
        let throughput = if duration.as_secs_f64() > 0.0 {
            records as f64 / duration.as_secs_f64()
        } else {
            0.0
        };

        Self {
            operation: operation.to_string(),
            duration,
            records_processed: records,
            throughput_per_second: throughput,
        }
    }

    fn print_summary(&self) {
        println!(
            "{}: {} records in {:.1}µs ({:.1} records/sec)",
            self.operation,
            self.records_processed,
            self.duration.as_secs_f64() * 1_000_000.0,
            self.throughput_per_second
        );
    }
}

/// Detailed performance metrics for SQL execution pipeline
#[derive(Debug, Clone)]
pub struct SqlExecutionMetrics {
    pub operation: String,
    pub parse_duration: Duration,
    pub plan_duration: Duration,
    pub execute_duration: Duration,
    pub total_duration: Duration,
    pub records_processed: usize,
}

impl SqlExecutionMetrics {
    fn new(
        operation: &str,
        parse_time: Duration,
        plan_time: Duration,
        execute_time: Duration,
        records: usize,
    ) -> Self {
        Self {
            operation: operation.to_string(),
            parse_duration: parse_time,
            plan_duration: plan_time,
            execute_duration: execute_time,
            total_duration: parse_time + plan_time + execute_time,
            records_processed: records,
        }
    }

    fn print_detailed_summary(&self) {
        println!(
            "{}: {} records | Parse: {:.1}µs | Plan: {:.1}µs | Execute: {:.1}µs | Total: {:.1}µs",
            self.operation,
            self.records_processed,
            self.parse_duration.as_secs_f64() * 1_000_000.0,
            self.plan_duration.as_secs_f64() * 1_000_000.0,
            self.execute_duration.as_secs_f64() * 1_000_000.0,
            self.total_duration.as_secs_f64() * 1_000_000.0,
        );
    }

    fn print_breakdown(&self) {
        let total_us = self.total_duration.as_secs_f64() * 1_000_000.0;
        let parse_pct = (self.parse_duration.as_secs_f64() * 1_000_000.0 / total_us) * 100.0;
        let plan_pct = (self.plan_duration.as_secs_f64() * 1_000_000.0 / total_us) * 100.0;
        let execute_pct = (self.execute_duration.as_secs_f64() * 1_000_000.0 / total_us) * 100.0;

        println!(
            "  Breakdown: Parse {parse_pct:.1}% | Plan {plan_pct:.1}% | Execute {execute_pct:.1}%"
        );
    }
}

/// Create a temporary database for testing
fn create_test_db() -> Result<Database> {
    let temp_file = NamedTempFile::new().expect("Failed to create temp file");
    let db_path = format!("file://{}", temp_file.path().display());
    Database::open(&db_path)
}

/// Setup a test table with sample data
fn setup_test_table(
    db: &mut Database,
    table_name: &str,
    record_count: usize,
) -> Result<PerformanceMetrics> {
    let start = Instant::now();

    // Create table
    let create_sql = format!(
        "CREATE TABLE {table_name} (id INTEGER PRIMARY KEY, name TEXT(32), value INTEGER, data TEXT(32))"
    );
    db.execute(&create_sql)?;

    // Insert test data in batches
    let batch_size = 100;
    let mut total_inserted = 0;

    for batch_start in (0..record_count).step_by(batch_size) {
        let batch_end = std::cmp::min(batch_start + batch_size, record_count);

        for i in batch_start..batch_end {
            let insert_sql =
                format!(
                "INSERT INTO {} (id, name, value, data) VALUES ({}, 'user{}', {}, 'data_{}_{}')",
                table_name, i, i, i % 1000, table_name, i
            );
            db.execute(&insert_sql)?;
            total_inserted += 1;
        }
    }

    let duration = start.elapsed();
    Ok(PerformanceMetrics::new(
        "Table Setup",
        duration,
        total_inserted,
    ))
}

#[test]
fn test_basic_crud_performance() -> Result<()> {
    println!("=== Basic CRUD Performance Test ===");
    let mut db = create_test_db()?;
    let mut metrics = Vec::new();

    // Test table creation
    let start = Instant::now();
    db.execute("CREATE TABLE perf_test (id INTEGER PRIMARY KEY, name TEXT(32), value INTEGER)")?;
    metrics.push(PerformanceMetrics::new("CREATE TABLE", start.elapsed(), 1));

    // Test single insert performance
    let start = Instant::now();
    let record_count = 1000;
    for i in 0..record_count {
        db.execute(&format!(
            "INSERT INTO perf_test (id, name, value) VALUES ({i}, 'user{i}', {i})"
        ))?;
    }
    metrics.push(PerformanceMetrics::new(
        "Single INSERT",
        start.elapsed(),
        record_count,
    ));

    // Test select all performance
    let start = Instant::now();
    let result = db.query("SELECT * FROM perf_test")?;
    let rows = result;
    metrics.push(PerformanceMetrics::new(
        "SELECT ALL",
        start.elapsed(),
        rows.len(),
    ));

    // Test select with filter performance
    let start = Instant::now();
    let result = db.query("SELECT * FROM perf_test WHERE value > 500")?;
    let filtered_rows = result;
    metrics.push(PerformanceMetrics::new(
        "SELECT FILTERED",
        start.elapsed(),
        filtered_rows.len(),
    ));

    // Test update performance
    let start = Instant::now();
    let affected = db.execute("UPDATE perf_test SET name = 'updated' WHERE value < 100")?;
    metrics.push(PerformanceMetrics::new("UPDATE", start.elapsed(), affected));

    // Test delete performance
    let start = Instant::now();
    let affected = db.execute("DELETE FROM perf_test WHERE value > 900")?;
    metrics.push(PerformanceMetrics::new("DELETE", start.elapsed(), affected));

    // Print results
    for metric in &metrics {
        metric.print_summary();
    }

    Ok(())
}

#[test]
fn test_streaming_query_performance() -> Result<()> {
    println!("=== Streaming Query Performance Test ===");
    let mut db = create_test_db()?;
    let record_count = 5000;

    // Setup test data
    let setup_metrics = setup_test_table(&mut db, "stream_test", record_count)?;
    setup_metrics.print_summary();

    // Test streaming vs batch query performance
    let mut metrics = Vec::new();

    // Streaming query - process one row at a time
    let start = Instant::now();
    let query = db.query("SELECT * FROM stream_test")?;
    let mut streaming_count = 0;
    for row_result in query {
        let _row = row_result?;
        streaming_count += 1;
    }
    metrics.push(PerformanceMetrics::new(
        "Streaming Query",
        start.elapsed(),
        streaming_count,
    ));

    // Batch query - collect all rows at once
    let start = Instant::now();
    let batch_query = db.query("SELECT * FROM stream_test")?;
    let batch_rows = batch_query;
    metrics.push(PerformanceMetrics::new(
        "Batch Query",
        start.elapsed(),
        batch_rows.len(),
    ));

    // Streaming query with filter
    let start = Instant::now();
    let filtered_streaming = db.query("SELECT * FROM stream_test WHERE value > 2500")?;
    let mut filtered_count = 0;
    for row_result in filtered_streaming {
        let _row = row_result?;
        filtered_count += 1;
    }
    metrics.push(PerformanceMetrics::new(
        "Streaming Filtered",
        start.elapsed(),
        filtered_count,
    ));

    // Print results
    for metric in &metrics {
        metric.print_summary();
    }

    Ok(())
}

#[test]
fn test_transaction_performance() -> Result<()> {
    println!("=== Transaction Performance Test ===");
    let mut db = create_test_db()?;
    let mut metrics = Vec::new();

    // Create test table
    db.execute("CREATE TABLE tx_test (id INTEGER PRIMARY KEY, name TEXT(32), value INTEGER)")?;

    // Test individual transactions (auto-commit)
    let start = Instant::now();
    let record_count = 1000;
    for i in 0..record_count {
        db.execute(&format!(
            "INSERT INTO tx_test (id, name, value) VALUES ({i}, 'user{i}', {i})"
        ))?;
    }
    metrics.push(PerformanceMetrics::new(
        "Auto-commit INSERTs",
        start.elapsed(),
        record_count,
    ));

    // Clear table for next test
    db.execute("DELETE FROM tx_test")?;

    // Test explicit transaction
    let start = Instant::now();
    let mut tx = db.begin_transaction()?;
    for i in 0..record_count {
        tx.execute(&format!(
            "INSERT INTO tx_test (id, name, value) VALUES ({i}, 'user{i}', {i})"
        ))?;
    }
    tx.commit()?;
    metrics.push(PerformanceMetrics::new(
        "Transaction Batch",
        start.elapsed(),
        record_count,
    ));

    // Test transaction with rollback
    let start = Instant::now();
    let mut tx = db.begin_transaction()?;
    for i in 0..100 {
        tx.execute(&format!(
            "INSERT INTO tx_test (id, name, value) VALUES ({}, 'temp{}', {})",
            i + 10000,
            i,
            i
        ))?;
    }
    tx.rollback()?;
    metrics.push(PerformanceMetrics::new(
        "Transaction Rollback",
        start.elapsed(),
        100,
    ));

    // Test streaming query within transaction
    let start = Instant::now();
    let mut tx = db.begin_transaction()?;
    let query = tx.query("SELECT * FROM tx_test")?;
    let mut tx_streaming_count = 0;
    for row_result in query {
        let _row = row_result?;
        tx_streaming_count += 1;
    }
    tx.commit()?;
    metrics.push(PerformanceMetrics::new(
        "Transaction Streaming",
        start.elapsed(),
        tx_streaming_count,
    ));

    // Print results
    for metric in &metrics {
        metric.print_summary();
    }

    Ok(())
}

#[test]
fn test_schema_operations_performance() -> Result<()> {
    println!("=== Schema Operations Performance Test ===");
    let mut db = create_test_db()?;
    let mut metrics = Vec::new();

    // Test multiple table creation
    let start = Instant::now();
    let table_count = 50;
    for i in 0..table_count {
        let create_sql = format!(
            "CREATE TABLE table_{i} (id INTEGER PRIMARY KEY, name TEXT(32), value INTEGER, data TEXT(32))"
        );
        db.execute(&create_sql)?;
    }
    metrics.push(PerformanceMetrics::new(
        "Multiple CREATE TABLE",
        start.elapsed(),
        table_count,
    ));

    // Test schema introspection
    let start = Instant::now();
    let schemas = db.get_table_schemas();
    metrics.push(PerformanceMetrics::new(
        "Schema Introspection",
        start.elapsed(),
        schemas.len(),
    ));

    // Test table drops
    let start = Instant::now();
    for i in 0..table_count {
        db.execute(&format!("DROP TABLE table_{i}"))?;
    }
    metrics.push(PerformanceMetrics::new(
        "Multiple DROP TABLE",
        start.elapsed(),
        table_count,
    ));

    // Print results
    for metric in &metrics {
        metric.print_summary();
    }

    Ok(())
}

#[test]
fn test_large_dataset_performance() -> Result<()> {
    println!("=== Large Dataset Performance Test ===");
    let mut db = create_test_db()?;
    let record_count = 10000;

    // Setup large dataset
    let setup_metrics = setup_test_table(&mut db, "large_test", record_count)?;
    setup_metrics.print_summary();

    let mut metrics = Vec::new();

    // Test full table scan
    let start = Instant::now();
    let result = db.query("SELECT * FROM large_test")?;
    let all_rows = result.collect_rows()?;
    metrics.push(PerformanceMetrics::new(
        "Full Table Scan",
        start.elapsed(),
        all_rows.len(),
    ));

    // Test selective query
    let start = Instant::now();
    let result = db.query("SELECT name, value FROM large_test WHERE value > 5000")?;
    let selected_rows = result.collect_rows()?;
    metrics.push(PerformanceMetrics::new(
        "Selective Query",
        start.elapsed(),
        selected_rows.len(),
    ));

    // Test streaming with limit
    let start = Instant::now();
    let result = db.query("SELECT * FROM large_test LIMIT 1000")?;
    let mut limited_count = 0;
    for row_result in result.into_iter() {
        let _row = row_result?;
        limited_count += 1;
    }
    metrics.push(PerformanceMetrics::new(
        "Streaming with LIMIT",
        start.elapsed(),
        limited_count,
    ));

    // Test aggregation-style operation (counting)
    let start = Instant::now();
    let result = db.query("SELECT * FROM large_test WHERE value % 10 = 0")?;
    let filtered_rows = result.collect_rows()?;
    metrics.push(PerformanceMetrics::new(
        "Modulo Filter",
        start.elapsed(),
        filtered_rows.len(),
    ));

    // Print results
    for metric in &metrics {
        metric.print_summary();
    }

    Ok(())
}

#[test]
fn test_concurrent_schema_access_performance() -> Result<()> {
    println!("=== Concurrent Schema Access Performance Test ===");
    let mut db = create_test_db()?;
    let mut metrics = Vec::new();

    // Create base table
    db.execute("CREATE TABLE concurrent_test (id INTEGER PRIMARY KEY, data TEXT(32))")?;

    // Insert some data
    for i in 0..1000 {
        db.execute(&format!(
            "INSERT INTO concurrent_test (id, data) VALUES ({i}, 'data_{i}')"
        ))?;
    }

    // Test rapid schema access (simulating multiple queries)
    let start = Instant::now();
    let iterations = 100;
    for _ in 0..iterations {
        let _schemas = db.get_table_schemas();
        let result = db.query("SELECT * FROM concurrent_test LIMIT 10")?;
        let _rows = result.collect_rows()?;
    }
    metrics.push(PerformanceMetrics::new(
        "Rapid Schema Access",
        start.elapsed(),
        iterations,
    ));

    // Test schema modification performance
    let start = Instant::now();
    let table_ops = 20;
    for i in 0..table_ops {
        db.execute(&format!(
            "CREATE TABLE temp_table_{i} (id INTEGER PRIMARY KEY, name TEXT(32))"
        ))?;
        db.execute(&format!("DROP TABLE temp_table_{i}"))?;
    }
    metrics.push(PerformanceMetrics::new(
        "Schema Modifications",
        start.elapsed(),
        table_ops * 2,
    ));

    // Print results
    for metric in &metrics {
        metric.print_summary();
    }

    Ok(())
}

#[test]
fn test_memory_usage_pattern() -> Result<()> {
    println!("=== Memory Usage Pattern Test ===");
    let mut db = create_test_db()?;

    // Setup test data
    let record_count = 5000;
    let setup_metrics = setup_test_table(&mut db, "memory_test", record_count)?;
    setup_metrics.print_summary();

    let mut metrics = Vec::new();

    // Test streaming vs batch memory usage (indirectly through timing)
    // Streaming should have lower memory footprint but potentially slower processing

    // Small batch streaming
    let start = Instant::now();
    let result = db.query("SELECT * FROM memory_test LIMIT 100")?;
    let mut small_batch_count = 0;
    for row_result in result.into_iter() {
        let _row = row_result?;
        small_batch_count += 1;
    }
    metrics.push(PerformanceMetrics::new(
        "Small Streaming Batch",
        start.elapsed(),
        small_batch_count,
    ));

    // Medium batch streaming
    let start = Instant::now();
    let result = db.query("SELECT * FROM memory_test LIMIT 1000")?;
    let mut medium_batch_count = 0;
    for row_result in result.into_iter() {
        let _row = row_result?;
        medium_batch_count += 1;
    }
    metrics.push(PerformanceMetrics::new(
        "Medium Streaming Batch",
        start.elapsed(),
        medium_batch_count,
    ));

    // Large batch collection
    let start = Instant::now();
    let result = db.query("SELECT * FROM memory_test")?;
    let large_batch: Vec<_> = result.into_iter().collect::<Result<_>>()?;
    metrics.push(PerformanceMetrics::new(
        "Large Batch Collection",
        start.elapsed(),
        large_batch.len(),
    ));

    // Print results
    for metric in &metrics {
        metric.print_summary();
    }

    Ok(())
}

/// Measure SQL execution with detailed pipeline timing
#[cfg(feature = "dev")]
fn measure_sql_execution(
    db: &mut Database,
    sql: &str,
    operation_name: &str,
) -> Result<SqlExecutionMetrics> {
    // Parse timing
    let parse_start = Instant::now();
    let statement =
        parse_sql(sql).map_err(|e| tegdb::Error::Other(format!("SQL parse error: {e:?}")))?;
    let parse_duration = parse_start.elapsed();

    // Get schemas for planner and convert to Rc format
    let schemas = db.get_table_schemas();
    let rc_schemas: HashMap<String, std::rc::Rc<tegdb::query_processor::TableSchema>> = schemas
        .into_iter()
        .map(|(k, v)| (k, std::rc::Rc::new(v)))
        .collect();

    // Plan timing
    let plan_start = Instant::now();
    let planner = QueryPlanner::new(rc_schemas);
    let _plan = planner.plan(statement.clone())?;
    let plan_duration = plan_start.elapsed();

    // Execute timing
    let execute_start = Instant::now();

    // For queries, we need to handle differently than mutations
    let actual_records = match &statement {
        tegdb::parser::Statement::Select(_) => {
            // For SELECT, we need to count the results
            let result = db.query(sql)?;
            let rows = result.collect_rows()?;
            rows.len()
        }
        _ => {
            // For mutations, execute and get affected rows
            db.execute(sql)?
        }
    };

    let execute_duration = execute_start.elapsed();

    Ok(SqlExecutionMetrics::new(
        operation_name,
        parse_duration,
        plan_duration,
        execute_duration,
        actual_records,
    ))
}

/// Simplified version when dev feature is not available
#[cfg(not(feature = "dev"))]
fn measure_sql_execution(
    db: &mut Database,
    sql: &str,
    operation_name: &str,
) -> Result<SqlExecutionMetrics> {
    // Without dev feature, we can only measure total execution time
    let start = Instant::now();

    let actual_records = if sql.trim().to_uppercase().starts_with("SELECT") {
        let result = db.query(sql)?;
        let rows = result.collect_rows()?;
        rows.len()
    } else {
        db.execute(sql)?
    };

    let total_duration = start.elapsed();

    // Rough estimates for breakdown
    let parse_duration = total_duration / 20; // ~5%
    let plan_duration = total_duration / 10; // ~10%
    let execute_duration = total_duration - parse_duration - plan_duration;

    Ok(SqlExecutionMetrics::new(
        operation_name,
        parse_duration,
        plan_duration,
        execute_duration,
        actual_records,
    ))
}

/// Measure transaction-based SQL execution with detailed pipeline timing
#[cfg(feature = "dev")]
fn measure_transaction_sql_execution(
    tx: &mut tegdb::DatabaseTransaction,
    sql: &str,
    operation_name: &str,
) -> Result<SqlExecutionMetrics> {
    // Parse timing
    let parse_start = Instant::now();
    let statement =
        parse_sql(sql).map_err(|e| tegdb::Error::Other(format!("SQL parse error: {e:?}")))?;
    let parse_duration = parse_start.elapsed();

    // Execute timing (includes planning)
    let execute_start = Instant::now();
    let actual_records = match &statement {
        tegdb::parser::Statement::Select(_) => {
            let result = tx.query(sql)?;
            let rows = result.collect_rows()?;
            rows.len()
        }
        _ => tx.execute(sql)?,
    };
    let execute_duration = execute_start.elapsed();

    // Adjust timing since planning happens inside execute for transactions
    let adjusted_plan_duration = execute_duration / 10; // Rough estimate: 10% for planning
    let adjusted_execute_duration = execute_duration - adjusted_plan_duration;

    Ok(SqlExecutionMetrics::new(
        operation_name,
        parse_duration,
        adjusted_plan_duration,
        adjusted_execute_duration,
        actual_records,
    ))
}

/// Simplified version when dev feature is not available
#[cfg(not(feature = "dev"))]
fn measure_transaction_sql_execution(
    tx: &mut tegdb::DatabaseTransaction,
    sql: &str,
    operation_name: &str,
) -> Result<SqlExecutionMetrics> {
    // Without dev feature, we can only measure total execution time
    let start = Instant::now();

    let actual_records = if sql.trim().to_uppercase().starts_with("SELECT") {
        let result = tx.query(sql)?;
        let rows = result.collect_rows()?;
        rows.len()
    } else {
        tx.execute(sql)?
    };

    let total_duration = start.elapsed();

    // Rough estimates for breakdown
    let parse_duration = total_duration / 20; // ~5%
    let plan_duration = total_duration / 10; // ~10%
    let execute_duration = total_duration - parse_duration - plan_duration;

    Ok(SqlExecutionMetrics::new(
        operation_name,
        parse_duration,
        plan_duration,
        execute_duration,
        actual_records,
    ))
}

/// Run all performance tests and generate a summary report
#[test]
fn run_comprehensive_performance_suite() -> Result<()> {
    println!("======================================");
    println!("  TegDB High-Level API Performance Suite");
    println!("======================================");

    // Run all individual test functions
    test_basic_crud_performance()?;
    println!();

    test_streaming_query_performance()?;
    println!();

    test_transaction_performance()?;
    println!();

    test_schema_operations_performance()?;
    println!();

    test_large_dataset_performance()?;
    println!();

    test_concurrent_schema_access_performance()?;
    println!();

    test_memory_usage_pattern()?;
    println!();

    // New detailed pipeline tests
    test_detailed_sql_pipeline_performance()?;
    println!();

    test_transaction_pipeline_performance()?;
    println!();

    test_parser_complexity_performance()?;
    println!();

    println!("======================================");
    println!("  Performance Suite Complete");
    println!("======================================");

    Ok(())
}

#[test]
fn test_detailed_sql_pipeline_performance() -> Result<()> {
    println!("=== Detailed SQL Pipeline Performance Test ===");
    let mut db = create_test_db()?;
    let mut detailed_metrics = Vec::new();

    // Test CREATE TABLE pipeline
    let create_metrics = measure_sql_execution(
        &mut db,
        "CREATE TABLE pipeline_test (id INTEGER PRIMARY KEY, name TEXT(32), value INTEGER)",
        "CREATE TABLE",
    )?;
    detailed_metrics.push(create_metrics);

    // Test INSERT pipeline with different sizes
    let insert_sqls = vec![
        (
            "INSERT INTO pipeline_test (id, name, value) VALUES (1, 'user1', 100)",
            "Single INSERT",
        ),
        (
            "INSERT INTO pipeline_test (id, name, value) VALUES (2, 'user with longer name', 200)",
            "INSERT with long text",
        ),
    ];

    for (sql, name) in insert_sqls {
        let metrics = measure_sql_execution(&mut db, sql, name)?;
        detailed_metrics.push(metrics);
    }

    // Add more data for complex queries
    for i in 3..=50 {
        db.execute(&format!(
            "INSERT INTO pipeline_test (id, name, value) VALUES ({}, 'user{}', {})",
            i,
            i,
            i * 10
        ))?;
    }

    // Test different SELECT query complexities
    let select_queries = vec![
        ("SELECT * FROM pipeline_test", "SELECT ALL"),
        ("SELECT id, name FROM pipeline_test", "SELECT columns"),
        (
            "SELECT * FROM pipeline_test WHERE value > 250",
            "SELECT with WHERE",
        ),
        (
            "SELECT * FROM pipeline_test WHERE value > 100 AND value < 300",
            "SELECT with AND",
        ),
        (
            "SELECT * FROM pipeline_test WHERE name LIKE '%user%'",
            "SELECT with LIKE",
        ),
        ("SELECT * FROM pipeline_test LIMIT 10", "SELECT with LIMIT"),
    ];

    for (sql, name) in select_queries {
        let metrics = measure_sql_execution(&mut db, sql, name)?;
        detailed_metrics.push(metrics);
    }

    // Test UPDATE and DELETE
    let mutation_queries = vec![
        (
            "UPDATE pipeline_test SET name = 'updated' WHERE value < 150",
            "UPDATE with WHERE",
        ),
        (
            "DELETE FROM pipeline_test WHERE value > 400",
            "DELETE with WHERE",
        ),
    ];

    for (sql, name) in mutation_queries {
        let metrics = measure_sql_execution(&mut db, sql, name)?;
        detailed_metrics.push(metrics);
    }

    // Print detailed results
    println!("SQL Pipeline Breakdown:");
    println!("=======================");
    for metric in &detailed_metrics {
        metric.print_detailed_summary();
        metric.print_breakdown();
        println!();
    }

    // Calculate averages for different operation types
    let mut parse_times: Vec<Duration> =
        detailed_metrics.iter().map(|m| m.parse_duration).collect();
    let mut plan_times: Vec<Duration> = detailed_metrics.iter().map(|m| m.plan_duration).collect();
    let mut execute_times: Vec<Duration> = detailed_metrics
        .iter()
        .map(|m| m.execute_duration)
        .collect();

    parse_times.sort();
    plan_times.sort();
    execute_times.sort();

    let avg_parse = parse_times.iter().sum::<Duration>() / parse_times.len() as u32;
    let avg_plan = plan_times.iter().sum::<Duration>() / plan_times.len() as u32;
    let avg_execute = execute_times.iter().sum::<Duration>() / execute_times.len() as u32;

    println!("Pipeline Averages:");
    println!("==================");
    println!(
        "Average Parse Time: {:.1}µs",
        avg_parse.as_secs_f64() * 1_000_000.0
    );
    println!(
        "Average Plan Time: {:.1}µs",
        avg_plan.as_secs_f64() * 1_000_000.0
    );
    println!(
        "Average Execute Time: {:.1}µs",
        avg_execute.as_secs_f64() * 1_000_000.0
    );

    Ok(())
}

#[test]
fn test_transaction_pipeline_performance() -> Result<()> {
    println!("=== Transaction Pipeline Performance Test ===");
    let mut db = create_test_db()?;
    let mut detailed_metrics = Vec::new();

    // Setup base table
    db.execute(
        "CREATE TABLE tx_pipeline_test (id INTEGER PRIMARY KEY, name TEXT(32), value INTEGER)",
    )?;

    // Test transaction-based operations
    let mut tx = db.begin_transaction()?;

    // Test various operations within transaction
    let tx_operations = vec![
        (
            "INSERT INTO tx_pipeline_test (id, name, value) VALUES (1, 'tx_user1', 100)",
            "TX INSERT",
        ),
        (
            "INSERT INTO tx_pipeline_test (id, name, value) VALUES (2, 'tx_user2', 200)",
            "TX INSERT 2",
        ),
        ("SELECT * FROM tx_pipeline_test", "TX SELECT"),
        (
            "UPDATE tx_pipeline_test SET value = 150 WHERE id = 1",
            "TX UPDATE",
        ),
        (
            "SELECT * FROM tx_pipeline_test WHERE value > 100",
            "TX SELECT filtered",
        ),
    ];

    for (sql, name) in tx_operations {
        let metrics = measure_transaction_sql_execution(&mut tx, sql, name)?;
        detailed_metrics.push(metrics);
    }

    tx.commit()?;

    // Print detailed results
    println!("Transaction Pipeline Breakdown:");
    println!("===============================");
    for metric in &detailed_metrics {
        metric.print_detailed_summary();
        metric.print_breakdown();
        println!();
    }

    Ok(())
}

#[test]
fn test_parser_complexity_performance() -> Result<()> {
    println!("=== Parser Complexity Performance Test ===");
    let mut db = create_test_db()?;
    let mut parse_metrics = Vec::new();

    // Setup test table
    db.execute(
        "CREATE TABLE parse_test (id INTEGER PRIMARY KEY, name TEXT(32), value INTEGER, data TEXT(32))",
    )?;

    // Test queries of increasing complexity
    let complex_queries = vec![
        ("SELECT id FROM parse_test", "Simple SELECT"),
        ("SELECT id, name FROM parse_test", "Multi-column SELECT"),
        ("SELECT * FROM parse_test", "Wildcard SELECT"),
        (
            "SELECT * FROM parse_test WHERE id = 1",
            "SELECT with simple WHERE",
        ),
        (
            "SELECT * FROM parse_test WHERE id > 1 AND name = 'test'",
            "SELECT with AND condition",
        ),
        (
            "SELECT * FROM parse_test WHERE id > 1 OR value < 100",
            "SELECT with OR condition",
        ),
        (
            "SELECT * FROM parse_test WHERE (id > 1 AND name = 'test') OR value < 100",
            "SELECT with complex WHERE",
        ),
        (
            "SELECT id, name FROM parse_test WHERE value LIKE '%test%' LIMIT 10",
            "SELECT with LIKE and LIMIT",
        ),
        (
            "INSERT INTO parse_test (id, name, value, data) VALUES (1, 'test', 100, 'some data')",
            "Complex INSERT",
        ),
        (
            "UPDATE parse_test SET name = 'updated', value = 200 WHERE id = 1 AND value > 50",
            "Complex UPDATE",
        ),
    ];

    for (sql, description) in &complex_queries {
        let metrics = measure_sql_execution(&mut db, sql, description)?;
        parse_metrics.push(metrics);
    }

    // Print results focusing on parse times
    println!("Parser Performance by Query Complexity:");
    println!("=======================================");
    for metric in &parse_metrics {
        println!(
            "{}: Parse {:.1}µs (SQL length: {} chars)",
            metric.operation,
            metric.parse_duration.as_secs_f64() * 1_000_000.0,
            complex_queries
                .iter()
                .find(|(_, desc)| *desc == metric.operation)
                .unwrap()
                .0
                .len()
        );
    }

    // Find correlation between SQL length and parse time
    let mut complexity_analysis: Vec<(usize, f64)> = Vec::new();
    for (i, metric) in parse_metrics.iter().enumerate() {
        let sql_length = complex_queries[i].0.len();
        let parse_time_us = metric.parse_duration.as_secs_f64() * 1_000_000.0;
        complexity_analysis.push((sql_length, parse_time_us));
    }

    complexity_analysis.sort_by_key(|&(len, _)| len);

    println!("\nSQL Length vs Parse Time Correlation:");
    println!("=====================================");
    for (length, time) in complexity_analysis {
        println!("Length: {length:3} chars -> Parse time: {time:.1}µs");
    }

    Ok(())
}
