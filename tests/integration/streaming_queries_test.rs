mod test_helpers {
    include!("../common/test_helpers.rs");
}
use test_helpers::run_with_both_backends;

use tegdb::{Database, Result, SqlValue};

#[test]
fn test_early_limit_termination() -> Result<()> {
    run_with_both_backends("test_early_limit_termination", |db_path| {
        let mut db = Database::open(db_path)?;

        // Create large table
        db.execute("CREATE TABLE large_table (id INTEGER PRIMARY KEY, value INTEGER)")?;
        for i in 1..=1000 {
            db.execute(&format!(
                "INSERT INTO large_table (id, value) VALUES ({}, {})",
                i,
                i * 10
            ))?;
        }

        // Query with LIMIT - should only process limited rows
        let result = db.query("SELECT * FROM large_table LIMIT 5")?;
        assert_eq!(result.len(), 5);

        // Verify we got the first 5 rows
        let rows = result.rows();
        assert_eq!(rows.len(), 5);
        for (i, row) in rows.iter().enumerate() {
            assert_eq!(row[0], SqlValue::Integer((i + 1) as i64));
        }

        // Query with smaller LIMIT
        let result = db.query("SELECT * FROM large_table LIMIT 10")?;
        assert_eq!(result.len(), 10);

        Ok(())
    })
}

#[test]
fn test_streaming_with_where_clauses() -> Result<()> {
    run_with_both_backends("test_streaming_with_where_clauses", |db_path| {
        let mut db = Database::open(db_path)?;

        // Create table with data
        db.execute("CREATE TABLE filtered_table (id INTEGER PRIMARY KEY, category TEXT(32), value INTEGER)")?;
        for i in 1..=100 {
            let category = if i % 2 == 0 { "even" } else { "odd" };
            db.execute(&format!(
                "INSERT INTO filtered_table (id, category, value) VALUES ({}, '{}', {})",
                i,
                category,
                i * 10
            ))?;
        }

        // Query with WHERE - should filter during streaming
        let result = db.query("SELECT * FROM filtered_table WHERE category = 'even' LIMIT 10")?;
        assert_eq!(result.len(), 10);

        // Verify all results match the filter
        for row in result.rows() {
            assert_eq!(row[1], SqlValue::Text("even".to_string()));
        }

        // Query with WHERE and no LIMIT
        let result = db.query("SELECT * FROM filtered_table WHERE value > 500")?;
        assert_eq!(result.len(), 50); // ids 51-100 have values > 500

        Ok(())
    })
}

#[test]
fn test_streaming_with_order_by() -> Result<()> {
    run_with_both_backends("test_streaming_with_order_by", |db_path| {
        let mut db = Database::open(db_path)?;

        // Create table with unsorted data
        db.execute("CREATE TABLE unsorted_table (id INTEGER PRIMARY KEY, value INTEGER)")?;
        for i in (1..=10).rev() {
            db.execute(&format!(
                "INSERT INTO unsorted_table (id, value) VALUES ({}, {})",
                i,
                i * 10
            ))?;
        }

        // Query with ORDER BY - should stream sorted results
        let result = db.query("SELECT * FROM unsorted_table ORDER BY value LIMIT 5")?;
        assert_eq!(result.len(), 5);

        // Verify results are sorted
        let rows = result.rows();
        for i in 0..4 {
            if let (SqlValue::Integer(v1), SqlValue::Integer(v2)) = (&rows[i][1], &rows[i + 1][1]) {
                assert!(v1 <= v2, "Results should be sorted");
            }
        }

        // Query with ORDER BY DESC
        let result = db.query("SELECT * FROM unsorted_table ORDER BY value DESC LIMIT 3")?;
        assert_eq!(result.len(), 3);

        // Verify descending order
        let rows = result.rows();
        for i in 0..2 {
            if let (SqlValue::Integer(v1), SqlValue::Integer(v2)) = (&rows[i][1], &rows[i + 1][1]) {
                assert!(v1 >= v2, "Results should be sorted descending");
            }
        }

        Ok(())
    })
}

#[test]
fn test_streaming_iterator_behavior() -> Result<()> {
    run_with_both_backends("test_streaming_iterator_behavior", |db_path| {
        let mut db = Database::open(db_path)?;

        // Create table
        db.execute("CREATE TABLE iterator_test (id INTEGER PRIMARY KEY, data TEXT(32))")?;
        for i in 1..=10 {
            db.execute(&format!(
                "INSERT INTO iterator_test (id, data) VALUES ({}, 'data_{}')",
                i, i
            ))?;
        }

        // Test iterator - process only first 3 rows
        let result = db.query("SELECT * FROM iterator_test")?;
        let mut count = 0;
        let mut collected = Vec::new();

        for row in result.rows().iter().take(3) {
            collected.push(row.clone());
            count += 1;
        }

        assert_eq!(count, 3);
        assert_eq!(collected.len(), 3);

        // Test skip
        let result = db.query("SELECT * FROM iterator_test")?;
        let skipped: Vec<_> = result.rows().iter().skip(5).take(3).collect();
        assert_eq!(skipped.len(), 3);

        Ok(())
    })
}

#[test]
fn test_streaming_large_result_sets() -> Result<()> {
    run_with_both_backends("test_streaming_large_result_sets", |db_path| {
        let mut db = Database::open(db_path)?;

        // Create large table
        db.execute("CREATE TABLE huge_table (id INTEGER PRIMARY KEY, value INTEGER)")?;
        for i in 1..=5000 {
            db.execute(&format!(
                "INSERT INTO huge_table (id, value) VALUES ({}, {})",
                i, i
            ))?;
        }

        // Query all rows - should handle large dataset
        let result = db.query("SELECT * FROM huge_table")?;
        assert_eq!(result.len(), 5000);

        // Process rows one by one
        let mut count = 0;
        for row in result.rows().iter() {
            assert_eq!(row[0], SqlValue::Integer((count + 1) as i64));
            count += 1;
            if count >= 100 {
                // Test early termination on large dataset
                break;
            }
        }

        assert_eq!(count, 100);

        Ok(())
    })
}

#[test]
fn test_multiple_streaming_queries_in_sequence() -> Result<()> {
    run_with_both_backends("test_multiple_streaming_queries_in_sequence", |db_path| {
        let mut db = Database::open(db_path)?;

        db.execute("CREATE TABLE seq_test (id INTEGER PRIMARY KEY, value INTEGER)")?;
        for i in 1..=20 {
            db.execute(&format!(
                "INSERT INTO seq_test (id, value) VALUES ({}, {})",
                i,
                i * 10
            ))?;
        }

        // First query
        let result1 = db.query("SELECT * FROM seq_test LIMIT 5")?;
        assert_eq!(result1.len(), 5);

        // Second query
        let result2 = db.query("SELECT * FROM seq_test WHERE value > 100 LIMIT 5")?;
        assert_eq!(result2.len(), 5);

        // Third query
        let result3 = db.query("SELECT * FROM seq_test ORDER BY value DESC LIMIT 3")?;
        assert_eq!(result3.len(), 3);

        // All queries should work independently
        assert_eq!(result1.rows().len(), 5);
        assert_eq!(result2.rows().len(), 5);
        assert_eq!(result3.rows().len(), 3);

        Ok(())
    })
}

#[test]
fn test_streaming_with_aggregates() -> Result<()> {
    run_with_both_backends("test_streaming_with_aggregates", |db_path| {
        let mut db = Database::open(db_path)?;

        // Create table
        db.execute("CREATE TABLE agg_test (id INTEGER PRIMARY KEY, value INTEGER)")?;
        for i in 1..=100 {
            db.execute(&format!(
                "INSERT INTO agg_test (id, value) VALUES ({}, {})",
                i,
                i * 10
            ))?;
        }

        // Aggregate queries should still work with streaming
        let result = db.query("SELECT COUNT(*) FROM agg_test")?;
        assert_eq!(result.rows()[0][0], SqlValue::Integer(100));

        let result = db.query("SELECT SUM(value) FROM agg_test")?;
        assert_eq!(result.rows()[0][0], SqlValue::Real(50500.0)); // Sum of 10, 20, ..., 1000

        let result = db.query("SELECT AVG(value) FROM agg_test")?;
        assert_eq!(result.rows()[0][0], SqlValue::Real(505.0)); // Average of 10, 20, ..., 1000

        Ok(())
    })
}

#[test]
fn test_streaming_memory_efficiency() -> Result<()> {
    run_with_both_backends("test_streaming_memory_efficiency", |db_path| {
        let mut db = Database::open(db_path)?;

        // Create large table
        db.execute("CREATE TABLE memory_test (id INTEGER PRIMARY KEY, data TEXT(100))")?;
        for i in 1..=1000 {
            let data = format!("data_{}_with_some_content_{}", i, "x".repeat(50));
            db.execute(&format!(
                "INSERT INTO memory_test (id, data) VALUES ({}, '{}')",
                i, data
            ))?;
        }

        // Query with LIMIT - should only process limited rows
        let result = db.query("SELECT * FROM memory_test LIMIT 10")?;
        assert_eq!(result.len(), 10);

        // Process only first 5 rows
        let mut processed = 0;
        for _row in result.rows().iter().take(5) {
            processed += 1;
        }
        assert_eq!(processed, 5);

        // Another query with different LIMIT
        let result2 = db.query("SELECT * FROM memory_test LIMIT 20")?;
        assert_eq!(result2.len(), 20);

        Ok(())
    })
}

#[test]
fn test_streaming_combined_operations() -> Result<()> {
    run_with_both_backends("test_streaming_combined_operations", |db_path| {
        let mut db = Database::open(db_path)?;

        db.execute(
            "CREATE TABLE combined_test (id INTEGER PRIMARY KEY, category TEXT(32), value INTEGER)",
        )?;
        for i in 1..=50 {
            let category = if i % 3 == 0 {
                "A"
            } else if i % 3 == 1 {
                "B"
            } else {
                "C"
            };
            db.execute(&format!(
                "INSERT INTO combined_test (id, category, value) VALUES ({}, '{}', {})",
                i,
                category,
                i * 10
            ))?;
        }

        // Combined WHERE, ORDER BY, and LIMIT
        let result = db.query(
            "SELECT * FROM combined_test WHERE category = 'A' ORDER BY value DESC LIMIT 5",
        )?;
        assert_eq!(result.len(), 5);

        // Verify all results match category
        for row in result.rows() {
            assert_eq!(row[1], SqlValue::Text("A".to_string()));
        }

        // Verify descending order
        let rows = result.rows();
        for i in 0..4 {
            if let (SqlValue::Integer(v1), SqlValue::Integer(v2)) = (&rows[i][2], &rows[i + 1][2]) {
                assert!(v1 >= v2, "Results should be sorted descending");
            }
        }

        Ok(())
    })
}
