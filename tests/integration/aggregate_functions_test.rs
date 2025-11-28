mod test_helpers {
    include!("../common/test_helpers.rs");
}
use test_helpers::run_with_both_backends;

use tegdb::{Database, Result, SqlValue};

#[test]
fn test_count_aggregate() -> Result<()> {
    run_with_both_backends("test_count_aggregate", |db_path| {
        let mut db = Database::open(db_path)?;

        // Empty table
        db.execute("CREATE TABLE empty_table (id INTEGER PRIMARY KEY)")?;
        let result = db.query("SELECT COUNT(*) FROM empty_table")?;
        assert_eq!(result.rows()[0][0], SqlValue::Integer(0));

        // Single row
        db.execute("CREATE TABLE single_table (id INTEGER PRIMARY KEY, name TEXT(32))")?;
        db.execute("INSERT INTO single_table (id, name) VALUES (1, 'Alice')")?;
        let result = db.query("SELECT COUNT(*) FROM single_table")?;
        assert_eq!(result.rows()[0][0], SqlValue::Integer(1));

        // Multiple rows
        db.execute("CREATE TABLE multi_table (id INTEGER PRIMARY KEY, name TEXT(32))")?;
        db.execute(
            "INSERT INTO multi_table (id, name) VALUES (1, 'Alice'), (2, 'Bob'), (3, 'Charlie')",
        )?;
        let result = db.query("SELECT COUNT(*) FROM multi_table")?;
        assert_eq!(result.rows()[0][0], SqlValue::Integer(3));

        // COUNT with WHERE
        let result = db.query("SELECT COUNT(*) FROM multi_table WHERE id > 1")?;
        assert_eq!(result.rows()[0][0], SqlValue::Integer(2));

        // COUNT(column) - counts non-NULL values
        db.execute("CREATE TABLE null_table (id INTEGER PRIMARY KEY, name TEXT(32))")?;
        db.execute(
            "INSERT INTO null_table (id, name) VALUES (1, 'Alice'), (2, NULL), (3, 'Charlie')",
        )?;
        let result = db.query("SELECT COUNT(name) FROM null_table")?;
        assert_eq!(result.rows()[0][0], SqlValue::Integer(2));

        // COUNT(column) with all NULLs
        db.execute("CREATE TABLE all_null_table (id INTEGER PRIMARY KEY, name TEXT(32))")?;
        db.execute("INSERT INTO all_null_table (id, name) VALUES (1, NULL), (2, NULL)")?;
        let result = db.query("SELECT COUNT(name) FROM all_null_table")?;
        assert_eq!(result.rows()[0][0], SqlValue::Integer(0));

        Ok(())
    })
}

#[test]
fn test_sum_aggregate() -> Result<()> {
    run_with_both_backends("test_sum_aggregate", |db_path| {
        let mut db = Database::open(db_path)?;

        // SUM with INTEGER
        db.execute("CREATE TABLE int_table (id INTEGER PRIMARY KEY, value INTEGER)")?;
        db.execute("INSERT INTO int_table (id, value) VALUES (1, 10), (2, 20), (3, 30)")?;
        let result = db.query("SELECT SUM(value) FROM int_table")?;
        assert_eq!(result.rows()[0][0], SqlValue::Real(60.0));

        // SUM with REAL
        db.execute("CREATE TABLE real_table (id INTEGER PRIMARY KEY, value REAL)")?;
        db.execute("INSERT INTO real_table (id, value) VALUES (1, 10.5), (2, 20.5), (3, 30.5)")?;
        let result = db.query("SELECT SUM(value) FROM real_table")?;
        assert_eq!(result.rows()[0][0], SqlValue::Real(61.5));

        // SUM with mixed INTEGER and REAL (if supported)
        db.execute("CREATE TABLE mixed_table (id INTEGER PRIMARY KEY, value REAL)")?;
        db.execute("INSERT INTO mixed_table (id, value) VALUES (1, 10), (2, 20.5)")?;
        let result = db.query("SELECT SUM(value) FROM mixed_table")?;
        assert_eq!(result.rows()[0][0], SqlValue::Real(30.5));

        // SUM with NULL values
        db.execute("CREATE TABLE null_sum_table (id INTEGER PRIMARY KEY, value INTEGER)")?;
        db.execute("INSERT INTO null_sum_table (id, value) VALUES (1, 10), (2, NULL), (3, 30)")?;
        let result = db.query("SELECT SUM(value) FROM null_sum_table")?;
        assert_eq!(result.rows()[0][0], SqlValue::Real(40.0));

        // SUM with all NULLs
        db.execute("CREATE TABLE all_null_sum_table (id INTEGER PRIMARY KEY, value INTEGER)")?;
        db.execute("INSERT INTO all_null_sum_table (id, value) VALUES (1, NULL), (2, NULL)")?;
        let result = db.query("SELECT SUM(value) FROM all_null_sum_table")?;
        assert_eq!(result.rows()[0][0], SqlValue::Null);

        // SUM with empty table
        db.execute("CREATE TABLE empty_sum_table (id INTEGER PRIMARY KEY, value INTEGER)")?;
        let result = db.query("SELECT SUM(value) FROM empty_sum_table")?;
        assert_eq!(result.rows()[0][0], SqlValue::Null);

        // SUM with WHERE
        let result = db.query("SELECT SUM(value) FROM int_table WHERE id > 1")?;
        assert_eq!(result.rows()[0][0], SqlValue::Real(50.0));

        // SUM with negative values
        db.execute("CREATE TABLE neg_table (id INTEGER PRIMARY KEY, value INTEGER)")?;
        db.execute("INSERT INTO neg_table (id, value) VALUES (1, -10), (2, 20), (3, -5)")?;
        let result = db.query("SELECT SUM(value) FROM neg_table")?;
        assert_eq!(result.rows()[0][0], SqlValue::Real(5.0));

        Ok(())
    })
}

#[test]
fn test_avg_aggregate() -> Result<()> {
    run_with_both_backends("test_avg_aggregate", |db_path| {
        let mut db = Database::open(db_path)?;

        // AVG with INTEGER
        db.execute("CREATE TABLE int_avg_table (id INTEGER PRIMARY KEY, value INTEGER)")?;
        db.execute("INSERT INTO int_avg_table (id, value) VALUES (1, 10), (2, 20), (3, 30)")?;
        let result = db.query("SELECT AVG(value) FROM int_avg_table")?;
        assert_eq!(result.rows()[0][0], SqlValue::Real(20.0));

        // AVG with REAL
        db.execute("CREATE TABLE real_avg_table (id INTEGER PRIMARY KEY, value REAL)")?;
        db.execute(
            "INSERT INTO real_avg_table (id, value) VALUES (1, 10.5), (2, 20.5), (3, 30.5)",
        )?;
        let result = db.query("SELECT AVG(value) FROM real_avg_table")?;
        assert_eq!(result.rows()[0][0], SqlValue::Real(20.5));

        // AVG with NULL values (NULLs are ignored)
        db.execute("CREATE TABLE null_avg_table (id INTEGER PRIMARY KEY, value INTEGER)")?;
        db.execute("INSERT INTO null_avg_table (id, value) VALUES (1, 10), (2, NULL), (3, 30)")?;
        let result = db.query("SELECT AVG(value) FROM null_avg_table")?;
        assert_eq!(result.rows()[0][0], SqlValue::Real(20.0));

        // AVG with all NULLs
        db.execute("CREATE TABLE all_null_avg_table (id INTEGER PRIMARY KEY, value INTEGER)")?;
        db.execute("INSERT INTO all_null_avg_table (id, value) VALUES (1, NULL), (2, NULL)")?;
        let result = db.query("SELECT AVG(value) FROM all_null_avg_table")?;
        assert_eq!(result.rows()[0][0], SqlValue::Null);

        // AVG with empty table
        db.execute("CREATE TABLE empty_avg_table (id INTEGER PRIMARY KEY, value INTEGER)")?;
        let result = db.query("SELECT AVG(value) FROM empty_avg_table")?;
        assert_eq!(result.rows()[0][0], SqlValue::Null);

        // AVG with WHERE
        let result = db.query("SELECT AVG(value) FROM int_avg_table WHERE id > 1")?;
        assert_eq!(result.rows()[0][0], SqlValue::Real(25.0));

        // AVG with single value
        db.execute("CREATE TABLE single_avg_table (id INTEGER PRIMARY KEY, value INTEGER)")?;
        db.execute("INSERT INTO single_avg_table (id, value) VALUES (1, 42)")?;
        let result = db.query("SELECT AVG(value) FROM single_avg_table")?;
        assert_eq!(result.rows()[0][0], SqlValue::Real(42.0));

        Ok(())
    })
}

#[test]
fn test_max_aggregate() -> Result<()> {
    run_with_both_backends("test_max_aggregate", |db_path| {
        let mut db = Database::open(db_path)?;

        // MAX with INTEGER
        db.execute("CREATE TABLE int_max_table (id INTEGER PRIMARY KEY, value INTEGER)")?;
        db.execute("INSERT INTO int_max_table (id, value) VALUES (1, 10), (2, 30), (3, 20)")?;
        let result = db.query("SELECT MAX(value) FROM int_max_table")?;
        assert_eq!(result.rows()[0][0], SqlValue::Integer(30));

        // MAX with REAL
        db.execute("CREATE TABLE real_max_table (id INTEGER PRIMARY KEY, value REAL)")?;
        db.execute(
            "INSERT INTO real_max_table (id, value) VALUES (1, 10.5), (2, 30.5), (3, 20.5)",
        )?;
        let result = db.query("SELECT MAX(value) FROM real_max_table")?;
        assert_eq!(result.rows()[0][0], SqlValue::Real(30.5));

        // MAX with TEXT
        db.execute("CREATE TABLE text_max_table (id INTEGER PRIMARY KEY, name TEXT(32))")?;
        db.execute(
            "INSERT INTO text_max_table (id, name) VALUES (1, 'Alice'), (2, 'Charlie'), (3, 'Bob')",
        )?;
        let result = db.query("SELECT MAX(name) FROM text_max_table")?;
        assert_eq!(result.rows()[0][0], SqlValue::Text("Charlie".to_string()));

        // MAX with NULL values (NULLs are ignored)
        db.execute("CREATE TABLE null_max_table (id INTEGER PRIMARY KEY, value INTEGER)")?;
        db.execute("INSERT INTO null_max_table (id, value) VALUES (1, 10), (2, NULL), (3, 30)")?;
        let result = db.query("SELECT MAX(value) FROM null_max_table")?;
        assert_eq!(result.rows()[0][0], SqlValue::Integer(30));

        // MAX with all NULLs
        db.execute("CREATE TABLE all_null_max_table (id INTEGER PRIMARY KEY, value INTEGER)")?;
        db.execute("INSERT INTO all_null_max_table (id, value) VALUES (1, NULL), (2, NULL)")?;
        let result = db.query("SELECT MAX(value) FROM all_null_max_table")?;
        assert_eq!(result.rows()[0][0], SqlValue::Null);

        // MAX with empty table
        db.execute("CREATE TABLE empty_max_table (id INTEGER PRIMARY KEY, value INTEGER)")?;
        let result = db.query("SELECT MAX(value) FROM empty_max_table")?;
        assert_eq!(result.rows()[0][0], SqlValue::Null);

        // MAX with WHERE
        let result = db.query("SELECT MAX(value) FROM int_max_table WHERE id < 3")?;
        assert_eq!(result.rows()[0][0], SqlValue::Integer(30));

        // MAX with negative values
        db.execute("CREATE TABLE neg_max_table (id INTEGER PRIMARY KEY, value INTEGER)")?;
        db.execute("INSERT INTO neg_max_table (id, value) VALUES (1, -10), (2, -30), (3, -5)")?;
        let result = db.query("SELECT MAX(value) FROM neg_max_table")?;
        assert_eq!(result.rows()[0][0], SqlValue::Integer(-5));

        Ok(())
    })
}

#[test]
fn test_min_aggregate() -> Result<()> {
    run_with_both_backends("test_min_aggregate", |db_path| {
        let mut db = Database::open(db_path)?;

        // MIN with INTEGER
        db.execute("CREATE TABLE int_min_table (id INTEGER PRIMARY KEY, value INTEGER)")?;
        db.execute("INSERT INTO int_min_table (id, value) VALUES (1, 10), (2, 30), (3, 20)")?;
        let result = db.query("SELECT MIN(value) FROM int_min_table")?;
        assert_eq!(result.rows()[0][0], SqlValue::Integer(10));

        // MIN with REAL
        db.execute("CREATE TABLE real_min_table (id INTEGER PRIMARY KEY, value REAL)")?;
        db.execute(
            "INSERT INTO real_min_table (id, value) VALUES (1, 10.5), (2, 30.5), (3, 20.5)",
        )?;
        let result = db.query("SELECT MIN(value) FROM real_min_table")?;
        assert_eq!(result.rows()[0][0], SqlValue::Real(10.5));

        // MIN with TEXT
        db.execute("CREATE TABLE text_min_table (id INTEGER PRIMARY KEY, name TEXT(32))")?;
        db.execute(
            "INSERT INTO text_min_table (id, name) VALUES (1, 'Alice'), (2, 'Charlie'), (3, 'Bob')",
        )?;
        let result = db.query("SELECT MIN(name) FROM text_min_table")?;
        assert_eq!(result.rows()[0][0], SqlValue::Text("Alice".to_string()));

        // MIN with NULL values (NULLs are ignored)
        db.execute("CREATE TABLE null_min_table (id INTEGER PRIMARY KEY, value INTEGER)")?;
        db.execute("INSERT INTO null_min_table (id, value) VALUES (1, 10), (2, NULL), (3, 30)")?;
        let result = db.query("SELECT MIN(value) FROM null_min_table")?;
        assert_eq!(result.rows()[0][0], SqlValue::Integer(10));

        // MIN with all NULLs
        db.execute("CREATE TABLE all_null_min_table (id INTEGER PRIMARY KEY, value INTEGER)")?;
        db.execute("INSERT INTO all_null_min_table (id, value) VALUES (1, NULL), (2, NULL)")?;
        let result = db.query("SELECT MIN(value) FROM all_null_min_table")?;
        assert_eq!(result.rows()[0][0], SqlValue::Null);

        // MIN with empty table
        db.execute("CREATE TABLE empty_min_table (id INTEGER PRIMARY KEY, value INTEGER)")?;
        let result = db.query("SELECT MIN(value) FROM empty_min_table")?;
        assert_eq!(result.rows()[0][0], SqlValue::Null);

        // MIN with WHERE
        let result = db.query("SELECT MIN(value) FROM int_min_table WHERE id > 1")?;
        assert_eq!(result.rows()[0][0], SqlValue::Integer(20));

        // MIN with negative values
        db.execute("CREATE TABLE neg_min_table (id INTEGER PRIMARY KEY, value INTEGER)")?;
        db.execute("INSERT INTO neg_min_table (id, value) VALUES (1, -10), (2, -30), (3, -5)")?;
        let result = db.query("SELECT MIN(value) FROM neg_min_table")?;
        assert_eq!(result.rows()[0][0], SqlValue::Integer(-30));

        Ok(())
    })
}

#[test]
fn test_multiple_aggregates() -> Result<()> {
    run_with_both_backends("test_multiple_aggregates", |db_path| {
        let mut db = Database::open(db_path)?;

        db.execute("CREATE TABLE sales (id INTEGER PRIMARY KEY, amount REAL)")?;
        db.execute("INSERT INTO sales (id, amount) VALUES (1, 100.0), (2, 200.0), (3, 300.0)")?;

        // Multiple aggregates in one query
        let result = db.query(
            "SELECT COUNT(*), SUM(amount), AVG(amount), MAX(amount), MIN(amount) FROM sales",
        )?;
        let row = &result.rows()[0];
        assert_eq!(row[0], SqlValue::Integer(3));
        assert_eq!(row[1], SqlValue::Real(600.0));
        assert_eq!(row[2], SqlValue::Real(200.0));
        assert_eq!(row[3], SqlValue::Real(300.0));
        assert_eq!(row[4], SqlValue::Real(100.0));

        Ok(())
    })
}

#[test]
fn test_aggregates_with_order_by() -> Result<()> {
    run_with_both_backends("test_aggregates_with_order_by", |db_path| {
        let mut db = Database::open(db_path)?;

        db.execute("CREATE TABLE items (id INTEGER PRIMARY KEY, value INTEGER)")?;
        db.execute("INSERT INTO items (id, value) VALUES (1, 10), (2, 20), (3, 30)")?;

        // Aggregate with ORDER BY (ORDER BY should not affect aggregate result)
        let result = db.query("SELECT SUM(value) FROM items ORDER BY id")?;
        assert_eq!(result.rows()[0][0], SqlValue::Real(60.0));

        Ok(())
    })
}
