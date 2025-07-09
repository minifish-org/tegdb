use tegdb::{Database, Result};

mod test_helpers;
use test_helpers::run_with_both_backends;

#[test]
fn test_drop_table_integration() -> Result<()> {
    run_with_both_backends("test_drop_table_integration", |db_path| {
        let mut db = Database::open(db_path)?;

        // Create a table first
        db.execute("CREATE TABLE test_table (id INTEGER PRIMARY KEY, name TEXT)")?;

        // Insert some data to verify the table works
        db.execute("INSERT INTO test_table (id, name) VALUES (1, 'test')")?;

        // Verify table exists and has data
        let result = db.query("SELECT * FROM test_table")?;
        assert_eq!(result.rows().len(), 1);

        // Drop the table
        let affected = db.execute("DROP TABLE test_table")?;
        assert_eq!(affected, 0); // DROP TABLE returns 0 affected rows

        // Try to query the dropped table (should fail)
        let result = db.query("SELECT * FROM test_table");
        assert!(result.is_err(), "Table should no longer exist");

        // Try to drop the same table again (should fail)
        let result = db.execute("DROP TABLE test_table");
        assert!(result.is_err(), "Dropping non-existent table should fail");

        // Try to drop with IF EXISTS (should succeed)
        let affected = db.execute("DROP TABLE IF EXISTS test_table")?;
        assert_eq!(affected, 0); // Should succeed even if table doesn't exist

        // Try to drop with IF EXISTS on a table that never existed (should also succeed)
        let affected = db.execute("DROP TABLE IF EXISTS never_existed_table")?;
        assert_eq!(affected, 0);

        Ok(())
    })
}
