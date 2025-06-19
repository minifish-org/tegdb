use tegdb::{Engine, executor::{Executor, ResultSet}};
use tegdb::parser::parse_sql;
use tempfile::tempdir;

#[test]
fn test_drop_table_integration() {
    let dir = tempdir().unwrap();
    let db_path = dir.path().join("test_drop_table.db");
    let mut engine = Engine::new(db_path).unwrap();
    
    let transaction = engine.begin_transaction();
    let mut executor = Executor::new(transaction);

    // Start a transaction
    let begin_result = executor.execute(parse_sql("BEGIN").unwrap().1).unwrap();
    assert!(matches!(begin_result, ResultSet::Begin));

    // Create a table first
    let create_sql = "CREATE TABLE test_table (id INTEGER PRIMARY KEY, name TEXT)";
    let create_result = executor.execute(parse_sql(create_sql).unwrap().1).unwrap();
    assert!(matches!(create_result, ResultSet::CreateTable { .. }));

    // Drop the table
    let drop_sql = "DROP TABLE test_table";
    let drop_result = executor.execute(parse_sql(drop_sql).unwrap().1).unwrap();
    
    match drop_result {
        ResultSet::DropTable { table_name, existed } => {
            assert_eq!(table_name, "test_table");
            assert_eq!(existed, true);
        }
        _ => panic!("Expected DropTable result"),
    }

    // Try to drop the same table again (should fail)
    let drop_again_result = executor.execute(parse_sql(drop_sql).unwrap().1);
    assert!(drop_again_result.is_err());

    // Try to drop with IF EXISTS (should succeed)
    let drop_if_exists_sql = "DROP TABLE IF EXISTS test_table";
    let drop_if_exists_result = executor.execute(parse_sql(drop_if_exists_sql).unwrap().1).unwrap();
    
    match drop_if_exists_result {
        ResultSet::DropTable { table_name, existed } => {
            assert_eq!(table_name, "test_table");
            assert_eq!(existed, false);
        }
        _ => panic!("Expected DropTable result"),
    }
}
