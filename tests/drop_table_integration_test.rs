use tegdb::{Engine, executor::{Executor, ResultSet}};
use tegdb::parser::{parse_sql, Statement};
use tempfile::tempdir;

#[test]
fn test_drop_table_integration() {
    let dir = tempdir().unwrap();
    let db_path = dir.path().join("test_drop_table.db");
    let mut engine = Engine::new(db_path).unwrap();
    
    let transaction = engine.begin_transaction();
    let mut executor = Executor::new(transaction);

    // Start a transaction
    let begin_result = executor.begin_transaction().unwrap();
    assert!(matches!(begin_result, ResultSet::Begin));

    // Create a table first
    let create_sql = "CREATE TABLE test_table (id INTEGER PRIMARY KEY, name TEXT)";
    let create_statement = match parse_sql(create_sql).unwrap().1 {
        Statement::CreateTable(create) => create,
        _ => panic!("Expected CREATE TABLE statement"),
    };
    let create_result = executor.execute_create_table(create_statement).unwrap();
    assert!(matches!(create_result, ResultSet::CreateTable { .. }));

    // Drop the table
    let drop_sql = "DROP TABLE test_table";
    let drop_statement = match parse_sql(drop_sql).unwrap().1 {
        Statement::DropTable(drop) => drop,
        _ => panic!("Expected DROP TABLE statement"),
    };
    let drop_result = executor.execute_drop_table(drop_statement).unwrap();
    
    match drop_result {
        ResultSet::DropTable { table_name, existed } => {
            assert_eq!(table_name, "test_table");
            assert_eq!(existed, true);
        }
        _ => panic!("Expected DropTable result"),
    }

    // Try to drop the same table again (should fail)
    let drop_again_statement = match parse_sql(drop_sql).unwrap().1 {
        Statement::DropTable(drop) => drop,
        _ => panic!("Expected DROP TABLE statement"),
    };
    let drop_again_result = executor.execute_drop_table(drop_again_statement);
    assert!(drop_again_result.is_err());

    // Try to drop with IF EXISTS (should succeed)
    let drop_if_exists_sql = "DROP TABLE IF EXISTS test_table";
    let drop_if_exists_statement = match parse_sql(drop_if_exists_sql).unwrap().1 {
        Statement::DropTable(drop) => drop,
        _ => panic!("Expected DROP TABLE statement"),
    };
    let drop_if_exists_result = executor.execute_drop_table(drop_if_exists_statement).unwrap();
    
    match drop_if_exists_result {
        ResultSet::DropTable { table_name, existed } => {
            assert_eq!(table_name, "test_table");
            assert_eq!(existed, false);
        }
        _ => panic!("Expected DropTable result"),
    }
}
