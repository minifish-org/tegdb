use tegdb::low_level::{parse_sql, Statement};

/// Test parsing BEGIN statements
#[test]
fn test_parse_begin() {
    // Test basic BEGIN
    let sql = "BEGIN";
    let result = parse_sql(sql);
    assert!(result.is_ok());
    let statement = result.unwrap();
    assert!(matches!(statement, Statement::Begin));

    // Test case insensitive
    let sql = "begin";
    let result = parse_sql(sql);
    assert!(result.is_ok());
    let statement = result.unwrap();
    assert!(matches!(statement, Statement::Begin));

    // Test with extra spaces
    let sql = "  BEGIN  ";
    let result = parse_sql(sql);
    assert!(result.is_ok());
    let statement = result.unwrap();
    assert!(matches!(statement, Statement::Begin));
}

/// Test parsing START TRANSACTION statements
#[test]
fn test_parse_start_transaction() {
    // Test START TRANSACTION
    let sql = "START TRANSACTION";
    let result = parse_sql(sql);
    assert!(result.is_ok());
    let statement = result.unwrap();
    assert!(matches!(statement, Statement::Begin));

    // Test case insensitive
    let sql = "start transaction";
    let result = parse_sql(sql);
    assert!(result.is_ok());
    let statement = result.unwrap();
    assert!(matches!(statement, Statement::Begin));

    // Test mixed case
    let sql = "Start Transaction";
    let result = parse_sql(sql);
    assert!(result.is_ok());
    let statement = result.unwrap();
    assert!(matches!(statement, Statement::Begin));

    // Test with extra spaces
    let sql = "  START    TRANSACTION  ";
    let result = parse_sql(sql);
    assert!(result.is_ok());
    let statement = result.unwrap();
    assert!(matches!(statement, Statement::Begin));
}

/// Test parsing COMMIT statements
#[test]
fn test_parse_commit() {
    // Test basic COMMIT
    let sql = "COMMIT";
    let result = parse_sql(sql);
    assert!(result.is_ok());
    let statement = result.unwrap();
    assert!(matches!(statement, Statement::Commit));

    // Test case insensitive
    let sql = "commit";
    let result = parse_sql(sql);
    assert!(result.is_ok());
    let statement = result.unwrap();
    assert!(matches!(statement, Statement::Commit));

    // Test with extra spaces
    let sql = "  COMMIT  ";
    let result = parse_sql(sql);
    assert!(result.is_ok());
    let statement = result.unwrap();
    assert!(matches!(statement, Statement::Commit));
}

/// Test parsing ROLLBACK statements
#[test]
fn test_parse_rollback() {
    // Test basic ROLLBACK
    let sql = "ROLLBACK";
    let result = parse_sql(sql);
    assert!(result.is_ok());
    let statement = result.unwrap();
    assert!(matches!(statement, Statement::Rollback));

    // Test case insensitive
    let sql = "rollback";
    let result = parse_sql(sql);
    assert!(result.is_ok());
    let statement = result.unwrap();
    assert!(matches!(statement, Statement::Rollback));

    // Test with extra spaces
    let sql = "  ROLLBACK  ";
    let result = parse_sql(sql);
    assert!(result.is_ok());
    let statement = result.unwrap();
    assert!(matches!(statement, Statement::Rollback));
}

/// Test transaction statement priority in parsing
#[test]
fn test_transaction_statement_priority() {
    // Verify that transaction statements are parsed correctly
    // even when they might conflict with other keywords

    let statements = vec!["BEGIN", "COMMIT", "ROLLBACK", "START TRANSACTION"];

    for sql in statements {
        let result = parse_sql(sql);
        assert!(result.is_ok(), "Failed to parse: {sql}");
        let statement = result.unwrap();

        match sql {
            "BEGIN" | "START TRANSACTION" => assert!(matches!(statement, Statement::Begin)),
            "COMMIT" => assert!(matches!(statement, Statement::Commit)),
            "ROLLBACK" => assert!(matches!(statement, Statement::Rollback)),
            _ => panic!("Unexpected SQL statement: {sql}"),
        }
    }
}

/// Test invalid transaction statements
#[test]
fn test_invalid_transaction_statements() {
    let invalid_statements = vec![
        "BEGINS",          // Invalid variant
        "COMMITS",         // Invalid variant
        "ROLLBACKS",       // Invalid variant
        "START",           // Incomplete
        "TRANSACTION",     // Incomplete
        "BEGIN COMMIT",    // Mixed statements
        "COMMIT ROLLBACK", // Mixed statements
    ];

    for sql in invalid_statements {
        let result = parse_sql(sql);
        // These should either fail to parse or parse as something else
        if result.is_ok() {
            // If it parses, that's unexpected for invalid statements
            panic!("Unexpected successful parse for: {sql}");
        }
        // Otherwise it failed to parse, which is expected
    }
}
