use tegdb::sql::{parse_sql, SqlStatement};

/// Test parsing BEGIN statements
#[test]
fn test_parse_begin() {
    // Test basic BEGIN
    let sql = "BEGIN";
    let result = parse_sql(sql);
    assert!(result.is_ok());
    let (_, statement) = result.unwrap();
    assert!(matches!(statement, SqlStatement::Begin));

    // Test case insensitive
    let sql = "begin";
    let result = parse_sql(sql);
    assert!(result.is_ok());
    let (_, statement) = result.unwrap();
    assert!(matches!(statement, SqlStatement::Begin));

    // Test with extra spaces
    let sql = "  BEGIN  ";
    let result = parse_sql(sql);
    assert!(result.is_ok());
    let (_, statement) = result.unwrap();
    assert!(matches!(statement, SqlStatement::Begin));
}

/// Test parsing START TRANSACTION statements
#[test]
fn test_parse_start_transaction() {
    // Test START TRANSACTION
    let sql = "START TRANSACTION";
    let result = parse_sql(sql);
    assert!(result.is_ok());
    let (_, statement) = result.unwrap();
    assert!(matches!(statement, SqlStatement::Begin));

    // Test case insensitive
    let sql = "start transaction";
    let result = parse_sql(sql);
    assert!(result.is_ok());
    let (_, statement) = result.unwrap();
    assert!(matches!(statement, SqlStatement::Begin));

    // Test mixed case
    let sql = "Start Transaction";
    let result = parse_sql(sql);
    assert!(result.is_ok());
    let (_, statement) = result.unwrap();
    assert!(matches!(statement, SqlStatement::Begin));

    // Test with extra spaces
    let sql = "  START    TRANSACTION  ";
    let result = parse_sql(sql);
    assert!(result.is_ok());
    let (_, statement) = result.unwrap();
    assert!(matches!(statement, SqlStatement::Begin));
}

/// Test parsing COMMIT statements
#[test]
fn test_parse_commit() {
    // Test basic COMMIT
    let sql = "COMMIT";
    let result = parse_sql(sql);
    assert!(result.is_ok());
    let (_, statement) = result.unwrap();
    assert!(matches!(statement, SqlStatement::Commit));

    // Test case insensitive
    let sql = "commit";
    let result = parse_sql(sql);
    assert!(result.is_ok());
    let (_, statement) = result.unwrap();
    assert!(matches!(statement, SqlStatement::Commit));

    // Test with extra spaces
    let sql = "  COMMIT  ";
    let result = parse_sql(sql);
    assert!(result.is_ok());
    let (_, statement) = result.unwrap();
    assert!(matches!(statement, SqlStatement::Commit));
}

/// Test parsing ROLLBACK statements
#[test]
fn test_parse_rollback() {
    // Test basic ROLLBACK
    let sql = "ROLLBACK";
    let result = parse_sql(sql);
    assert!(result.is_ok());
    let (_, statement) = result.unwrap();
    assert!(matches!(statement, SqlStatement::Rollback));

    // Test case insensitive
    let sql = "rollback";
    let result = parse_sql(sql);
    assert!(result.is_ok());
    let (_, statement) = result.unwrap();
    assert!(matches!(statement, SqlStatement::Rollback));

    // Test with extra spaces
    let sql = "  ROLLBACK  ";
    let result = parse_sql(sql);
    assert!(result.is_ok());
    let (_, statement) = result.unwrap();
    assert!(matches!(statement, SqlStatement::Rollback));
}

/// Test transaction statement priority in parsing
#[test]
fn test_transaction_statement_priority() {
    // Verify that transaction statements are parsed correctly
    // even when they might conflict with other keywords
    
    let statements = vec![
        "BEGIN",
        "COMMIT", 
        "ROLLBACK",
        "START TRANSACTION",
    ];

    for sql in statements {
        let result = parse_sql(sql);
        assert!(result.is_ok(), "Failed to parse: {}", sql);
        let (_, statement) = result.unwrap();
        
        match sql {
            "BEGIN" | "START TRANSACTION" => assert!(matches!(statement, SqlStatement::Begin)),
            "COMMIT" => assert!(matches!(statement, SqlStatement::Commit)),
            "ROLLBACK" => assert!(matches!(statement, SqlStatement::Rollback)),
            _ => panic!("Unexpected SQL statement: {}", sql),
        }
    }
}

/// Test invalid transaction statements
#[test]
fn test_invalid_transaction_statements() {
    let invalid_statements = vec![
        "BEGINS",           // Invalid variant
        "COMMITS",          // Invalid variant  
        "ROLLBACKS",        // Invalid variant
        "START",            // Incomplete
        "TRANSACTION",      // Incomplete
        "BEGIN COMMIT",     // Mixed statements
        "COMMIT ROLLBACK",  // Mixed statements
    ];

    for sql in invalid_statements {
        let result = parse_sql(sql);
        // These should either fail to parse or parse as something else
        if let Ok((remaining, _)) = result {
            // If it parses, there should be remaining input indicating partial parse
            assert!(!remaining.trim().is_empty(), "Unexpected successful parse for: {}", sql);
        }
        // Otherwise it failed to parse, which is expected
    }
}