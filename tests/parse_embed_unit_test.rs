//! Unit tests for EMBED/ABS parser behavior using direct parse_sql

use tegdb::parser::{parse_sql, Statement};

#[test]
#[cfg(feature = "dev")]
fn test_parse_basic_select() {
    let result = parse_sql("SELECT id FROM test;");
    assert!(
        matches!(result, Ok(Statement::Select(_))),
        "Failed to parse basic SELECT: {:?}",
        result.err()
    );
}

#[test]
#[cfg(feature = "dev")]
#[ignore]
fn test_parse_select_with_abs() {
    let result = parse_sql("SELECT ABS(-5) as result;");
    assert!(
        matches!(result, Ok(Statement::Select(_))),
        "Failed to parse SELECT with ABS: {:?}",
        result.err()
    );
}

#[test]
#[cfg(feature = "dev")]
#[ignore]
fn test_parse_embed_directly() {
    let result = parse_sql("SELECT EMBED('hello') as embedding;");
    assert!(
        matches!(result, Ok(Statement::Select(_))),
        "Failed to parse SELECT with EMBED: {:?}",
        result.err()
    );

    let result2 = parse_sql("INSERT INTO test (id, vec) VALUES (1, EMBED('hello'));");
    assert!(
        matches!(result2, Ok(Statement::Insert(_))),
        "Failed to parse INSERT with EMBED: {:?}",
        result2.err()
    );
}
