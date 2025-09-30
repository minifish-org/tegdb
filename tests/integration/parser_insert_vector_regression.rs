// Regression tests to exercise INSERT parsing with vector literals and strings
use tegdb::parser::parse_sql;

#[test]
fn regression_insert_vector_literal_should_parse() {
    let sql = "INSERT INTO chat_history (id, message, is_user, embedding) VALUES (1, 'hi', 1, [0.1, 0.2, 0.3])";
    let res = parse_sql(sql);
    assert!(
        res.is_ok(),
        "Failed to parse simple vector INSERT: {:?}",
        res.err()
    );
}

#[test]
fn regression_insert_single_line_string_should_parse() {
    let sql = "INSERT INTO chat_history (id, message, is_user, embedding) VALUES (1, 'hello world', 1, [0.1])";
    let res = parse_sql(sql);
    assert!(
        res.is_ok(),
        "Failed to parse single-line INSERT: {:?}",
        res.err()
    );
}

#[test]
fn regression_insert_long_string_should_parse() {
    let long_msg = "a".repeat(200);
    let sql = format!(
        "INSERT INTO chat_history (id, message, is_user, embedding) VALUES (1, '{long_msg}', 1, [0.1, 0.2])"
    );
    let res = parse_sql(&sql);
    assert!(res.is_ok(), "Failed to parse long INSERT: {:?}", res.err());
}

#[test]
fn regression_insert_beginning_at_col1_should_parse() {
    let sql = "INSERT INTO t (id) VALUES (1)";
    assert_eq!(sql.as_bytes()[0] as char, 'I');
    let res = parse_sql(sql);
    assert!(
        res.is_ok(),
        "Failed to parse INSERT at column 1: {:?}",
        res.err()
    );
}
