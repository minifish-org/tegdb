//! Unit test to verify SELECT parsing/execution entry using Database::query with normalization

use tegdb::Database;

#[test]
fn test_parse_basic_select_copy() {
    let mut db = Database::open("file:///tmp/parse_embed_test_basic.teg").unwrap();
    let _ = db.execute("DROP TABLE test;");
    db.execute("CREATE TABLE test (id INTEGER PRIMARY KEY, embedding VECTOR(3));")
        .unwrap();

    let res = db.query("SELECT COSINE_SIMILARITY(embedding, [1.0, 0.0, 0.0]) FROM test;");
    assert!(
        res.is_ok(),
        "SELECT with COSINE_SIMILARITY should parse via query()"
    );
}

#[test]
fn test_parse_abs_same_pattern() {
    let mut db = Database::open("file:///tmp/parse_embed_test_abs.teg").unwrap();
    let _ = db.execute("CREATE TABLE test (id INTEGER PRIMARY KEY);");
    let res = db.query("SELECT ABS(-5) FROM test;");
    assert!(res.is_ok(), "SELECT with ABS should parse via query()");
}

#[test]
fn test_parse_embed_same_pattern() {
    let mut db = Database::open("file:///tmp/parse_embed_test_embed.teg").unwrap();
    let _ = db.execute("DROP TABLE test;");
    db.execute("CREATE TABLE test (id INTEGER PRIMARY KEY, vec VECTOR(8));")
        .unwrap();

    let res = db.query("SELECT EMBED('hello') FROM test;");
    assert!(res.is_ok(), "SELECT with EMBED should parse via query()");
}
