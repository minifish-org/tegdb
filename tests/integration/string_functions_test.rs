mod test_helpers {
    include!("../common/test_helpers.rs");
}
use test_helpers::run_with_both_backends;

use tegdb::{Database, SqlValue, StringFunctionsExtension};

#[test]
fn test_upper_function() {
    let _ = run_with_both_backends("test_upper_function", |db_path| {
        let mut db = Database::open(db_path).unwrap();
        db.register_extension(Box::new(StringFunctionsExtension)).unwrap();

        // Basic uppercase conversion
        let sql = "SELECT UPPER('hello')";
        let result = db.query(sql).unwrap();
        assert_eq!(result.rows()[0][0], SqlValue::Text("HELLO".to_string()));

        // Empty string
        let result = db.query("SELECT UPPER('')").unwrap();
        assert_eq!(result.rows()[0][0], SqlValue::Text("".to_string()));

        // Already uppercase
        let result = db.query("SELECT UPPER('WORLD')").unwrap();
        assert_eq!(result.rows()[0][0], SqlValue::Text("WORLD".to_string()));

        // Mixed case
        let result = db.query("SELECT UPPER('Hello World')").unwrap();
        assert_eq!(
            result.rows()[0][0],
            SqlValue::Text("HELLO WORLD".to_string())
        );

        // Unicode
        let result = db.query("SELECT UPPER('café')").unwrap();
        assert_eq!(result.rows()[0][0], SqlValue::Text("CAFÉ".to_string()));

        // NULL handling
        let result = db.query("SELECT UPPER(NULL)").unwrap();
        assert_eq!(result.rows()[0][0], SqlValue::Null);

        Ok(())
    });
}

#[test]
fn test_lower_function() {
    let _ = run_with_both_backends("test_lower_function", |db_path| {
        let mut db = Database::open(db_path).unwrap();
        db.register_extension(Box::new(StringFunctionsExtension)).unwrap();

        // Basic lowercase conversion
        let result = db.query("SELECT LOWER('HELLO')").unwrap();
        assert_eq!(result.rows()[0][0], SqlValue::Text("hello".to_string()));

        // Empty string
        let result = db.query("SELECT LOWER('')").unwrap();
        assert_eq!(result.rows()[0][0], SqlValue::Text("".to_string()));

        // Already lowercase
        let result = db.query("SELECT LOWER('world')").unwrap();
        assert_eq!(result.rows()[0][0], SqlValue::Text("world".to_string()));

        // Mixed case
        let result = db.query("SELECT LOWER('Hello World')").unwrap();
        assert_eq!(
            result.rows()[0][0],
            SqlValue::Text("hello world".to_string())
        );

        // Unicode
        let result = db.query("SELECT LOWER('CAFÉ')").unwrap();
        assert_eq!(result.rows()[0][0], SqlValue::Text("café".to_string()));

        // NULL handling
        let result = db.query("SELECT LOWER(NULL)").unwrap();
        assert_eq!(result.rows()[0][0], SqlValue::Null);

        Ok(())
    });
}

#[test]
fn test_length_function() {
    let _ = run_with_both_backends("test_length_function", |db_path| {
        let mut db = Database::open(db_path).unwrap();
        db.register_extension(Box::new(StringFunctionsExtension)).unwrap();

        // Basic length
        let result = db.query("SELECT LENGTH('hello')").unwrap();
        assert_eq!(result.rows()[0][0], SqlValue::Integer(5));

        // Empty string
        let result = db.query("SELECT LENGTH('')").unwrap();
        assert_eq!(result.rows()[0][0], SqlValue::Integer(0));

        // Unicode characters (counted as single characters)
        let result = db.query("SELECT LENGTH('café')").unwrap();
        assert_eq!(result.rows()[0][0], SqlValue::Integer(4));

        // Multi-byte unicode
        let result = db.query("SELECT LENGTH('🚀')").unwrap();
        assert_eq!(result.rows()[0][0], SqlValue::Integer(1));

        // NULL handling
        let result = db.query("SELECT LENGTH(NULL)").unwrap();
        assert_eq!(result.rows()[0][0], SqlValue::Null);

        Ok(())
    });
}

#[test]
fn test_trim_functions() {
    let _ = run_with_both_backends("test_trim_functions", |db_path| {
        let mut db = Database::open(db_path).unwrap();
        db.register_extension(Box::new(StringFunctionsExtension)).unwrap();

        // TRIM - both sides
        let result = db.query("SELECT TRIM('  hello  ')").unwrap();
        assert_eq!(result.rows()[0][0], SqlValue::Text("hello".to_string()));

        // LTRIM - left side only
        let result = db.query("SELECT LTRIM('  hello  ')").unwrap();
        assert_eq!(result.rows()[0][0], SqlValue::Text("hello  ".to_string()));

        // RTRIM - right side only
        let result = db.query("SELECT RTRIM('  hello  ')").unwrap();
        assert_eq!(result.rows()[0][0], SqlValue::Text("  hello".to_string()));

        // Tabs
        let result = db.query("SELECT TRIM('\thello\t')").unwrap();
        assert_eq!(result.rows()[0][0], SqlValue::Text("hello".to_string()));

        // Newlines
        let result = db.query("SELECT TRIM('\nhello\n')").unwrap();
        assert_eq!(result.rows()[0][0], SqlValue::Text("hello".to_string()));

        // No whitespace
        let result = db.query("SELECT TRIM('hello')").unwrap();
        assert_eq!(result.rows()[0][0], SqlValue::Text("hello".to_string()));

        // Only whitespace
        let result = db.query("SELECT TRIM('   ')").unwrap();
        assert_eq!(result.rows()[0][0], SqlValue::Text("".to_string()));

        // NULL handling
        let result = db.query("SELECT TRIM(NULL)").unwrap();
        assert_eq!(result.rows()[0][0], SqlValue::Null);

        Ok(())
    });
}

#[test]
fn test_substr_function() {
    let _ = run_with_both_backends("test_substr_function", |db_path| {
        let mut db = Database::open(db_path).unwrap();
        db.register_extension(Box::new(StringFunctionsExtension)).unwrap();

        // Basic substring
        let result = db.query("SELECT SUBSTR('hello world', 1, 5)").unwrap();
        assert_eq!(result.rows()[0][0], SqlValue::Text("hello".to_string()));

        // Start from middle
        let result = db.query("SELECT SUBSTR('hello world', 7, 5)").unwrap();
        assert_eq!(result.rows()[0][0], SqlValue::Text("world".to_string()));

        // Length beyond string
        let result = db.query("SELECT SUBSTR('hello', 1, 100)").unwrap();
        assert_eq!(result.rows()[0][0], SqlValue::Text("hello".to_string()));

        // Start beyond string
        let result = db.query("SELECT SUBSTR('hello', 10, 5)").unwrap();
        assert_eq!(result.rows()[0][0], SqlValue::Text("".to_string()));

        // Zero length
        let result = db.query("SELECT SUBSTR('hello', 1, 0)").unwrap();
        assert_eq!(result.rows()[0][0], SqlValue::Text("".to_string()));

        // Unicode characters
        let result = db.query("SELECT SUBSTR('café', 1, 3)").unwrap();
        assert_eq!(result.rows()[0][0], SqlValue::Text("caf".to_string()));

        // NULL handling
        let result = db.query("SELECT SUBSTR(NULL, 1, 5)").unwrap();
        assert_eq!(result.rows()[0][0], SqlValue::Null);

        Ok(())
    });
}

#[test]
fn test_replace_function() {
    let _ = run_with_both_backends("test_replace_function", |db_path| {
        let mut db = Database::open(db_path).unwrap();
        db.register_extension(Box::new(StringFunctionsExtension)).unwrap();

        // Basic replacement
        let result = db.query("SELECT REPLACE('hello world', 'world', 'TegDB')").unwrap();
        assert_eq!(
            result.rows()[0][0],
            SqlValue::Text("hello TegDB".to_string())
        );

        // Multiple occurrences
        let result = db.query("SELECT REPLACE('hello hello', 'hello', 'hi')").unwrap();
        assert_eq!(result.rows()[0][0], SqlValue::Text("hi hi".to_string()));

        // No match
        let result = db.query("SELECT REPLACE('hello', 'xyz', 'abc')").unwrap();
        assert_eq!(result.rows()[0][0], SqlValue::Text("hello".to_string()));

        // Empty replacement
        let result = db.query("SELECT REPLACE('hello world', 'world', '')").unwrap();
        assert_eq!(result.rows()[0][0], SqlValue::Text("hello ".to_string()));

        // Empty search string
        let result = db.query("SELECT REPLACE('hello', '', 'x')").unwrap();
        assert_eq!(result.rows()[0][0], SqlValue::Text("xhellox".to_string()));

        // NULL handling
        let result = db.query("SELECT REPLACE(NULL, 'a', 'b')").unwrap();
        assert_eq!(result.rows()[0][0], SqlValue::Null);

        Ok(())
    });
}

#[test]
fn test_concat_function() {
    let _ = run_with_both_backends("test_concat_function", |db_path| {
        let mut db = Database::open(db_path).unwrap();
        db.register_extension(Box::new(StringFunctionsExtension)).unwrap();

        // Two arguments
        let result = db.query("SELECT CONCAT('hello', ' world')").unwrap();
        assert_eq!(
            result.rows()[0][0],
            SqlValue::Text("hello world".to_string())
        );

        // Three arguments
        let result = db.query("SELECT CONCAT('hello', ' ', 'world')").unwrap();
        assert_eq!(
            result.rows()[0][0],
            SqlValue::Text("hello world".to_string())
        );

        // Multiple arguments
        let result = db.query("SELECT CONCAT('a', 'b', 'c', 'd')").unwrap();
        assert_eq!(result.rows()[0][0], SqlValue::Text("abcd".to_string()));

        // NULL handling - NULL is ignored
        let result = db.query("SELECT CONCAT('hello', NULL, 'world')").unwrap();
        assert_eq!(
            result.rows()[0][0],
            SqlValue::Text("helloworld".to_string())
        );

        // Mixed types
        let result = db.query("SELECT CONCAT('Number: ', 42)").unwrap();
        assert_eq!(
            result.rows()[0][0],
            SqlValue::Text("Number: 42".to_string())
        );

        // Real numbers
        let result = db.query("SELECT CONCAT('Value: ', 3.14)").unwrap();
        assert_eq!(
            result.rows()[0][0],
            SqlValue::Text("Value: 3.14".to_string())
        );

        // Empty strings
        let result = db.query("SELECT CONCAT('', 'hello', '')").unwrap();
        assert_eq!(result.rows()[0][0], SqlValue::Text("hello".to_string()));

        Ok(())
    });
}

#[test]
fn test_reverse_function() {
    let _ = run_with_both_backends("test_reverse_function", |db_path| {
        let mut db = Database::open(db_path).unwrap();
        db.register_extension(Box::new(StringFunctionsExtension)).unwrap();

        // Basic reverse
        let result = db.query("SELECT REVERSE('hello')").unwrap();
        assert_eq!(result.rows()[0][0], SqlValue::Text("olleh".to_string()));

        // Empty string
        let result = db.query("SELECT REVERSE('')").unwrap();
        assert_eq!(result.rows()[0][0], SqlValue::Text("".to_string()));

        // Single character
        let result = db.query("SELECT REVERSE('a')").unwrap();
        assert_eq!(result.rows()[0][0], SqlValue::Text("a".to_string()));

        // Unicode characters
        let result = db.query("SELECT REVERSE('café')").unwrap();
        assert_eq!(result.rows()[0][0], SqlValue::Text("éfac".to_string()));

        // Multi-byte unicode
        let result = db.query("SELECT REVERSE('🚀🌍')").unwrap();
        assert_eq!(result.rows()[0][0], SqlValue::Text("🌍🚀".to_string()));

        // NULL handling
        let result = db.query("SELECT REVERSE(NULL)").unwrap();
        assert_eq!(result.rows()[0][0], SqlValue::Null);

        Ok(())
    });
}

#[test]
fn test_string_functions_in_table_queries() {
    let _ = run_with_both_backends("test_string_functions_in_table_queries", |db_path| {
        let mut db = Database::open(db_path).unwrap();
        db.register_extension(Box::new(StringFunctionsExtension)).unwrap();

        // Create table with text data
        db.execute("CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT(50), email TEXT(50))").unwrap();
        db.execute("INSERT INTO users (id, name, email) VALUES (1, 'Alice', 'ALICE@EXAMPLE.COM'), (2, 'Bob', 'bob@example.com')").unwrap();

        // Use functions in SELECT
        let result = db.query("SELECT UPPER(name), LOWER(email) FROM users WHERE id = 1").unwrap();
        assert_eq!(result.rows()[0][0], SqlValue::Text("ALICE".to_string()));
        assert_eq!(
            result.rows()[0][1],
            SqlValue::Text("alice@example.com".to_string())
        );

        // Use functions in WHERE clause
        let result = db.query("SELECT name FROM users WHERE UPPER(name) = 'ALICE'").unwrap();
        assert_eq!(result.len(), 1);
        assert_eq!(result.rows()[0][0], SqlValue::Text("Alice".to_string()));

        // Use LENGTH in SELECT
        let result = db.query("SELECT LENGTH(name) FROM users WHERE id = 1").unwrap();
        assert_eq!(result.rows()[0][0], SqlValue::Integer(5));

        // Use CONCAT in SELECT
        let result = db.query("SELECT CONCAT(name, ' (', email, ')') FROM users WHERE id = 1").unwrap();
        assert_eq!(
            result.rows()[0][0],
            SqlValue::Text("Alice (ALICE@EXAMPLE.COM)".to_string())
        );

        Ok(())
    });
}
