//! # TegDB - A high-performance, embedded database engine
//!
//! TegDB provides a high-level SQLite-like database interface for easy database operations.
//!
//! ## Database API
//!
//! The `Database` struct provides a SQLite-like interface for easy database operations:
//!
//! ```no_run
//! use tegdb::{Database, Result};
//!
//! # fn main() -> Result<()> {
//!     // Open or create a database
//!     let mut db = Database::open("file://my_database.db")?;
//!     
//!     // Create table
//!     db.execute("CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT, age INTEGER)")?;
//!     
//!     // Insert data
//!     db.execute("INSERT INTO users (id, name, age) VALUES (1, 'Alice', 30)")?;
//!     
//!     // Query data
//!     let result = db.query("SELECT * FROM users")?;
//!     for row in result.rows() {
//!         println!("User row: {:?}", row);
//!     }
//!     
//!     // Use transactions
//!     let mut tx = db.begin_transaction()?;
//!     tx.execute("UPDATE users SET age = 31 WHERE id = 1")?;
//!     tx.commit()?;
//!     
//!     # Ok(())
//! # }
//! ```
//!
//! ## Low-level API (Advanced Users)
//!
//! For advanced use cases, benchmarks, or examples, you can enable the `dev` feature
//! to access the low-level engine API:
//!
//! ```toml
//! [dependencies]
//! tegdb = { version = "0.2", features = ["dev"] }
//! ```
//!
//! This exposes additional types like `Engine`, `EngineConfig`, `Executor`, etc.
//! for direct engine manipulation.
pub mod backends;
pub mod catalog;
pub mod database;
pub mod error;
pub mod log;
pub mod protocol_utils;
pub mod sql_utils;
pub mod storage_engine;
pub mod storage_format;

// Make these modules public when dev feature is enabled or when running tests
#[cfg(any(feature = "dev", test))]
pub mod parser;
#[cfg(not(any(feature = "dev", test)))]
mod parser;

#[cfg(any(feature = "dev", test))]
pub mod query;
#[cfg(not(any(feature = "dev", test)))]
mod query;

// Planner modules are now always available since they're the main execution path
pub mod planner;

// Only export the high-level Database API and essential error types
pub use database::{Database, DatabaseTransaction, QueryResult};
pub use error::{Error, Result};

// Conditionally expose low-level API for development, examples, and benchmarks
#[cfg(feature = "dev")]
pub use catalog::Catalog;
#[cfg(feature = "dev")]
pub use parser::{
    parse_sql, Assignment, ColumnConstraint, ColumnDefinition, ComparisonOperator, Condition,
    CreateTableStatement, DataType, DeleteStatement, DropTableStatement, InsertStatement,
    OrderByClause, OrderDirection, SelectStatement, Statement, UpdateStatement, WhereClause,
};
#[cfg(feature = "dev")]
pub use planner::{
    ColumnStatistics, Cost, ExecutionPlan, PlannerConfig, QueryPlanner, TableStatistics,
};
#[cfg(feature = "dev")]
pub use query::{ColumnInfo, QueryProcessor, ResultSet, TableSchema};
#[cfg(feature = "dev")]
pub use storage_engine::{EngineConfig, StorageEngine, Transaction};
#[cfg(feature = "dev")]
pub use storage_format::StorageFormat;

// Export SqlValue unconditionally as it's needed for working with query results
pub use parser::SqlValue;

// For backward compatibility, also expose via modules when dev feature is enabled
#[cfg(feature = "dev")]
pub mod low_level {
    pub use crate::catalog::Catalog;
    pub use crate::parser::{parse_sql, SqlValue, Statement};
    pub use crate::planner::{ExecutionPlan, PlannerConfig, QueryPlanner};
    pub use crate::query::{ColumnInfo, QueryProcessor, ResultSet, TableSchema};
    pub use crate::storage_engine::{
        EngineConfig, StorageEngine, Transaction as EngineTransaction,
    };
}

#[cfg(target_arch = "wasm32")]
use wasm_bindgen::prelude::*;

#[cfg(target_arch = "wasm32")]
#[wasm_bindgen]
pub fn hello_wasm() -> JsValue {
    JsValue::from_str("Hello from WASM!")
}

#[cfg(target_arch = "wasm32")]
#[wasm_bindgen]
pub fn debug_protocol_parsing() -> JsValue {
    use crate::protocol_utils::parse_storage_identifier;

    let test_identifiers = vec![
        "localstorage://test_db",
        "browser://test_db",
        "file://test_db",
    ];

    let mut results = Vec::new();
    for identifier in test_identifiers {
        let (protocol, path) = parse_storage_identifier(identifier);
        results.push(format!(
            "'{}' -> protocol: '{}', path: '{}'",
            identifier, protocol, path
        ));
    }

    JsValue::from_str(&results.join("\n"))
}

#[cfg(target_arch = "wasm32")]
#[wasm_bindgen]
pub fn wasm_test_runner() -> JsValue {
    // Run comprehensive WASM tests and return results
    let mut results = Vec::new();

    // Test 1: Basic database creation
    match test_wasm_database_creation() {
        Ok(_) => results.push("✓ Basic database creation test passed".to_string()),
        Err(e) => results.push(format!("✗ Basic database creation test failed: {}", e)),
    }

    // Test 2: Basic CRUD operations
    match test_wasm_basic_crud() {
        Ok(_) => results.push("✓ Basic CRUD operations test passed".to_string()),
        Err(e) => results.push(format!("✗ Basic CRUD operations test failed: {}", e)),
    }

    // Test 3: Transaction operations
    match test_wasm_transactions() {
        Ok(_) => results.push("✓ Transaction operations test passed".to_string()),
        Err(e) => results.push(format!("✗ Transaction operations test failed: {}", e)),
    }

    // Test 4: Data types
    match test_wasm_data_types() {
        Ok(_) => results.push("✓ Data types test passed".to_string()),
        Err(e) => results.push(format!("✗ Data types test failed: {}", e)),
    }

    // Test 5: Comprehensive test suite
    match run_comprehensive_wasm_tests() {
        Ok(_) => results.push("✓ Comprehensive test suite passed".to_string()),
        Err(e) => results.push(format!("✗ Comprehensive test suite failed: {}", e)),
    }

    // Join all results
    let result_string = results.join("\n");
    JsValue::from_str(&result_string)
}

#[cfg(target_arch = "wasm32")]
#[wasm_bindgen]
pub fn comprehensive_wasm_test_runner() -> JsValue {
    // Run the full comprehensive test suite on WASM
    let mut results = Vec::new();

    // Test categories
    let test_categories: Vec<(&str, fn() -> crate::Result<()>)> = vec![
        (
            "Database Operations",
            test_wasm_database_operations as fn() -> crate::Result<()>,
        ),
        (
            "Transaction Patterns",
            test_wasm_transaction_patterns as fn() -> crate::Result<()>,
        ),
        (
            "Schema Operations",
            test_wasm_schema_operations as fn() -> crate::Result<()>,
        ),
        (
            "Query Operations",
            test_wasm_query_operations as fn() -> crate::Result<()>,
        ),
        (
            "Error Scenarios",
            test_wasm_error_scenarios as fn() -> crate::Result<()>,
        ),
        (
            "Advanced Database Tests",
            test_wasm_advanced_database_tests as fn() -> crate::Result<()>,
        ),
        (
            "SQL Integration Tests",
            test_wasm_sql_integration_tests as fn() -> crate::Result<()>,
        ),
        (
            "Transaction Integration Tests",
            test_wasm_transaction_integration_tests as fn() -> crate::Result<()>,
        ),
        (
            "Schema Persistence Tests",
            test_wasm_schema_persistence_tests as fn() -> crate::Result<()>,
        ),
        (
            "Query Iterator Tests",
            test_wasm_query_iterator_tests as fn() -> crate::Result<()>,
        ),
    ];

    for (category_name, test_fn) in test_categories {
        match test_fn() {
            Ok(_) => results.push(format!("✓ {} passed", category_name)),
            Err(e) => results.push(format!("✗ {} failed: {}", category_name, e)),
        }
    }

    // Join all results
    let result_string = results.join("\n");
    JsValue::from_str(&result_string)
}

#[cfg(target_arch = "wasm32")]
fn test_wasm_database_creation() -> crate::Result<()> {
    // Test that we can create a database on WASM
    let mut db = Database::open("localstorage://test_db")?;

    // Clean up any existing data
    let _ = db.execute("DROP TABLE IF EXISTS test_creation");

    // Test basic operations
    db.execute("CREATE TABLE test_creation (id INTEGER PRIMARY KEY, name TEXT)")?;
    db.execute("INSERT INTO test_creation (id, name) VALUES (1, 'test')")?;

    let result = db.query("SELECT * FROM test_creation")?;
    assert_eq!(result.rows().len(), 1);

    Ok(())
}

#[cfg(target_arch = "wasm32")]
fn test_wasm_basic_crud() -> crate::Result<()> {
    let mut db = Database::open("localstorage://test_crud")?;

    // Clean up any existing data
    let _ = db.execute("DROP TABLE IF EXISTS users_crud");

    // Create table
    db.execute(
        "CREATE TABLE users_crud (id INTEGER PRIMARY KEY, name TEXT NOT NULL, age INTEGER)",
    )?;

    // Insert data
    db.execute("INSERT INTO users_crud (id, name, age) VALUES (1, 'Alice', 30)")?;
    db.execute("INSERT INTO users_crud (id, name, age) VALUES (2, 'Bob', 25)")?;

    // Query data
    let result = db.query("SELECT * FROM users_crud")?;
    assert_eq!(result.rows().len(), 2);

    // Update data
    db.execute("UPDATE users_crud SET age = 31 WHERE name = 'Alice'")?;

    // Delete data
    db.execute("DELETE FROM users_crud WHERE age < 30")?;

    // Verify final state
    let result = db.query("SELECT * FROM users_crud")?;
    assert_eq!(result.rows().len(), 1);

    Ok(())
}

#[cfg(target_arch = "wasm32")]
fn test_wasm_transactions() -> crate::Result<()> {
    let mut db = Database::open("localstorage://test_transactions")?;

    // Clean up any existing data
    let _ = db.execute("DROP TABLE IF EXISTS accounts_tx");

    // Setup test table
    db.execute("CREATE TABLE accounts_tx (id INTEGER PRIMARY KEY, balance INTEGER)")?;
    db.execute("INSERT INTO accounts_tx (id, balance) VALUES (1, 1000)")?;
    db.execute("INSERT INTO accounts_tx (id, balance) VALUES (2, 500)")?;

    // Test successful transaction
    {
        let mut tx = db.begin_transaction()?;
        tx.execute("UPDATE accounts_tx SET balance = 800 WHERE id = 1")?;
        tx.execute("UPDATE accounts_tx SET balance = 700 WHERE id = 2")?;
        tx.commit()?;
    }

    // Verify changes persisted
    let result = db.query("SELECT balance FROM accounts_tx ORDER BY id")?;
    assert_eq!(result.rows().len(), 2);

    // Test rollback
    {
        let mut tx = db.begin_transaction()?;
        tx.execute("UPDATE accounts_tx SET balance = 0 WHERE id = 1")?;
        tx.rollback()?;
    }

    // Verify rollback worked
    let result = db.query("SELECT balance FROM accounts_tx WHERE id = 1")?;
    assert_eq!(result.rows()[0][0], crate::SqlValue::Integer(800));

    Ok(())
}

#[cfg(target_arch = "wasm32")]
fn test_wasm_data_types() -> crate::Result<()> {
    let mut db = Database::open("localstorage://test_data_types")?;

    // Clean up any existing data
    let _ = db.execute("DROP TABLE IF EXISTS test_types_dt");

    // Test all supported data types
    db.execute("CREATE TABLE test_types_dt (id INTEGER PRIMARY KEY, text_col TEXT, int_col INTEGER, real_col REAL, null_col TEXT)")?;

    // Insert data with different types
    db.execute("INSERT INTO test_types_dt (id, text_col, int_col, real_col, null_col) VALUES (1, 'hello', 42, 3.14159, NULL)")?;
    db.execute("INSERT INTO test_types_dt (id, text_col, int_col, real_col, null_col) VALUES (2, 'world', -100, -2.5, 'not null')")?;

    let result = db.query("SELECT * FROM test_types_dt ORDER BY id")?;
    assert_eq!(result.rows().len(), 2);

    // Verify data types
    let row1 = &result.rows()[0];
    assert_eq!(row1[0], crate::SqlValue::Integer(1));
    assert_eq!(row1[1], crate::SqlValue::Text("hello".to_string()));
    assert_eq!(row1[2], crate::SqlValue::Integer(42));
    assert_eq!(row1[3], crate::SqlValue::Real(3.14159));
    assert_eq!(row1[4], crate::SqlValue::Null);

    Ok(())
}

#[cfg(target_arch = "wasm32")]
fn run_comprehensive_wasm_tests() -> crate::Result<()> {
    // Import test modules and run them with WASM backend
    // This simulates running the full test suite on WASM

    // Test database operations with both backends pattern
    test_wasm_database_operations()?;
    test_wasm_transaction_patterns()?;
    test_wasm_schema_operations()?;
    test_wasm_query_operations()?;
    test_wasm_error_scenarios()?;

    Ok(())
}

#[cfg(target_arch = "wasm32")]
fn test_wasm_database_operations() -> crate::Result<()> {
    // Test basic database operations using the test helper pattern
    let _test_name = "wasm_database_operations";

    // Simulate run_with_both_backends for WASM
    let db_path = "localstorage://test_db_ops_unique";
    let mut db = Database::open(db_path)?;

    // Clean up first
    let _ = db.execute("DROP TABLE IF EXISTS users");

    // Test CREATE TABLE
    let affected =
        db.execute("CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT NOT NULL, age INTEGER)")?;
    assert_eq!(affected, 0);

    // Test INSERT
    let affected = db.execute("INSERT INTO users (id, name, age) VALUES (1, 'Alice', 30)")?;
    assert_eq!(affected, 1);

    // Test SELECT
    let result = db.query("SELECT * FROM users")?;
    assert_eq!(result.rows().len(), 1);

    // Test UPDATE
    let affected = db.execute("UPDATE users SET age = 31 WHERE name = 'Alice'")?;
    assert_eq!(affected, 1);

    // Test DELETE
    let affected = db.execute("DELETE FROM users WHERE age < 30")?;
    assert_eq!(affected, 0); // No rows deleted since age is 31

    // Clean up
    let _ = db.execute("DROP TABLE IF EXISTS users");

    Ok(())
}

#[cfg(target_arch = "wasm32")]
fn test_wasm_transaction_patterns() -> crate::Result<()> {
    let db_path = "localstorage://test_tx_patterns_unique";
    let mut db = Database::open(db_path)?;

    // Clean up
    let _ = db.execute("DROP TABLE IF EXISTS accounts_tx");

    // Setup test table
    db.execute("CREATE TABLE accounts_tx (id INTEGER PRIMARY KEY, balance INTEGER)")?;
    db.execute("INSERT INTO accounts_tx (id, balance) VALUES (1, 1000)")?;
    db.execute("INSERT INTO accounts_tx (id, balance) VALUES (2, 500)")?;

    // Test successful transaction
    {
        let mut tx = db.begin_transaction()?;
        tx.execute("UPDATE accounts_tx SET balance = 800 WHERE id = 1")?;
        tx.execute("UPDATE accounts_tx SET balance = 700 WHERE id = 2")?;
        tx.commit()?;
    }

    // Verify changes persisted
    let result = db.query("SELECT balance FROM accounts_tx ORDER BY id")?;
    assert_eq!(result.rows().len(), 2);

    // Test rollback
    {
        let mut tx = db.begin_transaction()?;
        tx.execute("UPDATE accounts_tx SET balance = 0 WHERE id = 1")?;
        tx.rollback()?;
    }

    // Verify rollback worked
    let result = db.query("SELECT balance FROM accounts_tx WHERE id = 1")?;
    assert_eq!(result.rows()[0][0], crate::SqlValue::Integer(800));

    // Clean up
    let _ = db.execute("DROP TABLE IF EXISTS accounts_tx");

    Ok(())
}

#[cfg(target_arch = "wasm32")]
fn test_wasm_schema_operations() -> crate::Result<()> {
    let db_path = "localstorage://test_schema_unique";
    let mut db = Database::open(db_path)?;

    // Clean up
    let _ = db.execute("DROP TABLE IF EXISTS schema_test");

    // Test schema creation and persistence
    db.execute("CREATE TABLE schema_test (id INTEGER PRIMARY KEY, data TEXT)")?;
    db.execute("INSERT INTO schema_test (id, data) VALUES (1, 'test data')")?;

    // Verify schema and data
    let result = db.query("SELECT * FROM schema_test")?;
    assert_eq!(result.rows().len(), 1);

    // Test DROP TABLE
    let affected = db.execute("DROP TABLE schema_test")?;
    assert_eq!(affected, 0);

    // Verify table is gone
    let result = db.query("SELECT * FROM schema_test");
    assert!(result.is_err());

    Ok(())
}

#[cfg(target_arch = "wasm32")]
fn test_wasm_query_operations() -> crate::Result<()> {
    let db_path = "localstorage://test_queries_unique";
    let mut db = Database::open(db_path)?;

    // Clean up
    let _ = db.execute("DROP TABLE IF EXISTS query_test");

    // Setup test data
    db.execute(
        "CREATE TABLE query_test (id INTEGER PRIMARY KEY, name TEXT, age INTEGER, city TEXT)",
    )?;
    db.execute("INSERT INTO query_test (id, name, age, city) VALUES (1, 'Alice', 30, 'New York')")?;
    db.execute("INSERT INTO query_test (id, name, age, city) VALUES (2, 'Bob', 25, 'Boston')")?;
    db.execute("INSERT INTO query_test (id, name, age, city) VALUES (3, 'Carol', 35, 'Chicago')")?;

    // Test WHERE clause
    let result = db.query("SELECT name FROM query_test WHERE age > 30")?;
    assert_eq!(result.rows().len(), 1);

    // Test ORDER BY
    let result = db.query("SELECT name FROM query_test ORDER BY age")?;
    assert_eq!(result.rows().len(), 3);

    // Test LIMIT
    let result = db.query("SELECT name FROM query_test ORDER BY age LIMIT 2")?;
    assert_eq!(result.rows().len(), 2);

    // Clean up
    let _ = db.execute("DROP TABLE IF EXISTS query_test");

    Ok(())
}

#[cfg(target_arch = "wasm32")]
fn test_wasm_error_scenarios() -> crate::Result<()> {
    let db_path = "localstorage://test_errors";
    let mut db = Database::open(db_path)?;

    // Clean up any existing data first
    let _ = db.execute("DROP TABLE IF EXISTS error_test");

    // Test SQL parse errors
    let result = db.execute("INVALID SQL STATEMENT");
    assert!(result.is_err());

    // Test constraint violations
    db.execute("CREATE TABLE error_test (id INTEGER PRIMARY KEY, name TEXT NOT NULL)")?;

    // Test using execute() for SELECT - this should fail
    db.execute("INSERT INTO error_test (id, name) VALUES (1, 'Alice')")?;
    let result = db.execute("SELECT * FROM error_test");
    assert!(result.is_err());

    // Test the proper way to do SELECT
    let query_result = db.query("SELECT * FROM error_test");
    assert!(query_result.is_ok());

    // Test using query() for non-SELECT (should fail)
    let result = db.query("INSERT INTO error_test (id, name) VALUES (2, 'Bob')");
    assert!(result.is_err());

    // Clean up
    let _ = db.execute("DROP TABLE IF EXISTS error_test");

    Ok(())
}

#[cfg(target_arch = "wasm32")]
#[wasm_bindgen]
pub fn run_basic_test() -> JsValue {
    // Run a basic test and return the result
    match test_wasm_database_creation() {
        Ok(_) => JsValue::from_str("Basic test passed"),
        Err(e) => JsValue::from_str(&format!("Test failed: {}", e)),
    }
}



#[cfg(target_arch = "wasm32")]
fn test_wasm_advanced_database_tests() -> crate::Result<()> {
    // Test advanced database features
    let db_path = "localstorage://test_advanced_unique";
    let mut db = Database::open(db_path)?;

    // Clean up
    let _ = db.execute("DROP TABLE IF EXISTS advanced_test");

    // Test complex table creation
    db.execute("CREATE TABLE advanced_test (id INTEGER PRIMARY KEY, name TEXT NOT NULL, age INTEGER, salary REAL, active INTEGER)")?;

    // Test multiple inserts
    db.execute("INSERT INTO advanced_test (id, name, age, salary, active) VALUES (1, 'Alice', 30, 50000.0, 1)")?;
    db.execute("INSERT INTO advanced_test (id, name, age, salary, active) VALUES (2, 'Bob', 25, 45000.0, 1)")?;
    db.execute("INSERT INTO advanced_test (id, name, age, salary, active) VALUES (3, 'Carol', 35, 60000.0, 0)")?;

    // Test complex queries
    let result = db.query("SELECT name, age FROM advanced_test WHERE active = 1 AND age > 25")?;
    assert_eq!(result.rows().len(), 1); // Only Alice should match

    // Test ORDER BY
    let result = db.query("SELECT name FROM advanced_test ORDER BY age DESC")?;
    assert_eq!(result.rows().len(), 3);

    // Test LIMIT
    let result = db.query("SELECT name FROM advanced_test ORDER BY age DESC LIMIT 2")?;
    assert_eq!(result.rows().len(), 2);

    // Clean up
    let _ = db.execute("DROP TABLE IF EXISTS advanced_test");

    Ok(())
}

#[cfg(target_arch = "wasm32")]
fn test_wasm_sql_integration_tests() -> crate::Result<()> {
    // Test SQL integration features
    let db_path = "localstorage://test_sql_integration_unique";
    let mut db = Database::open(db_path)?;

    // Clean up
    let _ = db.execute("DROP TABLE IF EXISTS sql_test");

    // Test case-insensitive keywords
    db.execute("create table sql_test (id integer primary key, name text)")?;
    db.execute("insert into sql_test (id, name) values (1, 'test')")?;

    let result = db.query("select * from sql_test")?;
    assert_eq!(result.rows().len(), 1);

    // Test complex WHERE clauses
    db.execute("INSERT INTO sql_test (id, name) VALUES (2, 'test2')")?;
    db.execute("INSERT INTO sql_test (id, name) VALUES (3, 'test3')")?;

    let result = db.query("SELECT name FROM sql_test WHERE id > 1 AND name LIKE 'test%'")?;
    assert_eq!(result.rows().len(), 2);

    // Clean up
    let _ = db.execute("DROP TABLE IF EXISTS sql_test");

    Ok(())
}

#[cfg(target_arch = "wasm32")]
fn test_wasm_transaction_integration_tests() -> crate::Result<()> {
    // Test transaction integration
    let db_path = "localstorage://test_tx_integration_unique";
    let mut db = Database::open(db_path)?;

    // Clean up
    let _ = db.execute("DROP TABLE IF EXISTS tx_integration_test");

    // Setup
    db.execute(
        "CREATE TABLE tx_integration_test (id INTEGER PRIMARY KEY, step INTEGER, data TEXT)",
    )?;

    // Test auto-commit
    db.execute("INSERT INTO tx_integration_test (id, step, data) VALUES (1, 1, 'auto-commit')")?;

    // Verify immediate visibility
    let result = db.query("SELECT data FROM tx_integration_test WHERE id = 1")?;
    assert_eq!(
        result.rows()[0][0],
        crate::SqlValue::Text("auto-commit".to_string())
    );

    // Test explicit transaction
    {
        let mut tx = db.begin_transaction()?;
        tx.execute("INSERT INTO tx_integration_test (id, step, data) VALUES (2, 2, 'tx1-data')")?;
        tx.commit()?;
    }

    // Verify transaction committed
    let result = db.query("SELECT * FROM tx_integration_test ORDER BY id")?;
    assert_eq!(result.rows().len(), 2);

    // Clean up
    let _ = db.execute("DROP TABLE IF EXISTS tx_integration_test");

    Ok(())
}

#[cfg(target_arch = "wasm32")]
fn test_wasm_schema_persistence_tests() -> crate::Result<()> {
    // Test schema persistence
    let db_path = "localstorage://test_schema_persistence_unique";

    // First session: Create table
    {
        let mut db = Database::open(db_path)?;
        db.execute("CREATE TABLE schema_persistence_test (id INTEGER PRIMARY KEY, description TEXT, value INTEGER)")?;
        db.execute("INSERT INTO schema_persistence_test (id, description, value) VALUES (1, 'Critical Data', 9999)")?;
    }

    // Second session: Verify persistence
    {
        let mut db = Database::open(db_path)?;
        let result = db.query("SELECT * FROM schema_persistence_test")?;
        assert_eq!(result.rows().len(), 1);

        // Add more data
        db.execute("INSERT INTO schema_persistence_test (id, description, value) VALUES (2, 'More Data', 8888)")?;
    }

    // Third session: Verify all data persisted
    {
        let mut db = Database::open(db_path)?;
        let result = db.query("SELECT * FROM schema_persistence_test ORDER BY id")?;
        assert_eq!(result.rows().len(), 2);
    }

    // Clean up
    {
        let mut db = Database::open(db_path)?;
        let _ = db.execute("DROP TABLE IF EXISTS schema_persistence_test");
    }

    Ok(())
}

#[cfg(target_arch = "wasm32")]
fn test_wasm_query_iterator_tests() -> crate::Result<()> {
    // Test query iterator functionality
    let db_path = "localstorage://test_query_iterator_unique";
    let mut db = Database::open(db_path)?;

    // Clean up
    let _ = db.execute("DROP TABLE IF EXISTS iterator_test");

    // Setup test data
    db.execute("CREATE TABLE iterator_test (id INTEGER PRIMARY KEY, value TEXT)")?;
    db.execute("INSERT INTO iterator_test (id, value) VALUES (1, 'first')")?;
    db.execute("INSERT INTO iterator_test (id, value) VALUES (2, 'second')")?;
    db.execute("INSERT INTO iterator_test (id, value) VALUES (3, 'third')")?;

    // Test basic query
    let result = db.query("SELECT * FROM iterator_test ORDER BY id")?;
    assert_eq!(result.rows().len(), 3);

    // Test WHERE clause
    let result = db.query("SELECT value FROM iterator_test WHERE id > 1")?;
    assert_eq!(result.rows().len(), 2);

    // Test empty result
    let result = db.query("SELECT * FROM iterator_test WHERE id > 100")?;
    assert_eq!(result.rows().len(), 0);

    // Test transaction with query
    {
        let mut tx = db.begin_transaction()?;
        tx.execute("INSERT INTO iterator_test (id, value) VALUES (4, 'fourth')")?;

        let result = tx.query("SELECT * FROM iterator_test ORDER BY id")?;
        assert_eq!(result.rows().len(), 4);

        tx.commit()?;
    }

    // Verify transaction changes
    let result = db.query("SELECT * FROM iterator_test ORDER BY id")?;
    assert_eq!(result.rows().len(), 4);

    // Clean up
    let _ = db.execute("DROP TABLE IF EXISTS iterator_test");

    Ok(())
}

#[cfg(test)]
mod tests {


    #[test]
    #[cfg(target_arch = "wasm32")]
    fn test_wasm_compilation() {
        // This test just verifies that the code compiles and runs on WASM
        assert_eq!(2 + 2, 4);
    }

    #[test]
    #[cfg(target_arch = "wasm32")]
    fn test_wasm_database_creation() -> Result<()> {
        // Test that we can create a database on WASM
        let mut db = Database::open("localstorage://test_db")?;

        // Clean up any existing data
        let _ = db.execute("DROP TABLE IF EXISTS test_creation");

        // Test basic operations
        db.execute("CREATE TABLE test_creation (id INTEGER PRIMARY KEY, name TEXT)")?;
        db.execute("INSERT INTO test_creation (id, name) VALUES (1, 'test')")?;

        let result = db.query("SELECT * FROM test_creation")?;
        assert_eq!(result.rows().len(), 1);

        Ok(())
    }
}
