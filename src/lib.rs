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
//!     // Open or create a database (.teg enforced)
//!     let mut db = Database::open("file:///absolute/path/to/my_database.teg")?;
//!     
//!     // Create table
//!     db.execute("CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT(32), age INTEGER)")?;
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
pub mod backends;
pub mod catalog;
pub mod database;
pub mod embedding;
pub mod error;
pub mod log;
pub mod protocol_utils;
pub mod sql_utils;
pub mod storage_engine;
pub mod storage_format;
pub mod vector_index;

// Make parser module public since it contains types needed for public API (DataType, ColumnConstraint)
pub mod parser;

// Make query_processor module public since it contains types needed for public API (TableSchema, ColumnInfo)
pub mod query_processor;

// Planner modules are now always available since they're the main execution path
pub mod planner;

// Cloud sync module (tgstream)
#[cfg(feature = "tgstream")]
pub mod tgstream;

// Extension system
pub mod extension;

// Only export the high-level Database API and essential error types
pub use database::{Database, DatabaseTransaction, PreparedStatement, QueryResult};
pub use error::{Error, Result};

// Export extension system types
pub use extension::{
    AggregateFunction, AggregateState, ArgType, Extension, ExtensionError, ExtensionFactory,
    ExtensionRegistry, ExtensionResult, ExtensionWrapper, FunctionSignature,
    MathFunctionsExtension, ScalarFunction, StringFunctionsExtension,
};

// Export schema-related types that are needed for public API (get_table_schemas_ref)
pub use parser::{ColumnConstraint, DataType};
pub use query_processor::{ColumnInfo, TableSchema};

// Export parse_sql since it's used by the public Database API (prepare method)
pub use parser::parse_sql;

// Export SqlValue unconditionally as it's needed for working with query results
pub use parser::SqlValue;

// Export embedding functionality for semantic search
pub use embedding::{cosine_similarity, embed, EmbeddingModel};
