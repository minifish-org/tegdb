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

// Cloud sync module (tegstream) - only available with cloud-sync feature
#[cfg(feature = "cloud-sync")]
pub mod tegstream;

// Only export the high-level Database API and essential error types
pub use database::{Database, DatabaseTransaction, PreparedStatement, QueryResult};
pub use error::{Error, Result};

// Export schema-related types that are needed for public API (get_table_schemas_ref)
pub use parser::{ColumnConstraint, DataType};
pub use query_processor::{ColumnInfo, TableSchema};

// Export parse_sql since it's used by the public Database API (prepare method)
pub use parser::parse_sql;

// Conditionally expose low-level API for development, examples, and benchmarks
#[cfg(feature = "dev")]
pub use catalog::Catalog;
#[cfg(feature = "dev")]
pub use parser::{
    debug_parse_sql, parse_sql_with_suggestions, Assignment, ColumnDefinition, ComparisonOperator,
    Condition, CreateTableStatement, DeleteStatement, DropTableStatement, Expression,
    InsertStatement, ParseError, SelectStatement, Statement, UpdateStatement, WhereClause,
};
#[cfg(feature = "dev")]
pub use planner::{ExecutionPlan, QueryPlanner};
#[cfg(feature = "dev")]
pub use query_processor::{QueryProcessor, ResultSet};
#[cfg(feature = "dev")]
pub use storage_engine::{EngineConfig, StorageEngine, Transaction};
#[cfg(feature = "dev")]
pub use storage_format::StorageFormat;
#[cfg(feature = "dev")]
pub use vector_index::{HNSWIndex, IVFIndex, LSHIndex};

// Export SqlValue unconditionally as it's needed for working with query results
pub use parser::SqlValue;

// Export embedding functionality for semantic search
pub use embedding::{cosine_similarity, embed, EmbeddingModel};

// For backward compatibility, also expose via modules when dev feature is enabled
#[cfg(feature = "dev")]
pub mod low_level {
    pub use crate::catalog::Catalog;
    pub use crate::parser::{parse_sql, SqlValue, Statement};
    pub use crate::planner::{ExecutionPlan, QueryPlanner};
    pub use crate::query_processor::{ColumnInfo, QueryProcessor, ResultSet, TableSchema};
    pub use crate::storage_engine::{
        EngineConfig, StorageEngine, Transaction as EngineTransaction,
    };
}
