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
//!     let mut db = Database::open("my_database.db")?;
//!     
//!     // Create table
//!     db.execute("CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT, age INTEGER)")?;
//!     
//!     // Insert data
//!     db.execute("INSERT INTO users (id, name, age) VALUES (1, 'Alice', 30)")?;
//!     
//!     // Query data
//!     let result = db.query("SELECT * FROM users")?;
//!     for row in result.iter() {
//!         println!("User: {:?}", row.get("name"));
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
pub mod engine;
pub mod error;
pub mod database;
pub mod serialization;

// Make these modules public when dev feature is enabled so tests can access them
#[cfg(feature = "dev")]
pub mod parser;
#[cfg(not(feature = "dev"))]
mod parser;

#[cfg(feature = "dev")]
pub mod executor;
#[cfg(not(feature = "dev"))]
mod executor;

// Only export the high-level Database API and essential error types
pub use error::{Error, Result};
pub use database::{Database, QueryResult, Row, Transaction as DbTransaction};

// Conditionally expose low-level API for development, examples, and benchmarks
#[cfg(feature = "dev")]
pub use engine::{Engine, EngineConfig, Entry, Transaction};
#[cfg(feature = "dev")]
pub use executor::{Executor, ResultSet};
#[cfg(feature = "dev")]
pub use parser::{
    parse_sql, Statement, SqlValue, DataType, ColumnConstraint, ComparisonOperator, 
    OrderDirection, Condition, SelectStatement, InsertStatement, UpdateStatement, 
    DeleteStatement, CreateTableStatement, DropTableStatement, ColumnDefinition, WhereClause, 
    Assignment, OrderByClause
};

// Export SqlValue unconditionally as it's needed for working with query results
#[cfg(not(feature = "dev"))]
pub use parser::SqlValue;

// For backward compatibility, also expose via modules when dev feature is enabled
#[cfg(feature = "dev")]
pub mod low_level {
    pub use crate::engine::{Engine, Transaction as EngineTransaction, Entry, EngineConfig};
    pub use crate::executor::{Executor, ResultSet};
    pub use crate::parser::{parse_sql, Statement, SqlValue};
}
