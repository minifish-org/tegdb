//! # TegDB - A high-performance, embedded database engine
//!
//! TegDB provides both low-level engine access and a high-level SQLite-like database interface.
//!
//! ## High-level Database API (Recommended)
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
//! ## Low-level Engine API
//!
//! For advanced use cases, you can access the low-level engine directly:
//!
//! ```no_run
//! use tempfile::tempdir;
//! use tegdb::{Engine, EngineConfig, Entry, Result};
//!
//! # fn main() -> Result<()> {
//!     let dir = tempdir()?;
//!     let db_path = dir.path().join("demo.db");
//!
//!     let config = EngineConfig { sync_on_write: true, ..Default::default() };
//!     let mut engine = Engine::with_config(db_path.clone(), config)?;
//!
//!     engine.set(b"foo", b"bar".to_vec())?;
//!     assert_eq!(engine.get(b"foo").map(|a| a.as_ref().to_vec()), Some(b"bar".to_vec()));
//!
//!     let entries = vec![
//!         (b"a".to_vec(), Some(b"1".to_vec())),
//!         (b"b".to_vec(), Some(b"2".to_vec())),
//!     ]
//!     .into_iter()
//!     .map(|(k, v)| Entry::new(k, v))
//!     .collect::<Vec<_>>();
//!     engine.batch(entries)?;
//!
//!     // Collect scan results into a Vec to get length
//!     let iter = engine.scan(b"a".to_vec()..b"z".to_vec())?;
//!     let results: Vec<_> = iter.collect();
//!     assert_eq!(results.len(), 2);
//!
//!     # Ok(())
//! # }
//! ```
mod engine;
mod error;
pub mod parser;
pub mod executor;
mod database;

pub use engine::{Engine, EngineConfig, Entry, Transaction};
pub use error::{Error, Result};
pub use executor::{Executor, ResultSet};
pub use database::{Database, QueryResult, Row, Transaction as DbTransaction};

// Keep low-level API for advanced users
pub mod low_level {
    pub use crate::engine::{Engine, Transaction as EngineTransaction};
    pub use crate::executor::{Executor, ResultSet};
    pub use crate::parser::{parse_sql, Statement, SqlValue};
}
