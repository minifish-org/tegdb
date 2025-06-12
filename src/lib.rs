//! # Examples
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
pub mod sql;
pub mod executor;

pub use engine::{Engine, EngineConfig, Entry, Transaction};
pub use error::{Error, Result};
pub use executor::{Executor, ResultSet};
