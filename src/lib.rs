//! Tegdb: A persistent key-value store with MVCC, GC, and log compaction.
//! 
//! # Overview
//! This crate exposes the core Engine and Transaction types.

mod engine;
pub mod wal;         // Changed from mod wal to pub mod wal
mod transaction;
mod database;
mod snapshot;
mod types;
pub mod constants;   // Changed from mod constants to pub mod constants
pub mod utils;       // Changed from mod utils to pub mod utils

pub use engine::Engine;
pub use transaction::Transaction;
pub use database::Database;
