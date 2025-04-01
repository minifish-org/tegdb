//! Tegdb: A persistent key-value store with MVCC, GC, and log compaction.
//!
//! # Overview
//! This crate exposes the core Engine and Transaction types.

pub mod constants; // Changed from mod constants to pub mod constants
mod database;
mod engine;
mod snapshot;
mod transaction;
mod types;
pub mod utils;
pub mod wal; // Changed from mod wal to pub mod wal // Changed from mod utils to pub mod utils

pub use database::Database;
pub use engine::Engine;
pub use transaction::Transaction;
