//! Tegdb: A persistent key-value store with MVCC, GC, and log compaction.
//! 
//! # Overview
//! This crate exposes the core Engine and Transaction types.

mod engine;
mod wal;         // Renamed from log to wal
mod transaction;
mod database;
mod snapshot;
mod types;
mod constants;
mod logger;
mod utils;

pub use engine::Engine;
pub use transaction::Transaction;
pub use database::Database;
