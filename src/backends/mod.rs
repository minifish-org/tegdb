//! Storage backends for TegDB

pub mod file_log_backend;

pub use file_log_backend::FileLogBackend as DefaultLogBackend;
