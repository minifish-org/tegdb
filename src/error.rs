use std::fmt;
use std::io;

/// Custom error type for tegdb operations
#[derive(Debug)]
pub enum Error {
    /// I/O error from underlying file operations
    Io(io::Error),
    /// Error when key is too large (> 1KB)
    KeyTooLarge(usize),
    /// Error when value is too large (> 256KB)
    ValueTooLarge(usize),
    /// Error when database file is locked by another process
    FileLocked(String),
    /// Error when file is corrupted
    Corrupted(String),
    /// SQL parsing, planning, or execution error
    SqlError(String),
    /// Error during SQL parsing
    ParseError(String),
    /// Error during query planning
    PlanError(String),
    /// Table not found
    TableNotFound(String),
    /// Column not found
    ColumnNotFound(String),
    /// Other database errors
    Other(String),
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Error::Io(err) => write!(f, "I/O error: {err}"),
            Error::KeyTooLarge(size) => write!(f, "Key too large: {size} bytes (max 1KB)"),
            Error::ValueTooLarge(size) => write!(f, "Value too large: {size} bytes (max 256KB)"),
            Error::FileLocked(msg) => write!(f, "Database file is locked: {msg}"),
            Error::Corrupted(msg) => write!(f, "Database corrupted: {msg}"),
            Error::SqlError(msg) => write!(f, "SQL error: {msg}"),
            Error::ParseError(msg) => write!(f, "SQL parse error: {msg}"),
            Error::PlanError(msg) => write!(f, "Query planning error: {msg}"),
            Error::TableNotFound(table) => write!(f, "Table '{table}' not found"),
            Error::ColumnNotFound(column) => write!(f, "Column '{column}' not found"),
            Error::Other(msg) => write!(f, "Database error: {msg}"),
        }
    }
}

impl std::error::Error for Error {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Error::Io(err) => Some(err),
            _ => None,
        }
    }
}

impl From<io::Error> for Error {
    fn from(err: io::Error) -> Self {
        Error::Io(err)
    }
}

/// Result type for tegdb operations
pub type Result<T> = std::result::Result<T, Error>;
