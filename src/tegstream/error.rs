use std::fmt;

#[derive(Debug)]
pub enum Error {
    Io(std::io::Error),
    S3(String),
    Parse(String),
    InvalidState(String),
    Config(String),
    Restore(String),
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Error::Io(e) => write!(f, "I/O error: {}", e),
            Error::S3(e) => write!(f, "S3 error: {}", e),
            Error::Parse(e) => write!(f, "Parse error: {}", e),
            Error::InvalidState(e) => write!(f, "Invalid state: {}", e),
            Error::Config(e) => write!(f, "Config error: {}", e),
            Error::Restore(e) => write!(f, "Restore error: {}", e),
        }
    }
}

impl std::error::Error for Error {}

impl From<std::io::Error> for Error {
    fn from(e: std::io::Error) -> Self {
        Error::Io(e)
    }
}

impl From<serde_json::Error> for Error {
    fn from(e: serde_json::Error) -> Self {
        Error::Parse(format!("JSON error: {}", e))
    }
}

pub type Result<T> = std::result::Result<T, Error>;
