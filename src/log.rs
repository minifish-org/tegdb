use std::collections::BTreeMap;
use std::sync::Arc;

use crate::backends::DefaultLogBackend;
use crate::error::Result;

/// KeyMap maps keys to shared buffers instead of owned Vecs
pub type KeyMap = BTreeMap<Vec<u8>, Arc<[u8]>>;

/// Transaction commit marker - special marker that won't be part of keymap
pub const TX_COMMIT_MARKER: &[u8] = b"__TX_COMMIT__";

/// Config options for the log
#[derive(Debug, Clone)]
pub struct LogConfig {
    /// Maximum key size in bytes
    pub max_key_size: usize,
    /// Maximum value size in bytes
    pub max_value_size: usize,
}

/// Trait for different log storage backends (file, browser, etc.)
pub trait LogBackend {
    /// Initialize storage with the given identifier and configuration
    fn new(identifier: String, config: &LogConfig) -> Result<Self>
    where
        Self: Sized;

    /// Build key map from stored data (load existing database)
    fn build_key_map(&mut self, config: &LogConfig) -> Result<KeyMap>;

    /// Write an entry to storage
    fn write_entry(&mut self, key: &[u8], value: &[u8]) -> Result<()>;

    /// Sync/flush data to persistent storage
    fn sync_all(&mut self) -> Result<()>;

    /// Truncate/clear storage to specified size
    fn set_len(&mut self, size: u64) -> Result<()>;

    /// Rename/move storage to new identifier
    fn rename_to(&mut self, new_identifier: String) -> Result<()>;
}

/// Universal log structure that works with different storage backends
pub struct Log {
    backend: DefaultLogBackend,
}

impl Log {
    pub fn new(identifier: String, config: &LogConfig) -> Result<Self> {
        let backend = DefaultLogBackend::new(identifier, config)?;
        Ok(Self { backend })
    }

    pub fn build_key_map(&mut self, config: &LogConfig) -> Result<KeyMap> {
        self.backend.build_key_map(config)
    }

    pub fn write_entry(&mut self, key: &[u8], value: &[u8]) -> Result<()> {
        self.backend.write_entry(key, value)
    }

    pub fn sync_all(&mut self) -> Result<()> {
        self.backend.sync_all()
    }

    pub fn set_len(&mut self, size: u64) -> Result<()> {
        self.backend.set_len(size)
    }

    /// Atomically rename this log to a new identifier
    pub fn rename_to(&mut self, new_identifier: String) -> Result<()> {
        self.backend.rename_to(new_identifier)
    }
}
