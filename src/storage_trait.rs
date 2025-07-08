use crate::error::Result;
use crate::log::{KeyMap, LogConfig};

/// Trait for different storage backends (file, browser, etc.)
pub trait StorageBackend {
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
