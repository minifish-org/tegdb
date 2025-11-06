use std::collections::BTreeMap;
use std::rc::Rc;

use crate::backends::DefaultLogBackend;
use crate::error::Result;

/// KeyMap maps keys to shared buffers instead of owned Vecs
pub type KeyMap = BTreeMap<Vec<u8>, Rc<[u8]>>;

/// Transaction commit marker - special marker that won't be part of keymap
pub const TX_COMMIT_MARKER: &[u8] = b"__TX_COMMIT__";

/// Default size limits for keys and values
pub const DEFAULT_MAX_KEY_SIZE: usize = 1024; // 1KB
pub const DEFAULT_MAX_VALUE_SIZE: usize = 256 * 1024; // 256KB
/// Byte width of the length fields in the log format
pub const LENGTH_FIELD_BYTES: usize = 4;

/// Storage file magic header and format
pub const STORAGE_MAGIC: &[u8; 6] = b"TEGDB\0"; // 6 bytes
pub const STORAGE_FORMAT_VERSION: u16 = 2; // big-endian on disk
/// Total header size in bytes (fixed)
pub const STORAGE_HEADER_SIZE: usize = 64; // leave room for future fields

/// In-file header layout (explicit read/write, not repr(C)):
/// [0..6)   magic:    b"TEGDB\0"
/// [6..8)   version:  u16 BE
/// [8..12)  flags:    u32 BE (unused = 0)
/// [12..16) max_key:  u32 BE
/// [16..20) max_val:  u32 BE
/// [20..21) endian:   u8 (1=BE, 2=LE; we write 1)
/// [21..29) valid_data_end: u64 BE (tracks actual data boundary)
/// [29..64) reserved: zero padding
/// Config options for the log
#[derive(Debug, Clone)]
pub struct LogConfig {
    /// Maximum key size in bytes
    pub max_key_size: usize,
    /// Maximum value size in bytes
    pub max_value_size: usize,
    /// Initial capacity for BTreeMap (memory preallocation)
    pub initial_capacity: Option<usize>,
    /// Preallocate disk space in bytes
    pub preallocate_size: Option<u64>,
}

/// Trait for different log storage backends (file-based).
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
