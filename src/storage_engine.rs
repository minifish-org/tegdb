// filepath: /home/runner/work/tegdb/tegdb/src/storage.rs
use std::ops::Range;
use std::path::PathBuf;

use crate::error::{Error, Result};
use crate::log::{KeyMap, Log, LogConfig, TX_COMMIT_MARKER};

use std::rc::Rc;

/// Config options for the database engine
#[derive(Debug, Clone)]
pub struct EngineConfig {
    /// Maximum key size in bytes (default: 1KB)
    pub max_key_size: usize,
    /// Maximum value size in bytes (default: 256KB)
    pub max_value_size: usize,
    /// Whether to automatically compact on open (default: true)
    pub auto_compact: bool,
}

impl Default for EngineConfig {
    fn default() -> Self {
        Self {
            max_key_size: crate::log::DEFAULT_MAX_KEY_SIZE,
            max_value_size: crate::log::DEFAULT_MAX_VALUE_SIZE,
            auto_compact: true,
        }
    }
}

/// The main database storage engine
pub struct StorageEngine {
    log: Log,
    key_map: KeyMap,
    config: EngineConfig,
    identifier: String, // Store the database identifier
}

// Type alias for scan result (returns keys and shared buffer Rcs for values)
type ScanResult<'a> = Box<dyn Iterator<Item = (Vec<u8>, Rc<[u8]>)> + 'a>;

impl StorageEngine {
    /// Creates a new database engine with default configuration
    pub fn new(path: PathBuf) -> Result<Self> {
        Self::with_config(path, EngineConfig::default())
    }

    /// Creates a new database engine with custom configuration
    pub fn with_config(path: PathBuf, config: EngineConfig) -> Result<Self> {
        let path_str = path.to_string_lossy().to_string();
        Self::with_config_and_identifier(path_str, config)
    }

    /// Internal helper that wires a string identifier to the log backend
    pub fn with_config_and_identifier(identifier: String, config: EngineConfig) -> Result<Self> {
        let log_config = LogConfig {
            max_key_size: config.max_key_size,
            max_value_size: config.max_value_size,
        };
        let mut log = Log::new(identifier.clone(), &log_config)?;
        let key_map = log.build_key_map(&log_config)?;

        let mut engine = Self {
            log,
            key_map,
            config,
            identifier,
        };

        if engine.config.auto_compact {
            engine.compact()?;
        }

        Ok(engine)
    }

    /// Begins a new write-through transaction
    pub fn begin_transaction(&mut self) -> Transaction<'_> {
        // Don't write begin marker - only write commit marker on commit
        Transaction {
            engine: self,
            undo_log: None, // Lazy initialization
            finalized: false,
        }
    }

    /// Retrieves a value by key (zero-copy refcounted Rc)
    pub fn get(&self, key: &[u8]) -> Option<Rc<[u8]>> {
        self.key_map.get(key).cloned()
    }

    /// Sets a key-value pair
    pub fn set(&mut self, key: &[u8], value: Vec<u8>) -> Result<()> {
        // Validate input sizes
        if key.len() > self.config.max_key_size {
            return Err(Error::KeyTooLarge(key.len()));
        }
        if value.len() > self.config.max_value_size {
            return Err(Error::ValueTooLarge(value.len()));
        }

        if value.is_empty() {
            return self.del(key);
        }

        // Skip writing if the value hasn't changed
        if let Some(existing) = self.key_map.get(key) {
            if existing.as_ref() == value.as_slice() {
                return Ok(());
            }
        }

        // write to log, then store shared buffer
        self.log.write_entry(key, &value)?;
        // store as shared buffer for cheap cloning on get
        let shared = Rc::from(value.into_boxed_slice());
        self.key_map.insert(key.to_vec(), shared);

        Ok(())
    }

    /// Deletes a key-value pair
    pub fn del(&mut self, key: &[u8]) -> Result<()> {
        if !self.key_map.contains_key(key) {
            return Ok(());
        }

        self.log.write_entry(key, &[])?;
        self.key_map.remove(key);

        Ok(())
    }

    /// Scans a range of key-value pairs
    pub fn scan(&self, range: Range<Vec<u8>>) -> Result<ScanResult<'_>> {
        let iter = self
            .key_map
            .range(range)
            // Minimize cloning - only clone key Vec (small) and increment Rc refcount (cheap)
            .map(|(key, value)| (key.clone(), Rc::clone(value)));
        Ok(Box::new(iter))
    }

    /// Explicitly flushes data to disk
    pub fn flush(&mut self) -> Result<()> {
        self.log.sync_all()
    }

    /// Manually triggers compaction to reclaim space
    pub fn compact(&mut self) -> Result<()> {
        // Create a temporary identifier for the new log
        let tmp_identifier = format!("{}.new", self.current_identifier());

        let (mut new_log, new_key_map) = self.construct_log(tmp_identifier.clone())?;

        // Rename the new log to replace the current one
        new_log.rename_to(self.current_identifier())?;

        self.log = new_log;
        self.key_map = new_key_map;

        Ok(())
    }

    /// Get the current log identifier
    fn current_identifier(&self) -> String {
        self.identifier.clone()
    }

    /// Returns the number of key-value pairs in the database
    pub fn len(&self) -> usize {
        self.key_map.len()
    }

    /// Returns true if the database is empty
    pub fn is_empty(&self) -> bool {
        self.key_map.is_empty()
    }

    /// Constructs a new log file with only current key-value pairs
    fn construct_log(&mut self, identifier: String) -> Result<(Log, KeyMap)> {
        let mut new_key_map = KeyMap::new();
        let log_config = LogConfig {
            max_key_size: self.config.max_key_size,
            max_value_size: self.config.max_value_size,
        };
        let mut new_log = Log::new(identifier, &log_config)?;
        // New logs now include a header; ensure we don't truncate it away
        // We'll rely on backend initialization to have written the header,
        // so do not reset to 0 here.
        for (key, value) in &self.key_map {
            new_log.write_entry(key, value.as_ref())?;
            new_key_map.insert(key.clone(), value.clone());
        }

        Ok((new_log, new_key_map))
    }
}

impl Drop for StorageEngine {
    fn drop(&mut self) {
        // Ignore errors during drop, but try to flush
        let _ = self.flush();
    }
}

/// Undo log entry for rollback
struct UndoEntry {
    key: Vec<u8>,
    old_value: Option<Rc<[u8]>>, // None means key didn't exist
}

/// Write-through transactional context for ACID operations
pub struct Transaction<'a> {
    engine: &'a mut StorageEngine,
    undo_log: Option<Vec<UndoEntry>>, // Lazy initialization
    finalized: bool,                  // Track if transaction has been committed or rolled back
}

impl Transaction<'_> {
    /// Records the current state for potential rollback and returns the old value
    fn record_undo(&mut self, key: &[u8]) -> Option<Rc<[u8]>> {
        let old_value = self.engine.key_map.get(key).cloned();

        // Lazy initialization of undo_log
        if self.undo_log.is_none() {
            self.undo_log = Some(Vec::new());
        }

        self.undo_log.as_mut().unwrap().push(UndoEntry {
            key: key.to_vec(),
            old_value: old_value.clone(),
        });
        old_value
    }

    /// Sets a key-value pair directly in the engine with undo logging
    pub fn set(&mut self, key: &[u8], value: Vec<u8>) -> Result<()> {
        // Validate input sizes
        if key.len() > self.engine.config.max_key_size {
            return Err(Error::KeyTooLarge(key.len()));
        }
        if value.len() > self.engine.config.max_value_size {
            return Err(Error::ValueTooLarge(value.len()));
        }

        // Check if the value would actually change (same logic as engine.set())
        if value.is_empty() {
            return self.delete(key);
        }

        // Check if value hasn't changed - if so, no undo recording needed
        if let Some(existing) = self.engine.key_map.get(key) {
            if existing.as_ref() == value.as_slice() {
                return Ok(());
            }
        }

        // Record undo information only when we're about to make a real change
        self.record_undo(key);

        // Write-through: directly modify engine state
        self.engine.set(key, value)?;

        Ok(())
    }

    /// Deletes a key directly in the engine with undo logging
    pub fn delete(&mut self, key: &[u8]) -> Result<()> {
        // Check if key exists - if not, no undo recording needed (same logic as engine.del())
        if !self.engine.key_map.contains_key(key) {
            return Ok(());
        }

        // Record undo information only when we're about to make a real change
        self.record_undo(key);

        // Write-through: directly modify engine state
        self.engine.del(key)?;

        Ok(())
    }

    /// Retrieves a value directly from the engine (no transaction-local state)
    pub fn get(&self, key: &[u8]) -> Option<Rc<[u8]>> {
        self.engine.get(key)
    }

    /// Scans a range directly from the engine (no transaction-local state)
    pub fn scan(&self, range: Range<Vec<u8>>) -> Result<ScanResult<'_>> {
        self.engine.scan(range)
    }

    /// Returns true if the transaction has pending operations (i.e., uncommitted changes)
    pub fn has_pending_operations(&self) -> bool {
        !self.finalized && self.undo_log.as_ref().is_some_and(|log| !log.is_empty())
    }

    /// Returns true if the transaction is clean (no pending operations)
    pub fn is_clean(&self) -> bool {
        self.finalized || self.undo_log.as_ref().is_none_or(|log| log.is_empty())
    }

    /// Returns true if the transaction has been finalized (committed or rolled back)
    pub fn is_finalized(&self) -> bool {
        self.finalized
    }

    /// Commits the transaction by writing commit marker
    pub fn commit(&mut self) -> Result<()> {
        if self.finalized {
            return Err(Error::Other("Transaction already finalized".to_string()));
        }

        // Check if this is a read-only transaction (no write operations)
        let has_writes = self.undo_log.as_ref().is_some_and(|log| !log.is_empty());

        if has_writes {
            // Write transaction commit marker directly to log (not to keymap) and always sync on commit
            self.engine.log.write_entry(TX_COMMIT_MARKER, &[])?;

            // Clear the undo log
            if let Some(ref mut log) = self.undo_log {
                log.clear();
            }
        }
        // For read-only transactions, no commit marker or sync needed

        self.finalized = true;
        Ok(())
    }

    /// Rolls back the transaction by restoring original values
    pub fn rollback(&mut self) -> Result<()> {
        if self.finalized {
            return Err(Error::Other("Transaction already finalized".to_string()));
        }

        // Check if there's anything to rollback
        let has_operations = self.undo_log.as_ref().is_some_and(|log| !log.is_empty());
        if !has_operations {
            // No operations performed, nothing to rollback
            self.finalized = true;
            return Ok(());
        }

        // Restore original values in reverse order using engine's set/del methods
        if let Some(ref mut log) = self.undo_log {
            for undo_entry in log.drain(..).rev() {
                if let Some(old_value) = undo_entry.old_value {
                    // Restore the old value using engine's set method
                    self.engine.set(&undo_entry.key, old_value.to_vec())?;
                } else {
                    // Key didn't exist, remove it using engine's del method
                    self.engine.del(&undo_entry.key)?;
                }
            }
        }

        // undo_log is now empty, transaction is rolled back
        self.finalized = true;
        Ok(())
    }
}

impl Drop for Transaction<'_> {
    fn drop(&mut self) {
        // Automatically rollback if transaction has uncommitted operations
        if !self.finalized {
            let _ = self.rollback(); // Ignore errors during drop
        }
    }
}
