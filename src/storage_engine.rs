// filepath: /home/runner/work/tegdb/tegdb/src/storage.rs
use std::cell::RefCell;
use std::collections::{HashMap, VecDeque};
use std::ops::Range;
use std::path::PathBuf;
use std::rc::Rc;
use std::time::{Duration, Instant};

use crate::error::{Error, Result};
use crate::log::{KeyMap, Log, LogConfig, ValuePointer, TX_COMMIT_MARKER};

pub const DEFAULT_PREALLOCATE_SIZE_BYTES: u64 = 10 * 1024 * 1024; // 10 MiB
pub const DEFAULT_PREALLOCATE_SIZE_MB: u64 = 10;
pub const DEFAULT_INITIAL_CAPACITY_KEYS: usize = 10_000;

/// Default minimum compaction threshold in bytes when no preallocation is set (10 MB)
pub const DEFAULT_COMPACTION_THRESHOLD_BYTES: u64 = 10 * 1024 * 1024;
/// Default compaction threshold ratio relative to the preallocated size (50%)
pub const DEFAULT_COMPACTION_THRESHOLD_RATIO: f64 = 0.5;
/// Stringified default compaction threshold ratio (for CLI help text)
pub const DEFAULT_COMPACTION_THRESHOLD_RATIO_STR: &str = "0.5";
/// Default compaction ratio (log size vs active data)
pub const DEFAULT_COMPACTION_RATIO: f64 = 2.0;
/// Stringified default compaction ratio
pub const DEFAULT_COMPACTION_RATIO_STR: &str = "2.0";
/// Default inline threshold for keeping hot small values resident (bytes)
pub const DEFAULT_INLINE_VALUE_THRESHOLD: usize = 64;
/// Default cache size in bytes for value/page cache
pub const DEFAULT_CACHE_SIZE_BYTES: u64 = 8 * 1024 * 1024;

/// Config options for the database engine
#[derive(Debug, Clone)]
pub struct EngineConfig {
    /// Maximum key size in bytes (default: 1KB)
    pub max_key_size: usize,
    /// Maximum value size in bytes (default: 256KB)
    pub max_value_size: usize,
    /// Whether to automatically compact during runtime when thresholds are met (default: true)
    pub auto_compact: bool,
    /// Initial capacity for BTreeMap (memory preallocation).
    /// Also acts as a hard cap on the number of resident keys.
    /// Defaults to 10 000 keys.
    pub initial_capacity: Option<usize>,
    /// Preallocate disk space in bytes.
    /// Acts as a hard cap on the WAL-backed log size. Defaults to 10 MiB.
    pub preallocate_size: Option<u64>,
    /// Ratio of preallocated log space that must be used before compaction can start
    pub compaction_threshold_ratio: f64,
    /// Ratio of log size to active data size to trigger compaction (default: 2.0)
    pub compaction_ratio: f64,
    /// Durability configuration (fsync strategy)
    pub durability: DurabilityConfig,
    /// Values up to this size are kept inline in memory for faster reads
    pub inline_value_threshold: usize,
    /// Maximum bytes used by the value cache
    pub cache_size_bytes: u64,
}

impl Default for EngineConfig {
    fn default() -> Self {
        Self {
            max_key_size: crate::log::DEFAULT_MAX_KEY_SIZE,
            max_value_size: crate::log::DEFAULT_MAX_VALUE_SIZE,
            auto_compact: true,
            initial_capacity: Some(DEFAULT_INITIAL_CAPACITY_KEYS),
            preallocate_size: Some(DEFAULT_PREALLOCATE_SIZE_BYTES),
            compaction_threshold_ratio: DEFAULT_COMPACTION_THRESHOLD_RATIO,
            compaction_ratio: DEFAULT_COMPACTION_RATIO,
            durability: DurabilityConfig::default(),
            inline_value_threshold: DEFAULT_INLINE_VALUE_THRESHOLD,
            cache_size_bytes: DEFAULT_CACHE_SIZE_BYTES,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DurabilityLevel {
    Immediate,
    GroupCommit,
}

#[derive(Debug, Clone)]
pub struct DurabilityConfig {
    pub level: DurabilityLevel,
    pub group_commit_interval: Duration,
}

impl Default for DurabilityConfig {
    fn default() -> Self {
        Self {
            level: DurabilityLevel::Immediate,
            group_commit_interval: Duration::from_millis(0),
        }
    }
}

#[derive(Default, Debug, Clone)]
pub struct StorageMetrics {
    pub bytes_written: u64,
    pub bytes_read: u64,
    pub cache_hits: u64,
    pub cache_misses: u64,
    pub fsync_count: u64,
}

struct CacheEntry {
    value: Rc<[u8]>,
    len: usize,
}

/// Simple byte-bounded LRU-ish cache for values
struct ValueCache {
    cap_bytes: u64,
    used_bytes: u64,
    map: HashMap<u64, CacheEntry>,
    order: VecDeque<u64>,
}

impl ValueCache {
    fn new(cap_bytes: u64) -> Self {
        Self {
            cap_bytes,
            used_bytes: 0,
            map: HashMap::new(),
            order: VecDeque::new(),
        }
    }

    fn get(&mut self, offset: u64) -> Option<Rc<[u8]>> {
        if let Some(value) = self.map.get(&offset).map(|entry| entry.value.clone()) {
            self.touch(offset);
            return Some(value);
        }
        None
    }

    fn insert(&mut self, offset: u64, len: usize, value: Rc<[u8]>) {
        if len as u64 > self.cap_bytes {
            return; // too large to cache
        }

        if let Some(existing) = self.map.remove(&offset) {
            self.used_bytes = self.used_bytes.saturating_sub(existing.len as u64);
        }

        self.order.push_back(offset);
        self.map.insert(
            offset,
            CacheEntry {
                value: value.clone(),
                len,
            },
        );
        self.used_bytes = self.used_bytes.saturating_add(len as u64);
        self.evict();
    }

    fn touch(&mut self, offset: u64) {
        self.order.push_back(offset);
    }

    fn evict(&mut self) {
        while self.used_bytes > self.cap_bytes {
            if let Some(oldest) = self.order.pop_front() {
                if let Some(entry) = self.map.remove(&oldest) {
                    self.used_bytes = self.used_bytes.saturating_sub(entry.len as u64);
                }
            } else {
                break;
            }
        }
    }
}

/// The main database storage engine
pub struct StorageEngine {
    log: RefCell<Log>,
    key_map: KeyMap,
    config: EngineConfig,
    identifier: String, // Store the database identifier
    active_data_size: u64,
    cache: RefCell<ValueCache>,
    last_sync: Instant,
    pending_sync: bool,
    metrics: RefCell<StorageMetrics>,
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
        // Default to .teg suffix if none provided; keep user-provided suffixes intact
        let path = ensure_teg_extension(&path)?;
        let path_str = path.to_string_lossy().to_string();
        Self::with_config_and_identifier(path_str, config)
    }

    /// Internal helper that wires a string identifier to the log backend
    pub fn with_config_and_identifier(identifier: String, config: EngineConfig) -> Result<Self> {
        let log_config = LogConfig {
            max_key_size: config.max_key_size,
            max_value_size: config.max_value_size,
            initial_capacity: config.initial_capacity,
            preallocate_size: config.preallocate_size,
            inline_value_threshold: config.inline_value_threshold,
            group_commit_interval: config.durability.group_commit_interval,
        };
        let mut log = Log::new(identifier.clone(), &log_config)?;
        let (key_map, active_data_size) = log.build_key_map(&log_config)?;

        if let Some(cap) = config.initial_capacity {
            if key_map.len() > cap {
                return Err(Error::OutOfMemoryQuota { max_keys: cap });
            }
        }

        let cache_size_bytes = config.cache_size_bytes;
        let engine = Self {
            log: RefCell::new(log),
            key_map,
            config,
            identifier,
            active_data_size,
            cache: RefCell::new(ValueCache::new(cache_size_bytes)),
            last_sync: Instant::now(),
            pending_sync: false,
            metrics: RefCell::new(StorageMetrics::default()),
        };

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
        let pointer = self.key_map.get(key)?;
        self.load_value(pointer).ok()
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

        let is_new_key = !self.key_map.contains_key(key);
        if is_new_key {
            if let Some(cap) = self.config.initial_capacity {
                if self.key_map.len() >= cap {
                    return Err(Error::OutOfMemoryQuota { max_keys: cap });
                }
            }
        }

        // Skip writing if the value hasn't changed
        if let Some(existing) = self.key_map.get(key) {
            if let Ok(existing_val) = self.load_value(existing) {
                if existing_val.as_ref() == value.as_slice() {
                    return Ok(());
                }
            }
        }

        let write_outcome = self.log.borrow_mut().write_entry(key, &value)?;
        let inline_value = if value.len() <= self.config.inline_value_threshold {
            Some(Rc::from(value.clone().into_boxed_slice()))
        } else {
            None
        };
        let pointer = ValuePointer {
            value_offset: write_outcome.value_offset,
            value_len: write_outcome.value_len,
            inline_value,
        };

        // store cached inline value if available
        if let Some(ref inline) = pointer.inline_value {
            self.cache.borrow_mut().insert(
                pointer.value_offset,
                pointer.value_len as usize,
                inline.clone(),
            );
        }

        // Update active data size
        if let Some(old_val) = self.key_map.insert(key.to_vec(), pointer) {
            self.active_data_size = self.active_data_size.saturating_sub(old_val.len() as u64);
            // Key size doesn't change
        } else {
            // New key: add key size + overhead
            self.active_data_size += key.len() as u64;
            self.active_data_size += (crate::log::LENGTH_FIELD_BYTES * 2) as u64;
        }
        // Add new value size
        self.active_data_size += write_outcome.value_len as u64;

        // Metrics
        let mut metrics = self.metrics.borrow_mut();
        metrics.bytes_written = metrics
            .bytes_written
            .saturating_add(write_outcome.entry_len as u64);
        drop(metrics);

        // Check for compaction
        self.check_compaction_trigger()?;

        Ok(())
    }

    /// Deletes a key-value pair
    pub fn del(&mut self, key: &[u8]) -> Result<()> {
        if !self.key_map.contains_key(key) {
            return Ok(());
        }

        let write_outcome = self.log.borrow_mut().write_entry(key, &[])?;

        // Update active data size
        if let Some(old_val) = self.key_map.remove(key) {
            // Subtract old value size
            self.active_data_size = self.active_data_size.saturating_sub(old_val.len() as u64);
            // Subtract key size
            self.active_data_size = self.active_data_size.saturating_sub(key.len() as u64);
            // Subtract overhead
            self.active_data_size = self
                .active_data_size
                .saturating_sub((crate::log::LENGTH_FIELD_BYTES * 2) as u64);
        }

        let mut metrics = self.metrics.borrow_mut();
        metrics.bytes_written = metrics
            .bytes_written
            .saturating_add(write_outcome.entry_len as u64);
        drop(metrics);

        // Check for compaction
        self.check_compaction_trigger()?;

        Ok(())
    }

    /// Scans a range of key-value pairs
    pub fn scan(&self, range: Range<Vec<u8>>) -> Result<ScanResult<'_>> {
        let mut items = Vec::new();
        for (key, pointer) in self.key_map.range(range) {
            let value = self.load_value(pointer)?;
            items.push((key.clone(), value));
        }
        Ok(Box::new(items.into_iter()))
    }

    /// Explicitly flushes data to disk
    pub fn flush(&mut self) -> Result<()> {
        let res = self.log.borrow_mut().sync_all();
        if res.is_ok() {
            self.pending_sync = false;
            self.last_sync = Instant::now();
            self.metrics.borrow_mut().fsync_count += 1;
        }
        res
    }

    /// Manually triggers compaction to reclaim space
    pub fn compact(&mut self) -> Result<()> {
        // Create a temporary identifier for the new log
        let tmp_identifier = format!("{}.new", self.current_identifier());

        let (mut new_log, new_key_map) = self.construct_log(tmp_identifier.clone())?;

        // Rename the new log to replace the current one
        new_log.rename_to(self.current_identifier())?;

        // Update active data size to reflect the compacted state
        // The new log size should be very close to active_data_size
        // But let's be precise and use the new log's size
        let new_size = new_log.current_size()?;

        self.log = RefCell::new(new_log);
        self.key_map = new_key_map;
        self.active_data_size = new_size;
        self.cache = RefCell::new(ValueCache::new(self.config.cache_size_bytes));

        Ok(())
    }

    /// Check if compaction should be triggered
    fn check_compaction_trigger(&mut self) -> Result<()> {
        if !self.config.auto_compact {
            return Ok(());
        }

        let log_size = self.log.borrow().current_size()?;
        let threshold_bytes = self.effective_compaction_threshold_bytes();

        // Check thresholds
        // 1. Log size must be greater than configured threshold
        if log_size <= threshold_bytes {
            return Ok(());
        }

        // 2. Log size must be significantly larger than active data size (fragmentation)
        // Avoid division by zero
        if self.active_data_size == 0 {
            // If empty but log is large, compact
            if log_size > threshold_bytes {
                return self.compact();
            }
            return Ok(());
        }

        let ratio = log_size as f64 / self.active_data_size as f64;
        if ratio > self.config.compaction_ratio {
            self.compact()?;
        }

        Ok(())
    }

    fn effective_compaction_threshold_bytes(&self) -> u64 {
        if let Some(prealloc) = self.config.preallocate_size {
            let ratio = if self.config.compaction_threshold_ratio <= 0.0 {
                DEFAULT_COMPACTION_THRESHOLD_RATIO
            } else {
                self.config.compaction_threshold_ratio
            };
            let computed = (prealloc as f64 * ratio).round() as u64;
            return computed.max(1);
        }
        DEFAULT_COMPACTION_THRESHOLD_BYTES
    }

    fn load_value(&self, pointer: &ValuePointer) -> Result<Rc<[u8]>> {
        if let Some(inline) = &pointer.inline_value {
            return Ok(inline.clone());
        }

        if pointer.value_len == 0 {
            return Ok(Rc::from(Vec::<u8>::new().into_boxed_slice()));
        }

        {
            let mut cache = self.cache.borrow_mut();
            if let Some(value) = cache.get(pointer.value_offset) {
                self.metrics.borrow_mut().cache_hits += 1;
                return Ok(value);
            }
            self.metrics.borrow_mut().cache_misses += 1;
        }

        let data = self
            .log
            .borrow_mut()
            .read_value(pointer.value_offset, pointer.value_len)?;
        let rc: Rc<[u8]> = Rc::from(data.into_boxed_slice());
        {
            let mut cache = self.cache.borrow_mut();
            cache.insert(pointer.value_offset, pointer.value_len as usize, rc.clone());
        }
        let mut metrics = self.metrics.borrow_mut();
        metrics.bytes_read = metrics.bytes_read.saturating_add(pointer.value_len as u64);

        Ok(rc)
    }

    fn sync_on_commit(&mut self) -> Result<()> {
        match self.config.durability.level {
            DurabilityLevel::Immediate => {
                self.log.borrow_mut().sync_all()?;
                self.metrics.borrow_mut().fsync_count += 1;
                self.pending_sync = false;
                self.last_sync = Instant::now();
            }
            DurabilityLevel::GroupCommit => {
                let interval = self.config.durability.group_commit_interval;
                self.pending_sync = true;
                if interval.is_zero() || self.last_sync.elapsed() >= interval {
                    self.log.borrow_mut().sync_all()?;
                    self.metrics.borrow_mut().fsync_count += 1;
                    self.pending_sync = false;
                    self.last_sync = Instant::now();
                } else {
                    self.pending_sync = true;
                }
            }
        }
        Ok(())
    }

    pub fn metrics(&self) -> StorageMetrics {
        self.metrics.borrow().clone()
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
            initial_capacity: self.config.initial_capacity,
            preallocate_size: self.config.preallocate_size,
            inline_value_threshold: self.config.inline_value_threshold,
            group_commit_interval: self.config.durability.group_commit_interval,
        };
        let mut new_log = Log::new(identifier, &log_config)?;
        // New logs now include a header; ensure we don't truncate it away
        // We'll rely on backend initialization to have written the header,
        // so do not reset to 0 here.
        for (key, pointer) in &self.key_map {
            let value = self.load_value(pointer)?;
            let outcome = new_log.write_entry(key, value.as_ref())?;
            let inline_value = if value.len() <= self.config.inline_value_threshold {
                Some(value)
            } else {
                None
            };
            new_key_map.insert(
                key.clone(),
                ValuePointer {
                    value_offset: outcome.value_offset,
                    value_len: outcome.value_len,
                    inline_value,
                },
            );
        }

        Ok((new_log, new_key_map))
    }
}

/// Ensure the database file has a `.teg` suffix when creating/opening by path.
/// - If the path has no extension, append `.teg`.
/// - If the path points to an existing directory, return an error.
fn ensure_teg_extension(path: &std::path::Path) -> Result<std::path::PathBuf> {
    // Basic sanity: path must not be a directory if it already exists
    if let Ok(meta) = std::fs::metadata(path) {
        if meta.is_dir() {
            return Err(crate::error::Error::Other(format!(
                "Path points to a directory, expected file: {}",
                path.display()
            )));
        }
    }

    // If there's no extension, default to .teg
    if path.extension().is_none() {
        let mut with_ext = path.to_path_buf();
        with_ext.set_extension("teg");
        Ok(with_ext)
    } else {
        // Enforce .teg-only files; reject any other extension
        if path.extension().and_then(|s| s.to_str()) != Some("teg") {
            return Err(crate::error::Error::Other(format!(
                "Unsupported database file extension. Expected '.teg': {}",
                path.display()
            )));
        }
        Ok(path.to_path_buf())
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
        let old_value = self.engine.get(key);

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
            if let Ok(existing_val) = self.engine.load_value(existing) {
                if existing_val.as_ref() == value.as_slice() {
                    return Ok(());
                }
            }
        }

        // Record undo information only when we're about to make a real change
        self.record_undo(key);

        // Write-through: directly modify engine state
        let result = self.engine.set(key, value);
        if result.is_err() {
            if let Some(ref mut log) = self.undo_log {
                log.pop();
            }
        }
        result
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
        let result = self.engine.del(key);
        if result.is_err() {
            if let Some(ref mut log) = self.undo_log {
                log.pop();
            }
        }
        result
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
            let outcome = self
                .engine
                .log
                .borrow_mut()
                .write_entry(TX_COMMIT_MARKER, &[])?;
            self.engine.sync_on_commit()?;
            {
                let mut metrics = self.engine.metrics.borrow_mut();
                metrics.bytes_written = metrics
                    .bytes_written
                    .saturating_add(outcome.entry_len as u64);
            }

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
