// filepath: /Users/yusp/work/tegdb/src/engine.rs
use std::io::{self, BufReader, BufWriter, Read, Seek, SeekFrom, Write};
use std::ops::Range;
use std::path::PathBuf;
use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;
use fs2::FileExt;  // For file locking

use crate::error::{Error, Result};

/// Config options for the database engine
#[derive(Debug, Clone)]
pub struct EngineConfig {
    /// Maximum key size in bytes (default: 1KB)
    pub max_key_size: usize,
    /// Maximum value size in bytes (default: 256KB)
    pub max_value_size: usize,
    /// Whether to sync to disk after every write (default: false)
    pub sync_on_write: bool,
    /// Whether to automatically compact on open (default: true)
    pub auto_compact: bool,
}

impl Default for EngineConfig {
    fn default() -> Self {
        Self {
            max_key_size: 1024,
            max_value_size: 256 * 1024,
            sync_on_write: false,
            auto_compact: true,
        }
    }
}

/// Entry type for batch operations
pub struct Entry {
    pub key: Vec<u8>,
    pub value: Option<Vec<u8>>,
}

impl Entry {
    pub fn new(key: Vec<u8>, value: Option<Vec<u8>>) -> Self {
        Self { key, value }
    }
}

/// The main database engine
pub struct Engine {
    log: Log,
    key_map: KeyMap,
    config: EngineConfig,
}

// KeyMap maps keys to shared buffers instead of owned Vecs
type KeyMap = BTreeMap<Vec<u8>, Arc<[u8]>>;
// Type alias for scan result (returns keys and shared buffer Arcs for values)
type ScanResult<'a> = Box<dyn Iterator<Item = (Vec<u8>, Arc<[u8]>)> + 'a>;

impl Engine {
    /// Creates a new database engine with default configuration
    pub fn new(path: PathBuf) -> Result<Self> {
        Self::with_config(path, EngineConfig::default())
    }

    /// Creates a new database engine with custom configuration
    pub fn with_config(path: PathBuf, config: EngineConfig) -> Result<Self> {
        let mut log = Log::new(path)?;
        let key_map = log.build_key_map()?;
        
        let mut engine = Self {
            log,
            key_map,
            config,
        };
        
        if engine.config.auto_compact {
            engine.compact()?;
        }
        
        Ok(engine)
    }

    /// Begins a new transaction
    pub fn begin_transaction(&mut self) -> Transaction<'_> {
        let entries = Vec::new();
        let pending_changes = None; // Start without HashMap for better performance
        Transaction { engine: self, entries, pending_changes, state: TxState::Active }
    }

    /// Retrieves a value by key (zero-copy refcounted Arc)
    pub fn get(&self, key: &[u8]) -> Option<Arc<[u8]>> {
        self.key_map.get(key).cloned()
    }

    /// Sets a key-value pair
    pub fn set(&mut self, key: &[u8], value: Vec<u8>) -> Result<()> {
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
        self.log.write_entry(key, &value, self.config.sync_on_write)?;
        // store as shared buffer for cheap cloning on get
        let shared = Arc::from(value.into_boxed_slice());
        self.key_map.insert(key.to_vec(), shared);
        
        Ok(())
    }

    /// Deletes a key-value pair
    pub fn del(&mut self, key: &[u8]) -> Result<()> {
        if !self.key_map.contains_key(key) {
            return Ok(());
        }

        self.log.write_entry(key, &[], self.config.sync_on_write)?;
        self.key_map.remove(key);
        
        Ok(())
    }

    /// Scans a range of key-value pairs
    pub fn scan(
        &self,
        range: Range<Vec<u8>>,
    ) -> Result<ScanResult<'_>> {
        let iter = self.key_map.range(range)
            // clone key Vec (small) and clone Arc (cheap refcount increment)
            .map(|(key, value)| (key.clone(), Arc::clone(value)));
        Ok(Box::new(iter))
    }

    /// Performs multiple operations in a batch
    pub fn batch(&mut self, entries: Vec<Entry>) -> Result<()> {
        // Fast path for empty batch
        if entries.is_empty() {
            return Ok(());
        }
        
        // Pre-validate all entries for size limits to ensure atomicity
        for entry in &entries {
            if entry.key.len() > self.config.max_key_size {
                return Err(Error::KeyTooLarge(entry.key.len()));
            }
            if let Some(ref value) = entry.value {
                if value.len() > self.config.max_value_size {
                    return Err(Error::ValueTooLarge(value.len()));
                }
            }
        }
        
        // Execute all operations
        for entry in entries {
            match entry.value {
                Some(value) => self.set(&entry.key, value)?,
                None => self.del(&entry.key)?,
            }
        }
        
        // Force a sync if batch operations
        if !self.config.sync_on_write {
            self.flush()?;
        }
        
        Ok(())
    }

    /// Explicitly flushes data to disk
    pub fn flush(&mut self) -> Result<()> {
        self.log.file.sync_all()?;
        Ok(())
    }

    /// Manually triggers compaction to reclaim space
    pub fn compact(&mut self) -> Result<()> {
        let mut tmp_path = self.log.path.clone();
        tmp_path.set_extension("new");
        
        let (mut new_log, new_key_map) = self.construct_log(tmp_path)?;

        std::fs::rename(&new_log.path, &self.log.path)?;
        new_log.path = self.log.path.clone();

        self.log = new_log;
        self.key_map = new_key_map;
        
        Ok(())
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
    fn construct_log(&mut self, path: PathBuf) -> Result<(Log, KeyMap)> {
        let mut new_key_map = KeyMap::new();
        let mut new_log = Log::new(path)?;
        new_log.file.set_len(0)?;
        for (key, value) in &self.key_map {
            new_log.write_entry(key, value.as_ref(), true)?;
            new_key_map.insert(key.clone(), value.clone());
        }
         
         Ok((new_log, new_key_map))
    }
}

impl Drop for Engine {
    fn drop(&mut self) {
        // Ignore errors during drop, but try to flush
        let _ = self.flush();
    }
}

/// Transaction state
enum TxState {
    Active,
    Committed,
    RolledBack,
}

/// Transactional context for multi-key ACID operations
pub struct Transaction<'a> {
    engine: &'a mut Engine,
    entries: Vec<Entry>,
    // Fast lookup cache for pending changes - only populated when beneficial
    pending_changes: Option<HashMap<Vec<u8>, usize>>,
    state: TxState,
}

impl Transaction<'_> {
    /// Threshold for when to build the pending changes cache
    const CACHE_THRESHOLD: usize = 10;
    
    /// Ensures pending_changes HashMap is built if we have enough entries
    fn ensure_cache_if_beneficial(&mut self) {
        if self.pending_changes.is_none() && self.entries.len() >= Self::CACHE_THRESHOLD {
            let mut cache = HashMap::with_capacity(self.entries.len());
            for (index, entry) in self.entries.iter().enumerate() {
                cache.insert(entry.key.clone(), index);
            }
            self.pending_changes = Some(cache);
        }
    }
    
    /// Inserts or updates a key-value pair in the transaction
    pub fn set(&mut self, key: Vec<u8>, value: Vec<u8>) -> Result<()> {
        // Validate input sizes
        if key.len() > self.engine.config.max_key_size {
            return Err(Error::KeyTooLarge(key.len()));
        }
        if value.len() > self.engine.config.max_value_size {
            return Err(Error::ValueTooLarge(value.len()));
        }
        
        let index = self.entries.len();
        
        // Update cache if it exists
        if let Some(ref mut cache) = self.pending_changes {
            cache.insert(key.clone(), index);
        }
        
        self.entries.push(Entry::new(key, Some(value)));
        
        // Check if we should build cache for future operations
        self.ensure_cache_if_beneficial();
        Ok(())
    }

    /// Deletes a key in the transaction
    pub fn delete(&mut self, key: Vec<u8>) -> Result<()> {
        let index = self.entries.len();
        
        // Update cache if it exists
        if let Some(ref mut cache) = self.pending_changes {
            cache.insert(key.clone(), index);
        }
        
        self.entries.push(Entry::new(key, None));
        
        // Check if we should build cache for future operations
        self.ensure_cache_if_beneficial();
        Ok(())
    }

    /// Retrieves a value within the transaction (engine state + local changes)
    pub fn get(&self, key: &[u8]) -> Option<Vec<u8>> {
        // Use cache if available for fast O(1) lookup
        if let Some(ref cache) = self.pending_changes {
            if let Some(&index) = cache.get(key) {
                if let Some(entry) = self.entries.get(index) {
                    return entry.value.clone();
                }
            }
        } else {
            // Fallback to linear search for small transactions
            for entry in self.entries.iter().rev() {
                if entry.key == key {
                    return entry.value.clone();
                }
            }
        }
        // Fallback to engine's current state
        self.engine.key_map.get(key).map(|arc| arc.as_ref().to_vec())
    }

    /// Scans a range of key-value pairs in the transaction context
    pub fn scan(&self, range: Range<Vec<u8>>) -> Vec<(Vec<u8>, Vec<u8>)> {
        // Always build cache for scanning since it's more efficient than cloning
        let cache = if let Some(ref existing_cache) = self.pending_changes {
            existing_cache
        } else {
            // Build temporary cache for this scan
            let mut temp_cache = HashMap::with_capacity(self.entries.len());
            for (index, entry) in self.entries.iter().enumerate() {
                temp_cache.insert(entry.key.clone(), index);
            }
            // We can't update self.pending_changes here since we're in a &self method
            // So we'll use the temporary cache
            return self.scan_with_cache(&temp_cache, range);
        };
        
        self.scan_with_cache(cache, range)
    }
    
    /// Helper method for scanning with a given cache
    fn scan_with_cache(&self, cache: &HashMap<Vec<u8>, usize>, range: Range<Vec<u8>>) -> Vec<(Vec<u8>, Vec<u8>)> {
        let mut result = Vec::new();
        
        // Step 1: Get base data from engine within range
        for (key, value) in self.engine.key_map.range(range.clone()) {
            // Check if this key is overridden in pending changes
            if let Some(&index) = cache.get(key) {
                if let Some(entry) = self.entries.get(index) {
                    // Only include if not deleted (value is Some)
                    if let Some(ref pending_value) = entry.value {
                        result.push((key.clone(), pending_value.clone()));
                    }
                    // If deleted (value is None), skip this key
                }
            } else {
                // No pending change, use engine value
                result.push((key.clone(), value.as_ref().to_vec()));
            }
        }
        
        // Step 2: Add new keys from pending changes that fall within range
        for (key, &index) in cache {
            if key >= &range.start && key < &range.end {
                // Only add if this key wasn't already in the engine's key_map
                if !self.engine.key_map.contains_key(key) {
                    if let Some(entry) = self.entries.get(index) {
                        if let Some(ref value) = entry.value {
                            result.push((key.clone(), value.clone()));
                        }
                    }
                }
            }
        }
        
        // Step 3: Sort the result to maintain order (since we might have added out-of-order)
        result.sort_by(|a, b| a.0.cmp(&b.0));
        result
    }

    /// Commits the transaction atomically
    pub fn commit(&mut self) -> Result<()> {
        match self.state {
            TxState::Active => {
                // Fast path for empty transactions
                if self.entries.is_empty() {
                    self.state = TxState::Committed;
                    return Ok(());
                }
                
                let entries = std::mem::take(&mut self.entries);
                self.pending_changes = None; // Clear the cache after taking entries
                let res = self.engine.batch(entries);
                if res.is_ok() {
                    self.state = TxState::Committed;
                }
                res
            }
            _ => Err(Error::Other("Transaction already finalized".to_string())),
        }
    }

    /// Rolls back the transaction
    pub fn rollback(&mut self) {
        // Fast rollback: just mark as rolled back, clear all changes
        self.state = TxState::RolledBack;
        self.entries.clear();
        self.pending_changes = None;
    }
}

impl Drop for Transaction<'_> {
    fn drop(&mut self) {
        // Automatically rollback if transaction is still active
        if matches!(self.state, TxState::Active) {
            self.state = TxState::RolledBack;
            self.entries.clear();
            self.pending_changes = None;
        }
    }
}

/// Internal log file structure
struct Log {
    path: PathBuf,
    file: std::fs::File,
}

impl Log {
    fn new(path: PathBuf) -> Result<Self> {
        // Create directory if it doesn't exist
        if let Some(dir) = path.parent() {
            std::fs::create_dir_all(dir).map_err(Error::from)?;
        }

        let file = std::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false) // Don't truncate existing database files
            .open(&path)
            .map_err(Error::from)?;
        
        // Try to obtain an exclusive lock
        file.try_lock_exclusive()
            .map_err(|e| Error::FileLocked(e.to_string()))?;
        
        Ok(Self { path, file })
    }

    fn build_key_map(&mut self) -> Result<KeyMap> {
         let mut len_buf = [0u8; 4];
         let mut key_map = KeyMap::new();
         let file_len = self.file.metadata()?.len();
         let mut reader = BufReader::new(&mut self.file);
         let mut pos = reader.seek(SeekFrom::Start(0))?;

         while pos < file_len {
            // Read key length
            match reader.read_exact(&mut len_buf) {
                Ok(()) => {},
                Err(e) if e.kind() == io::ErrorKind::UnexpectedEof => {
                    // Detected corrupted or incomplete file
                    return Err(Error::Corrupted(format!(
                        "Unexpected EOF while reading key length at position {}", pos
                    )));
                },
                Err(e) => return Err(e.into()),
            }
            
            let key_len = u32::from_be_bytes(len_buf);
            
            // Read value length
            match reader.read_exact(&mut len_buf) {
                Ok(()) => {},
                Err(e) => return Err(Error::Corrupted(format!(
                    "Failed to read value length at position {}: {}", pos + 4, e
                ))),
            }
            
            let value_len = u32::from_be_bytes(len_buf);
            let value_pos = pos + 4 + 4 + key_len as u64;

            // Validate sizes
            if key_len > 1024 {
                return Err(Error::Corrupted(format!("Key length too large: {}", key_len)));
            }
            
            if value_len > 256 * 1024 {
                return Err(Error::Corrupted(format!("Value length too large: {}", value_len)));
            }

            // Read key
            let mut key = vec![0; key_len as usize];
            match reader.read_exact(&mut key) {
                Ok(()) => {},
                Err(e) => return Err(Error::Corrupted(format!(
                    "Failed to read key at position {}: {}", pos + 8, e
                ))),
            }

            // Read value
            let mut value = vec![0; value_len as usize];
            match reader.read_exact(&mut value) {
                Ok(()) => {},
                Err(e) => return Err(Error::Corrupted(format!(
                    "Failed to read value at position {}: {}", value_pos, e
                ))),
            }

            if value_len == 0 {
                key_map.remove(&key);
            } else {
                // wrap in Arc for cheap clones on get()
                let shared = Arc::from(value.into_boxed_slice());
                key_map.insert(key, shared);
            }
             
             pos = value_pos + value_len as u64;
         }
         
         Ok(key_map)
    }

    fn write_entry(&mut self, key: &[u8], value: &[u8], sync: bool) -> Result<()> {
        if key.len() > 1024 || value.len() > 256 * 1024 {
            return Err(Error::Other(format!(
                "Key or value length exceeds limits: key_len={}, value_len={}", 
                key.len(), value.len()
            )));
        }
        
        // Calculate entry size
        let key_len = key.len() as u32;
        let value_len = value.len() as u32;
        let len = 4 + 4 + key_len + value_len;

        // Prepare buffer
        let mut buffer = Vec::with_capacity(len as usize);
        buffer.extend_from_slice(&key_len.to_be_bytes());
        buffer.extend_from_slice(&value_len.to_be_bytes());
        buffer.extend_from_slice(key);
        buffer.extend_from_slice(value);
        
        // Write to file
        self.file.seek(SeekFrom::End(0))?;
        {
            let mut writer = BufWriter::with_capacity(len as usize, &mut self.file);
            writer.write_all(&buffer)?;
            writer.flush()?;
        }
        
        // Sync to disk if requested
        if sync {
            self.file.sync_all()?;
        }
        
        Ok(())
    }
}

// Add a Drop implementation for Log to unlock the file
impl Drop for Log {
    fn drop(&mut self) {
        // Ignore errors during drop, but try to unlock
        // Use fully qualified syntax to avoid name collisions
        let _ = FileExt::unlock(&self.file);
    }
}
