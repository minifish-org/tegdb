// filepath: /Users/yusp/work/tegdb/src/engine.rs
use std::io::{self, BufReader, BufWriter, Read, Seek, SeekFrom, Write};
use std::ops::Range;
use std::path::PathBuf;
use std::collections::BTreeMap;
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
    /// Note: TegDB prioritizes performance over durability - latest commits may not persist on crash
    pub sync_on_write: bool,
    /// Whether to automatically compact on open (default: true)
    pub auto_compact: bool,
}

impl Default for EngineConfig {
    fn default() -> Self {
        Self {
            max_key_size: 1024,
            max_value_size: 256 * 1024,
            sync_on_write: false, // Changed: only sync on explicit commits, not every write
            auto_compact: true,
        }
    }
}



/// The main database engine
pub struct Engine {
    log: Log,
    key_map: KeyMap,
    config: EngineConfig,
}

// Transaction commit marker - special marker that won't be part of keymap
const TX_COMMIT_MARKER: &[u8] = b"__TX_COMMIT__";

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

    /// Begins a new write-through transaction
    pub fn begin_transaction(&mut self) -> Transaction<'_> {
        // Don't write begin marker - only write commit marker on commit
        Transaction { 
            engine: self, 
            undo_log: None, // Lazy initialization
            finalized: false,
        }
    }

    /// Retrieves a value by key (zero-copy refcounted Arc)
    pub fn get(&self, key: &[u8]) -> Option<Arc<[u8]>> {
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
        let shared = Arc::from(value.into_boxed_slice());
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
    pub fn scan(
        &self,
        range: Range<Vec<u8>>,
    ) -> Result<ScanResult<'_>> {
        let iter = self.key_map.range(range)
            // clone key Vec (small) and clone Arc (cheap refcount increment)
            .map(|(key, value)| (key.clone(), Arc::clone(value)));
        Ok(Box::new(iter))
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
            new_log.write_entry(key, value.as_ref())?;
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

/// Undo log entry for rollback
struct UndoEntry {
    key: Vec<u8>,
    old_value: Option<Arc<[u8]>>, // None means key didn't exist
}

/// Write-through transactional context for ACID operations
pub struct Transaction<'a> {
    engine: &'a mut Engine,
    undo_log: Option<Vec<UndoEntry>>, // Lazy initialization
    finalized: bool, // Track if transaction has been committed or rolled back
}

impl Transaction<'_> {
    /// Records the current state for potential rollback and returns the old value
    fn record_undo(&mut self, key: &[u8]) -> Option<Arc<[u8]>> {
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
    pub fn get(&self, key: &[u8]) -> Option<Arc<[u8]>> {
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
        self.finalized || self.undo_log.as_ref().map_or(true, |log| log.is_empty())
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
                match undo_entry.old_value {
                    Some(old_value) => {
                        // Restore the old value using engine's set method
                        self.engine.set(&undo_entry.key, old_value.to_vec())?;
                    }
                    None => {
                        // Key didn't exist, remove it using engine's del method
                        self.engine.del(&undo_entry.key)?;
                    }
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
         let mut last_commit_pos = 0; // Track position of last commit marker

         // First pass: find the last commit marker
         while pos < file_len {
            // Read key length
            match reader.read_exact(&mut len_buf) {
                Ok(()) => {},
                Err(e) if e.kind() == io::ErrorKind::UnexpectedEof => {
                    // Corrupted file, stop processing
                    break;
                },
                Err(e) => return Err(e.into()),
            }
            
            let key_len = u32::from_be_bytes(len_buf);
            
            // Read value length
            match reader.read_exact(&mut len_buf) {
                Ok(()) => {},
                Err(_) => break, // Corrupted file, stop processing
            }
            
            let value_len = u32::from_be_bytes(len_buf);

            // Validate sizes to prevent memory allocation attacks
            if key_len > 1024 || value_len > 256 * 1024 {
                break; // Invalid sizes, stop processing
            }

            // Read key
            let mut key = vec![0; key_len as usize];
            match reader.read_exact(&mut key) {
                Ok(()) => {},
                Err(_) => break, // Corrupted file, stop processing
            }

            // Check if this is a commit marker
            if key == TX_COMMIT_MARKER {
                // This is a commit marker, update last commit position
                last_commit_pos = pos + 4 + 4 + key_len as u64 + value_len as u64;
            }

            // Skip value and move to next entry
            let value_pos = pos + 4 + 4 + key_len as u64;
            pos = value_pos + value_len as u64;
            reader.seek(SeekFrom::Start(pos))?;
         }
         
         // If no commit markers found, recover all data (for non-transactional operations)
         let recovery_end = if last_commit_pos == 0 { file_len } else { last_commit_pos };
         
         // Second pass: build keymap up to recovery end
         reader.seek(SeekFrom::Start(0))?;
         pos = 0;
         
         while pos < recovery_end {
            // Read key length
            if reader.read_exact(&mut len_buf).is_err() {
                break;
            }
            let key_len = u32::from_be_bytes(len_buf);
            
            // Read value length
            if reader.read_exact(&mut len_buf).is_err() {
                break;
            }
            let value_len = u32::from_be_bytes(len_buf);

            // Read key
            let mut key = vec![0; key_len as usize];
            if reader.read_exact(&mut key).is_err() {
                break;
            }

            // Skip commit markers in keymap building
            if key == TX_COMMIT_MARKER {
                // Skip value and continue
                reader.seek(SeekFrom::Current(value_len as i64))?;
                pos = pos + 4 + 4 + key_len as u64 + value_len as u64;
                continue;
            }

            // Read value
            let mut value = vec![0; value_len as usize];
            if reader.read_exact(&mut value).is_err() {
                break;
            }

            // Apply to keymap
            if value.is_empty() {
                key_map.remove(&key);
            } else {
                let shared = Arc::from(value.into_boxed_slice());
                key_map.insert(key, shared);
            }

            pos = pos + 4 + 4 + key_len as u64 + value_len as u64;
         }
         
         Ok(key_map)
    }
    
    fn write_entry(&mut self, key: &[u8], value: &[u8]) -> Result<()> {
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
        
        // Note: No fsync for performance - latest commits may not persist on crash
        
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
