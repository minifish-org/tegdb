//! Tegdb Engine: A persistent key-value store implementing the Engine layer of the two-layer architecture.
//! Features Write-Ahead Logging (WAL) for durability and MVCC for transaction isolation.

use crate::types::KeyMap;
use crate::wal;

use log::info;
use std::fs::OpenOptions;
use std::ops::Range;
use std::path::PathBuf;
use std::sync::Arc;

const MAX_KEY_SIZE: usize = 1024;
const MAX_VALUE_SIZE: usize = 256 * 1024;

/// Core storage engine implementing the Engine layer with WAL and MVCC support.
/// Provides CRUD operations with log compaction and transaction isolation.
#[derive(Clone)]
pub struct Engine {
    // Write-Ahead Log for durability and crash recovery
    wal: Arc<wal::Wal>,
    // In-memory key map using SkipList for efficient concurrent access
    key_map: Arc<KeyMap>,
    // Lock file for exclusive access to the database directory
    lock_file: Arc<std::fs::File>,
    lock_path: PathBuf,
}

impl Engine {
    /// Creates a new Engine instance.
    /// Initializes the WAL, reconstructs the in-memory key map from the log,
    /// and performs compaction if the removal/insertion ratio is at least 0.3.
    /// The WAL ensures durability while the key map provides efficient access for MVCC.
    pub fn new(path: PathBuf) -> Self {
        // Create the directory if it doesn't exist.
        std::fs::create_dir_all(&path).expect("Failed to create directory");
        // Create a lock file for exclusive access.
        let lock_path = path.join("lock.lock");
        let lock_file = OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&lock_path)
            .expect("Data directory is already in use by another instance");
        // Use a fixed log file for new writes.
        let wal_path = path.join("wal.new"); // Updated file name
        let wal = Arc::new(wal::Wal::new(wal_path));
        let (key_map, (insert_count, remove_count)) = wal.build_key_map();
        let mut s = Self {
            wal,
            key_map: Arc::new(key_map),
            lock_file: Arc::new(lock_file),
            lock_path,
        };
        info!("Engine initialized");
        // println!("Engine stats: {} inserts, {} removals", insert_count, remove_count);
        if insert_count > crate::constants::COMPACTION_INSERT_THRESHOLD
            && ((remove_count as f64) / (insert_count as f64))
                >= crate::constants::REMOVAL_RATIO_THRESHOLD
        {
            s.compact().expect("Failed to compact wal");
        }
        s
    }

    /// Retrieves the value associated with the given key asynchronously.
    pub async fn get(&self, key: &[u8]) -> Option<Vec<u8>> {
        // Updated: Using SkipMap's API which returns a reference where value() retrieves the stored value.
        self.key_map.get(key).map(|entry| entry.value().clone())
    }

    /// Inserts or updates the value for the given key.
    /// If an empty value is provided, the key is removed.
    /// Returns an error if the key or value exceeds predefined size limits.
    pub async fn set(&self, key: &[u8], value: Vec<u8>) -> Result<(), std::io::Error> {
        if key.len() > MAX_KEY_SIZE {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                "Key length exceeds allowed limit",
            ));
        }
        if value.len() > MAX_VALUE_SIZE {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                "Value length exceeds allowed limit",
            ));
        }
        if value.is_empty() {
            return self.del(key).await;
        }
        if let Some(existing) = self.key_map.get(key) {
            if *existing.value() == value {
                return Ok(());
            }
        }
        self.wal.write_entry(key, &value);
        // Updated: SkipMap insert API
        self.key_map.insert(key.to_vec(), value);
        Ok(())
    }

    /// Deletes a key-value pair from the store.
    /// If the key does not exist, the operation is a no-op.
    pub async fn del(&self, key: &[u8]) -> Result<(), std::io::Error> {
        if self.key_map.get(key).is_none() {
            return Ok(());
        }
        self.wal.write_entry(key, &[]);
        // Updated: SkipMap removal API
        self.key_map.remove(key);
        Ok(())
    }

    // Updated: Use SkipMap's range API.
    fn scan_internal(
        &self,
        start: Vec<u8>,
        end: Vec<u8>,
        reverse: bool,
    ) -> Vec<(Vec<u8>, Vec<u8>)> {
        let range_iter = self
            .key_map
            .range(start..end)
            .map(|entry| (entry.key().clone(), entry.value().clone()));
        if reverse {
            range_iter.rev().collect()
        } else {
            range_iter.collect()
        }
    }

    /// Returns a Vec of key-value pairs within the specified range.
    pub async fn scan(
        &self,
        range: Range<Vec<u8>>,
    ) -> Result<Vec<(Vec<u8>, Vec<u8>)>, std::io::Error> {
        Ok(self.scan_internal(range.start, range.end, false))
    }

    /// Returns a Vec of key-value pairs within the specified range in reverse order.
    pub async fn reverse_scan(
        &self,
        range: Range<Vec<u8>>,
    ) -> Result<Vec<(Vec<u8>, Vec<u8>)>, std::io::Error> {
        Ok(self.scan_internal(range.start, range.end, true))
    }

    /// Flushes the current log and shuts down the log writer to ensure data persistence.
    fn shutdown(&mut self) -> Result<(), std::io::Error> {
        self.wal.writer.flush();
        self.wal.writer.shutdown();
        Ok(())
    }

    pub fn flush(&self) -> Result<(), std::io::Error> {
        self.wal.writer.flush();
        Ok(())
    }

    /// Compacts logs by switching new writes to a new log file (number incremented by 1)
    /// and then rewriting the old log with compacted data.
    pub fn compact(&mut self) -> Result<(), std::io::Error> {
        info!("Compacting wal...");
        // Rename the current active log ("log.new") to "log.old".
        let old_wal_path = self.wal.path.clone();
        let parent = old_wal_path.parent().expect("Invalid directory");
        let new_old_wal_path = parent.join("wal.old");
        // Remove existing log.old if it exists to avoid panic.
        if new_old_wal_path.exists() {
            std::fs::remove_file(&new_old_wal_path)?;
        }
        std::fs::rename(&old_wal_path, &new_old_wal_path)?;
        // Create a new "log.new" file for fresh writes.
        let new_wal_path = parent.join("wal.new");
        let new_wal = wal::Wal::new(new_wal_path);
        self.wal = Arc::new(new_wal);
        // Compact the renamed old log.
        self.construct_wal(new_old_wal_path)?;
        info!("Compacting done.");
        Ok(())
    }

    /// Constructs a compacted log file based on valid entries from the current key map.
    fn construct_wal(&mut self, path: PathBuf) -> Result<wal::Wal, std::io::Error> {
        let new_wal = wal::Wal::new(path);
        {
            // Clear new log file
            let file = std::fs::OpenOptions::new()
                .write(true)
                .open(&new_wal.path)?;
            file.set_len(0)?;
        }
        // Write existing key map entries to the new log
        for entry in self.key_map.iter() {
            new_wal.write_entry(entry.key(), entry.value());
        }
        // Flush and close the log file.
        new_wal.writer.flush();
        new_wal.writer.shutdown();
        Ok(new_wal)
    }
}

impl Drop for Engine {
    fn drop(&mut self) {
        self.shutdown().unwrap();
        // Remove the lock file only once.
        if Arc::strong_count(&self.lock_file) == 1 {
            let _ = std::fs::remove_file(&self.lock_path);
        }
    }
}
