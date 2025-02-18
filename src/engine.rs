//! Tegdb Engine: A persistent key-value store with an append-only log and automatic compaction.
//! This module implements CRUD operations and log rebuilding to maintain data integrity.

use crate::wal; // Changed from crate::log
use crate::types::KeyMap;
use crate::logger::Logger;

use std::path::PathBuf;
use std::sync::Arc;
use std::ops::Range;
use std::fs::OpenOptions;

const MAX_KEY_SIZE: usize = 1024;
const MAX_VALUE_SIZE: usize = 256 * 1024;

/// Core storage engine that provides CRUD operations with log compaction.
#[derive(Clone)]
pub struct Engine {
    wal: Arc<wal::Wal>,               // Changed field name from log to wal
    key_map: Arc<KeyMap>,
    lock_file: Arc<std::fs::File>,
    lock_path: PathBuf,
    logger: Arc<Logger>,
}

impl Engine {
    /// Creates a new Engine instance.
    /// Initializes the underlying log, reconstructs the in-memory key map from the log,
    /// and performs compaction if the removal/insertion ratio is at least 0.3.
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
        // Initialize the logger using the same data directory.
        let logger = Logger::new(&path).expect("Failed to initialize logger");
        let mut s = Self { 
            wal, 
            key_map: Arc::new(key_map),
            lock_file: Arc::new(lock_file),
            lock_path,
            logger: logger.clone(),
        };
        s.logger.log("Engine initialized");
        // println!("Engine stats: {} inserts, {} removals", insert_count, remove_count);
        if insert_count > 0 && ((remove_count as f64) / (insert_count as f64)) >= 0.3 {
            s.compact().expect("Failed to compact wal");
        }
        s
    }

    /// Retrieves the value associated with the given key asynchronously.
    pub async fn get(&self, key: &[u8]) -> Option<Vec<u8>> {
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
            if *existing == value {
                return Ok(());
            }
        }
        self.wal.write_entry(key, &value);
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
        self.key_map.remove(key);
        Ok(())
    }

    /// Returns an iterator over key-value pairs within the specified range.
    pub async fn scan<'a>(
        &'a self,
        range: Range<Vec<u8>>,
    ) -> Result<Box<dyn Iterator<Item = (Vec<u8>, Vec<u8>)> + 'a>, std::io::Error> {
        let mut results: Vec<(Vec<u8>, Vec<u8>)> = self
            .key_map
            .iter()
            .filter(|entry| entry.key() >= &range.start && entry.key() < &range.end)
            .map(|entry| (entry.key().clone(), entry.value().clone()))
            .collect();
        results.sort_by(|a, b| a.0.cmp(&b.0));
        Ok(Box::new(results.into_iter()))
    }

    /// Returns an iterator over key-value pairs within the specified range in reverse order.
    pub async fn reverse_scan(
        &self,
        range: Range<Vec<u8>>,
    ) -> Result<Box<dyn Iterator<Item = (Vec<u8>, Vec<u8>)>>, std::io::Error> {
        let mut results: Vec<(Vec<u8>, Vec<u8>)> = self
            .key_map
            .iter()
            .filter(|entry| entry.key() >= &range.start && entry.key() < &range.end)
            .map(|entry| (entry.key().clone(), entry.value().clone()))
            .collect();
        results.sort_by(|a, b| b.0.cmp(&a.0)); // sort in descending order
        Ok(Box::new(results.into_iter()))
    }

    /// Flushes the current log and shuts down the log writer to ensure data persistence.
    fn flush(&mut self) -> Result<(), std::io::Error> {
        self.logger.log("Flushing wal");
        self.wal.writer.flush();
        self.wal.writer.shutdown();
        Ok(())
    }

    /// Compacts logs by switching new writes to a new log file (number incremented by 1)
    /// and then rewriting the old log with compacted data.
    pub fn compact(&mut self) -> Result<(), std::io::Error> {
        self.logger.log("Compacting wal...");
        //println!("Compacting log...");
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
        self.logger.log("Compacting done.");
        //println!("Compacting done.");
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
        Ok(new_wal)
    }
}

impl Drop for Engine {
    fn drop(&mut self) {
        self.flush().unwrap();
        // Remove the lock file only once.
        if Arc::strong_count(&self.lock_file) == 1 {
            let _ = std::fs::remove_file(&self.lock_path);
        }
    }
}
