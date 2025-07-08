#[cfg(target_arch = "wasm32")]
use serde::{Deserialize, Serialize};
#[cfg(target_arch = "wasm32")]
use wasm_bindgen::prelude::*;
#[cfg(target_arch = "wasm32")]
use web_sys::{window, Storage};

#[cfg(target_arch = "wasm32")]
use crate::error::{Error, Result};
#[cfg(target_arch = "wasm32")]
use crate::log::{KeyMap, LogConfig, TX_COMMIT_MARKER};
#[cfg(target_arch = "wasm32")]
use crate::log::LogBackend;
#[cfg(target_arch = "wasm32")]
use std::sync::Arc;
#[cfg(target_arch = "wasm32")]
use crate::protocol_utils::parse_storage_identifier;

/// Browser-based storage backend for WASM platforms
#[cfg(target_arch = "wasm32")]
pub struct BrowserLogBackend {
    db_name: String,
    storage: Storage,
}

#[cfg(target_arch = "wasm32")]
#[derive(Serialize, Deserialize)]
struct LogEntry {
    key: Vec<u8>,
    value: Vec<u8>,
}

#[cfg(target_arch = "wasm32")]
impl LogBackend for BrowserLogBackend {
    fn new(identifier: String, _config: &LogConfig) -> Result<Self> {
        // Parse protocol and extract database name
        let (protocol, db_name) = parse_storage_identifier(&identifier);
        
        // Validate protocol for browser backend
        if !matches!(protocol, "browser" | "localstorage" | "indexeddb") {
            return Err(Error::Other(format!(
                "BrowserLogBackend only supports 'browser://', 'localstorage://', or 'indexeddb://' protocols, got '{}://'",
                protocol
            )));
        }

        let window =
            window().ok_or_else(|| Error::Other("No window object available".to_string()))?;
        let storage = window
            .local_storage()
            .map_err(|_| Error::Other("Cannot access localStorage".to_string()))?
            .ok_or_else(|| Error::Other("localStorage not available".to_string()))?;

        Ok(Self { db_name: db_name.to_string(), storage })
    }

    fn build_key_map(&mut self, config: &LogConfig) -> Result<KeyMap> {
        let mut key_map = KeyMap::new();
        let log_key = format!("{}:log", self.db_name);

        // Load existing log data from localStorage
        if let Ok(Some(log_data)) = self.storage.get_item(&log_key) {
            let entries: Vec<LogEntry> = serde_json::from_str(&log_data)
                .map_err(|e| Error::Other(format!("Failed to parse stored log: {}", e)))?;

            let mut uncommitted_changes = Vec::new();
            let mut committed = false;

            // Replay log entries (same logic as file backend)
            for entry in entries {
                if entry.key == TX_COMMIT_MARKER {
                    uncommitted_changes.clear();
                    committed = true;
                } else {
                    // Validate entry size
                    if entry.key.len() > config.max_key_size
                        || entry.value.len() > config.max_value_size
                    {
                        break; // Invalid entry
                    }

                    let old_value = if entry.value.is_empty() {
                        key_map.remove(&entry.key)
                    } else {
                        key_map.insert(entry.key.clone(), Arc::from(entry.value.into_boxed_slice()))
                    };
                    uncommitted_changes.push((entry.key, old_value));
                }
            }

            // Rollback uncommitted changes if we saw a commit marker
            if committed {
                for (key, old_value) in uncommitted_changes.into_iter().rev() {
                    if let Some(value) = old_value {
                        key_map.insert(key, value);
                    } else {
                        key_map.remove(&key);
                    }
                }
            }
        }

        Ok(key_map)
    }

    fn write_entry(&mut self, key: &[u8], value: &[u8]) -> Result<()> {
        if key.len() > 1024 || value.len() > 256 * 1024 {
            return Err(Error::Other(format!(
                "Key or value length exceeds limits: key_len={}, value_len={}",
                key.len(),
                value.len()
            )));
        }

        let log_key = format!("{}:log", self.db_name);

        // Read existing log
        let mut entries: Vec<LogEntry> = if let Ok(Some(data)) = self.storage.get_item(&log_key) {
            serde_json::from_str(&data).unwrap_or_default()
        } else {
            Vec::new()
        };

        // Append new entry
        entries.push(LogEntry {
            key: key.to_vec(),
            value: value.to_vec(),
        });

        // Store back to localStorage
        let serialized = serde_json::to_string(&entries)
            .map_err(|e| Error::Other(format!("Failed to serialize log: {}", e)))?;

        self.storage
            .set_item(&log_key, &serialized)
            .map_err(|_| Error::Other("Failed to write to localStorage".to_string()))?;

        Ok(())
    }

    fn sync_all(&mut self) -> Result<()> {
        // Browser storage is automatically persistent, no explicit sync needed
        Ok(())
    }

    fn set_len(&mut self, size: u64) -> Result<()> {
        if size == 0 {
            // Clear all data for this database
            let log_key = format!("{}:log", self.db_name);
            self.storage
                .remove_item(&log_key)
                .map_err(|_| Error::Other("Failed to clear storage".to_string()))?;
        }
        Ok(())
    }

    fn rename_to(&mut self, new_identifier: String) -> Result<()> {
        let new_db_name = new_identifier
            .trim_start_matches("browser://")
            .trim_start_matches("localstorage://")
            .trim_start_matches("indexeddb://")
            .to_string();

        let old_log_key = format!("{}:log", self.db_name);
        let new_log_key = format!("{}:log", new_db_name);

        // Copy data from old key to new key
        if let Ok(Some(data)) = self.storage.get_item(&old_log_key) {
            self.storage
                .set_item(&new_log_key, &data)
                .map_err(|_| Error::Other("Failed to copy to new storage key".to_string()))?;

            // Remove old data
            self.storage
                .remove_item(&old_log_key)
                .map_err(|_| Error::Other("Failed to remove old storage key".to_string()))?;
        }

        self.db_name = new_db_name;
        Ok(())
    }
}

// Export only when targeting WASM
#[cfg(target_arch = "wasm32")]
pub use BrowserLogBackend as DefaultLogBackend;
