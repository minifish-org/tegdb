use std::io::{Read, Seek, SeekFrom, Write};
use std::rc::Rc;

#[cfg(not(target_arch = "wasm32"))]
use fs2::FileExt;

use crate::error::{Error, Result};
use crate::log::LogBackend;
use crate::log::{KeyMap, LogConfig, TX_COMMIT_MARKER};
use crate::protocol_utils::parse_storage_identifier;

/// File-based storage backend for native platforms
#[cfg(not(target_arch = "wasm32"))]
pub struct FileLogBackend {
    path: std::path::PathBuf,
    file: std::fs::File,
}

#[cfg(not(target_arch = "wasm32"))]
impl LogBackend for FileLogBackend {
    fn new(identifier: String, _config: &LogConfig) -> Result<Self> {
        // Parse protocol and extract file path
        let (protocol, path_str) = parse_storage_identifier(&identifier);

        // Validate protocol for file backend
        if protocol != "file" {
            return Err(Error::Other(format!(
                "FileLogBackend only supports 'file://' protocol, got '{protocol}://'"
            )));
        }

        let path = std::path::PathBuf::from(path_str);

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

    fn build_key_map(&mut self, config: &LogConfig) -> Result<KeyMap> {
        let mut key_map = KeyMap::new();
        let mut uncommitted_changes: Vec<(Vec<u8>, Option<Rc<[u8]>>)> = Vec::new();
        let file_len = self.file.metadata()?.len();
        let mut reader = std::io::BufReader::new(&mut self.file);
        let mut pos = reader.seek(SeekFrom::Start(0))?;
        let mut len_buf = [0u8; 4];
        let mut committed = false;

        while pos < file_len {
            // Read key length
            if reader.read_exact(&mut len_buf).is_err() {
                break; // Corrupted entry
            }
            let key_len = u32::from_be_bytes(len_buf);

            // Read value length
            if reader.read_exact(&mut len_buf).is_err() {
                break; // Corrupted
            }
            let value_len = u32::from_be_bytes(len_buf);

            // Basic validation
            if key_len as usize > config.max_key_size || value_len as usize > config.max_value_size
            {
                break; // Invalid entry, treat as corruption
            }

            // Read key
            let mut key = vec![0; key_len as usize];
            if reader.read_exact(&mut key).is_err() {
                break; // Corrupted
            }

            // Check for commit marker
            if key == TX_COMMIT_MARKER {
                uncommitted_changes.clear();
                committed = true;
                reader.seek(SeekFrom::Current(value_len as i64))?;
            } else {
                // Read value
                let mut value = vec![0; value_len as usize];
                if reader.read_exact(&mut value).is_err() {
                    break; // Corrupted
                }

                let old_value = if value.is_empty() {
                    key_map.remove(&key)
                } else {
                    key_map.insert(key.clone(), Rc::from(value.into_boxed_slice()))
                };
                uncommitted_changes.push((key, old_value));
            }

            pos = reader.stream_position()?;
        }

        // Rollback uncommitted changes if any commit marker was seen
        if committed {
            for (key, old_value) in uncommitted_changes.into_iter().rev() {
                if let Some(value) = old_value {
                    key_map.insert(key, value);
                } else {
                    key_map.remove(&key);
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

        // Calculate entry size
        let key_len = key.len() as u32;
        let value_len = value.len() as u32;
        let len = 4 + 4 + key_len + value_len;

        // Prepare buffer with same binary format as original TegDB
        let mut buffer = Vec::with_capacity(len as usize);
        buffer.extend_from_slice(&key_len.to_be_bytes());
        buffer.extend_from_slice(&value_len.to_be_bytes());
        buffer.extend_from_slice(key);
        buffer.extend_from_slice(value);

        // Write to file
        self.file.seek(SeekFrom::End(0))?;
        {
            let mut writer = std::io::BufWriter::with_capacity(len as usize, &mut self.file);
            writer.write_all(&buffer)?;
            writer.flush()?;
        }

        Ok(())
    }

    fn sync_all(&mut self) -> Result<()> {
        self.file.sync_all().map_err(Error::from)
    }

    fn set_len(&mut self, size: u64) -> Result<()> {
        self.file.set_len(size).map_err(Error::from)
    }

    fn rename_to(&mut self, new_identifier: String) -> Result<()> {
        let new_path = std::path::PathBuf::from(new_identifier);
        std::fs::rename(&self.path, &new_path)?;
        self.path = new_path;
        Ok(())
    }
}

#[cfg(not(target_arch = "wasm32"))]
impl Drop for FileLogBackend {
    fn drop(&mut self) {
        // Ignore errors during drop, but try to unlock
        let _ = fs2::FileExt::unlock(&self.file);
    }
}

// Export only when not targeting WASM
#[cfg(not(target_arch = "wasm32"))]
pub use FileLogBackend as DefaultLogBackend;
