use std::io::{Read, Seek, SeekFrom, Write};

use fs2::FileExt;

use crate::error::{Error, Result};
use crate::log::{
    KeyMap, LogBackend, LogConfig, LENGTH_FIELD_BYTES, STORAGE_FORMAT_VERSION, STORAGE_HEADER_SIZE,
    STORAGE_MAGIC, TX_COMMIT_MARKER,
};
use crate::protocol_utils::parse_storage_identifier;
use std::rc::Rc;

type ChangeRecord = (Vec<u8>, Option<Rc<[u8]>>);

/// File-based storage backend for native platforms
pub struct FileLogBackend {
    path: std::path::PathBuf,
    file: std::fs::File,
}

impl LogBackend for FileLogBackend {
    fn new(identifier: String, _config: &LogConfig) -> Result<Self> {
        // Parse protocol and extract file path
        let (protocol, path_str) = parse_storage_identifier(&identifier);

        // Validate protocol for file backend
        if protocol != crate::protocol_utils::PROTOCOL_NAME_FILE {
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

        let mut backend = Self { path, file };

        // Initialize or validate header
        let len = backend.file.metadata()?.len();
        if len == 0 {
            // Fresh file: write header with provided limits
            backend.write_header(_config)?;
        } else {
            // Existing file: validate header
            backend.read_header()?;
        }

        Ok(backend)
    }

    fn build_key_map(&mut self, config: &LogConfig) -> Result<KeyMap> {
        let mut key_map = KeyMap::new();
        let mut uncommitted_changes: Vec<ChangeRecord> = Vec::new();
        let file_len = self.file.metadata()?.len();
        let mut reader = std::io::BufReader::new(&mut self.file);
        // Start scanning after fixed header
        let mut pos = reader.seek(SeekFrom::Start(STORAGE_HEADER_SIZE as u64))?;
        let mut len_buf = [0u8; LENGTH_FIELD_BYTES];
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
        if key.len() > crate::log::DEFAULT_MAX_KEY_SIZE
            || value.len() > crate::log::DEFAULT_MAX_VALUE_SIZE
        {
            return Err(Error::Other(format!(
                "Key or value length exceeds limits: key_len={}, value_len={}",
                key.len(),
                value.len()
            )));
        }

        // Calculate entry size
        let key_len = key.len() as u32;
        let value_len = value.len() as u32;
        let len = (LENGTH_FIELD_BYTES * 2) as u32 + key_len + value_len;

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

impl FileLogBackend {
    fn write_header(&mut self, config: &LogConfig) -> Result<()> {
        let mut header = vec![0u8; STORAGE_HEADER_SIZE];
        // magic [0..6)
        header[0..STORAGE_MAGIC.len()].copy_from_slice(STORAGE_MAGIC);
        // version [6..8)
        header[6..8].copy_from_slice(&STORAGE_FORMAT_VERSION.to_be_bytes());
        // flags [8..12) = 0
        header[8..12].copy_from_slice(&0u32.to_be_bytes());
        // max_key [12..16)
        let max_key = (config.max_key_size as u32).to_be_bytes();
        header[12..16].copy_from_slice(&max_key);
        // max_val [16..20)
        let max_val = (config.max_value_size as u32).to_be_bytes();
        header[16..20].copy_from_slice(&max_val);
        // endianness [20] = 1 (BE)
        header[20] = 1u8;
        // [21..64) reserved = 0

        self.file.seek(SeekFrom::Start(0))?;
        self.file.write_all(&header)?;
        self.file.sync_all().map_err(Error::from)
    }

    fn read_header(&mut self) -> Result<()> {
        let mut header = vec![0u8; STORAGE_HEADER_SIZE];
        self.file.seek(SeekFrom::Start(0))?;
        self.file.read_exact(&mut header)?;

        // magic
        if &header[0..STORAGE_MAGIC.len()] != STORAGE_MAGIC {
            return Err(Error::InvalidMagic);
        }
        // version
        let version = u16::from_be_bytes([header[6], header[7]]);
        if version != STORAGE_FORMAT_VERSION {
            return Err(Error::UnsupportedVersion(version));
        }
        // minimal sanity: endianness 1
        if header[20] != 1u8 {
            return Err(Error::CorruptHeader("unsupported endianness"));
        }

        Ok(())
    }
}

impl Drop for FileLogBackend {
    fn drop(&mut self) {
        // Ignore errors during drop, but try to unlock
        let _ = FileExt::unlock(&self.file);
    }
}

pub type DefaultLogBackend = FileLogBackend;
