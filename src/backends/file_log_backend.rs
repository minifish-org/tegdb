use std::io::{Read, Seek, SeekFrom, Write};

use fs2::FileExt;

use crate::error::{Error, Result};
use crate::log::{
    KeyMap, LogBackend, LogConfig, ValuePointer, WriteOutcome, LENGTH_FIELD_BYTES,
    STORAGE_FORMAT_VERSION, STORAGE_HEADER_SIZE, STORAGE_MAGIC, TX_COMMIT_MARKER,
};
use crate::protocol_utils::parse_storage_identifier;
use std::rc::Rc;

type ChangeRecord = (Vec<u8>, Option<ValuePointer>);

struct KeyBufferPool {
    buffers: Vec<Vec<u8>>,
    max_key_size: usize,
}

impl KeyBufferPool {
    fn new(capacity: usize, max_key_size: usize) -> Self {
        let mut buffers = Vec::with_capacity(capacity);
        for _ in 0..capacity {
            buffers.push(Vec::with_capacity(max_key_size));
        }
        Self {
            buffers,
            max_key_size,
        }
    }

    fn take(&mut self, min_len: usize) -> Vec<u8> {
        let mut buf = self
            .buffers
            .pop()
            .unwrap_or_else(|| Vec::with_capacity(self.max_key_size.max(min_len)));
        if buf.capacity() < min_len {
            buf.reserve(min_len - buf.capacity());
        }
        buf.clear();
        buf
    }

    fn clone_from(&mut self, data: &[u8]) -> Vec<u8> {
        let mut buf = self.take(data.len());
        buf.extend_from_slice(data);
        buf
    }

    fn recycle(&mut self, mut buf: Vec<u8>) {
        buf.clear();
        self.buffers.push(buf);
    }
}

/// File-based storage backend for native platforms
pub struct FileLogBackend {
    path: std::path::PathBuf,
    file: std::fs::File,
    valid_data_end: u64,       // Track the end of valid data (for preallocation)
    max_file_len: Option<u64>, // Optional hard limit for on-disk size
}

impl LogBackend for FileLogBackend {
    fn new(identifier: String, config: &LogConfig) -> Result<Self> {
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

        let max_file_len = if let Some(size) = config.preallocate_size {
            if size < STORAGE_HEADER_SIZE as u64 {
                return Err(Error::Other(format!(
                    "preallocate_size ({size}) must be at least {} bytes",
                    STORAGE_HEADER_SIZE
                )));
            }
            Some(size)
        } else {
            None
        };

        let mut backend = Self {
            path,
            file,
            valid_data_end: STORAGE_HEADER_SIZE as u64,
            max_file_len,
        };

        // Initialize or validate header
        let len = backend.file.metadata()?.len();
        if let Some(limit) = backend.max_file_len {
            if len > limit {
                return Err(Error::OutOfStorageQuota { bytes: limit });
            }
        }

        if len == 0 {
            // Fresh file: write header with provided limits
            backend.write_header(config)?;

            // Preallocate disk space if requested
            if let Some(preallocate_size) = backend.max_file_len {
                backend.file.set_len(preallocate_size)?;
            }
        } else {
            // Existing file: validate header and read valid_data_end
            backend.read_header()?;

            if let Some(limit) = backend.max_file_len {
                if backend.valid_data_end > limit {
                    return Err(Error::OutOfStorageQuota { bytes: limit });
                }
            }
        }

        Ok(backend)
    }

    fn build_key_map(&mut self, config: &LogConfig) -> Result<(KeyMap, u64)> {
        let initial_capacity = config.initial_capacity.unwrap_or(0);
        let mut key_map = KeyMap::new();
        let mut key_pool = KeyBufferPool::new(initial_capacity, config.max_key_size);
        let mut uncommitted_changes: Vec<ChangeRecord> = Vec::with_capacity(initial_capacity);
        let inline_threshold = config.inline_value_threshold;
        // Fall back to the physical file length so crash-written data is still discovered.
        let header_valid_data_end = self.valid_data_end;
        let scan_end = self.file.metadata()?.len();

        if let Some(limit) = self.max_file_len {
            if scan_end > limit {
                return Err(Error::OutOfStorageQuota { bytes: limit });
            }
            if header_valid_data_end > limit {
                return Err(Error::OutOfStorageQuota { bytes: limit });
            }
        }

        let mut reader = std::io::BufReader::new(&mut self.file);
        // Start scanning after fixed header
        let mut pos = reader.seek(SeekFrom::Start(STORAGE_HEADER_SIZE as u64))?;
        let mut last_good_pos = pos;
        let mut len_buf = [0u8; LENGTH_FIELD_BYTES];
        let mut commit_marker_seen = false;

        while pos < scan_end {
            let entry_start = pos;
            if reader.read_exact(&mut len_buf).is_err() {
                break; // Corrupted entry
            }
            let key_len = u32::from_be_bytes(len_buf);

            if reader.read_exact(&mut len_buf).is_err() {
                break; // Corrupted
            }
            let value_len = u32::from_be_bytes(len_buf);

            if pos >= header_valid_data_end && key_len == 0 && value_len == 0 {
                break; // Likely unwritten preallocated region
            }

            if key_len as usize > config.max_key_size || value_len as usize > config.max_value_size
            {
                break; // Invalid entry, treat as corruption
            }

            let key_len_usize = key_len as usize;
            let mut key_vec = key_pool.take(key_len_usize);
            key_vec.resize(key_len_usize, 0);
            if reader.read_exact(&mut key_vec).is_err() {
                key_pool.recycle(key_vec);
                break; // Corrupted
            }

            if key_vec.as_slice() == TX_COMMIT_MARKER {
                uncommitted_changes.clear();
                commit_marker_seen = true;
                if reader.seek(SeekFrom::Current(value_len as i64)).is_err() {
                    key_pool.recycle(key_vec);
                    break;
                }
                key_pool.recycle(key_vec);
                pos = reader.stream_position()?;
                last_good_pos = pos;
                continue;
            }

            let key_for_map = key_vec;
            let key_for_undo = key_pool.clone_from(&key_for_map);
            let mut old_value: Option<ValuePointer> = None;

            if value_len == 0 {
                if let Some((old_key, old_val)) = key_map.remove_entry(key_for_map.as_slice()) {
                    key_pool.recycle(old_key);
                    old_value = Some(old_val);
                }
                key_pool.recycle(key_for_map);
            } else {
                let value_offset = entry_start + (LENGTH_FIELD_BYTES * 2) as u64 + key_len as u64;
                if value_len as usize <= inline_threshold {
                    let mut value_buf = vec![0; value_len as usize];
                    if reader.read_exact(&mut value_buf).is_err() {
                        key_pool.recycle(key_for_map);
                        key_pool.recycle(key_for_undo);
                        break; // Corrupted
                    }
                    if let Some((old_key, old_val)) = key_map.remove_entry(key_for_map.as_slice()) {
                        key_pool.recycle(old_key);
                        old_value = Some(old_val);
                    }

                    let value_rc = Rc::from(value_buf.into_boxed_slice());
                    key_map.insert(
                        key_for_map,
                        ValuePointer::with_inline(value_offset, value_len, value_rc),
                    );
                } else {
                    // Skip over the value bytes efficiently
                    let mut take = reader.by_ref().take(value_len as u64);
                    std::io::copy(&mut take, &mut std::io::sink())?;

                    if let Some((old_key, old_val)) = key_map.remove_entry(key_for_map.as_slice()) {
                        key_pool.recycle(old_key);
                        old_value = Some(old_val);
                    }

                    key_map.insert(
                        key_for_map,
                        ValuePointer::new_on_disk(value_offset, value_len),
                    );
                }
            }

            uncommitted_changes.push((key_for_undo, old_value));

            let new_pos = reader.stream_position()?;
            last_good_pos = new_pos;
            pos = new_pos;
        }

        self.valid_data_end = last_good_pos;
        drop(reader);

        if self.valid_data_end != header_valid_data_end {
            if let Some(limit) = self.max_file_len {
                if self.valid_data_end > limit {
                    return Err(Error::OutOfStorageQuota { bytes: limit });
                }
            }
            self.update_valid_data_end_in_header()?;
        }

        if commit_marker_seen {
            for (key, old_value) in uncommitted_changes.into_iter().rev() {
                if let Some(value) = old_value {
                    key_map.insert(key, value);
                } else {
                    key_map.remove(&key);
                }
            }
        }

        // Calculate active data size
        let mut active_data_size: u64 = 0;
        for (key, value) in &key_map {
            // Overhead: key_len (4) + value_len (4)
            active_data_size += (LENGTH_FIELD_BYTES * 2) as u64;
            active_data_size += key.len() as u64;
            active_data_size += value.len() as u64;
        }

        Ok((key_map, active_data_size))
    }

    fn write_entry(&mut self, key: &[u8], value: &[u8]) -> Result<WriteOutcome> {
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
        let value_offset = self.valid_data_end + (LENGTH_FIELD_BYTES * 2) as u64 + key_len as u64;

        // Prepare buffer with same binary format as original TegDB
        let mut buffer = Vec::with_capacity(len as usize);
        buffer.extend_from_slice(&key_len.to_be_bytes());
        buffer.extend_from_slice(&value_len.to_be_bytes());
        buffer.extend_from_slice(key);
        buffer.extend_from_slice(value);

        // Write to file at valid_data_end position
        let new_end = self
            .valid_data_end
            .checked_add(len as u64)
            .ok_or_else(|| Error::Other("Log size would overflow u64".into()))?;
        if let Some(limit) = self.max_file_len {
            if new_end > limit {
                return Err(Error::OutOfStorageQuota { bytes: limit });
            }
        }
        self.file.seek(SeekFrom::Start(self.valid_data_end))?;
        {
            let mut writer = std::io::BufWriter::with_capacity(len as usize, &mut self.file);
            writer.write_all(&buffer)?;
            writer.flush()?;
        }

        // Update valid_data_end
        self.valid_data_end = new_end;

        // Update header with new valid_data_end
        self.update_valid_data_end_in_header()?;

        Ok(WriteOutcome {
            entry_len: len,
            value_offset,
            value_len,
        })
    }

    fn read_value(&mut self, offset: u64, len: u32) -> Result<Vec<u8>> {
        let mut buf = vec![0u8; len as usize];
        self.file.seek(SeekFrom::Start(offset))?;
        self.file.read_exact(&mut buf)?;
        Ok(buf)
    }

    fn sync_all(&mut self) -> Result<()> {
        self.file.sync_all().map_err(Error::from)
    }

    fn set_len(&mut self, size: u64) -> Result<()> {
        if size < STORAGE_HEADER_SIZE as u64 {
            return Err(Error::Other(format!(
                "Cannot shrink log smaller than header (requested {size} bytes)"
            )));
        }
        if let Some(limit) = self.max_file_len {
            if size > limit {
                return Err(Error::OutOfStorageQuota { bytes: limit });
            }
        }
        self.file.set_len(size)?;
        if self.valid_data_end > size {
            self.valid_data_end = size;
            self.update_valid_data_end_in_header()?;
        }
        Ok(())
    }

    fn rename_to(&mut self, new_identifier: String) -> Result<()> {
        let new_path = std::path::PathBuf::from(new_identifier);
        std::fs::rename(&self.path, &new_path)?;
        self.path = new_path;
        Ok(())
    }

    fn current_size(&self) -> Result<u64> {
        Ok(self.valid_data_end)
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
        // valid_data_end [21..29) = STORAGE_HEADER_SIZE initially
        header[21..29].copy_from_slice(&self.valid_data_end.to_be_bytes());
        // [29..64) reserved = 0

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

        self.valid_data_end = u64::from_be_bytes([
            header[21], header[22], header[23], header[24], header[25], header[26], header[27],
            header[28],
        ]);

        if let Some(limit) = self.max_file_len {
            if self.valid_data_end > limit {
                return Err(Error::OutOfStorageQuota { bytes: limit });
            }
        }

        Ok(())
    }

    /// Update valid_data_end field in the header
    fn update_valid_data_end_in_header(&mut self) -> Result<()> {
        // Seek to valid_data_end field [21..29)
        self.file.seek(SeekFrom::Start(21))?;
        self.file.write_all(&self.valid_data_end.to_be_bytes())?;
        // Note: We don't sync here to avoid performance impact
        // sync_all() will flush this along with data
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
