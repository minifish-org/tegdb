use super::error::{Error, Result};
use crate::log::{
    LENGTH_FIELD_BYTES, STORAGE_FORMAT_VERSION, STORAGE_HEADER_SIZE, STORAGE_MAGIC,
    TX_COMMIT_MARKER,
};

/// Maximum key and value sizes to prevent parsing huge records
const MAX_KEY_SIZE: usize = 1024 * 10; // 10KB
const MAX_VALUE_SIZE: usize = 256 * 1024; // 256KB

/// Result of parsing a record from the log
#[derive(Debug, Clone)]
pub enum Record {
    /// A normal key-value entry
    Entry { key: Vec<u8>, value: Vec<u8> },
    /// A transaction commit marker
    Commit,
}

/// Parser for TegDB log format records
pub struct RecordParser;

impl RecordParser {
    pub fn new() -> Self {
        Self
    }

    /// Find the offset of the last commit marker in the file
    /// Returns the byte offset immediately after the commit marker value
    pub fn find_last_commit_offset(file: &mut std::fs::File) -> Result<u64> {
        use std::io::{Read, Seek, SeekFrom};

        let file_size = file.metadata()?.len();
        if file_size < STORAGE_HEADER_SIZE as u64 {
            return Ok(STORAGE_HEADER_SIZE as u64);
        }

        // Read header to get valid_data_end
        let scan_end = Self::read_valid_data_end(file)?;

        file.seek(SeekFrom::Start(STORAGE_HEADER_SIZE as u64))?;

        let mut pos = STORAGE_HEADER_SIZE as u64;
        let mut len_buf = [0u8; LENGTH_FIELD_BYTES];
        let mut last_commit_offset = STORAGE_HEADER_SIZE as u64;

        while pos < scan_end {
            // Read key length
            if file.read_exact(&mut len_buf).is_err() {
                break; // End of valid data
            }
            let key_len = u32::from_be_bytes(len_buf) as u64;
            pos += LENGTH_FIELD_BYTES as u64;

            if key_len > MAX_KEY_SIZE as u64 {
                break; // Invalid
            }

            // Read value length
            if file.read_exact(&mut len_buf).is_err() {
                break;
            }
            let value_len = u32::from_be_bytes(len_buf) as u64;
            pos += LENGTH_FIELD_BYTES as u64;

            if value_len > MAX_VALUE_SIZE as u64 {
                break;
            }

            // Read key
            let mut key = vec![0; key_len as usize];
            if file.read_exact(&mut key).is_err() {
                break;
            }
            pos += key_len;

            // Check if this is a commit marker
            if key == TX_COMMIT_MARKER {
                // Skip value and record this as last commit
                file.seek(SeekFrom::Current(value_len as i64))?;
                pos += value_len;
                last_commit_offset = pos;
            } else {
                // Skip value
                file.seek(SeekFrom::Current(value_len as i64))?;
                pos += value_len;
            }
        }

        Ok(last_commit_offset)
    }

    /// Parse a single record starting at the given offset
    /// Returns (Record, bytes_consumed)
    pub fn parse_record(
        file: &mut std::fs::File,
        start_offset: u64,
    ) -> Result<Option<(Record, usize)>> {
        use std::io::{Read, Seek, SeekFrom};

        file.seek(SeekFrom::Start(start_offset))?;

        let mut len_buf = [0u8; LENGTH_FIELD_BYTES];

        // Try to read key length
        if file.read_exact(&mut len_buf).is_err() {
            return Ok(None); // End of file or incomplete record
        }
        let key_len = u32::from_be_bytes(len_buf) as usize;

        if key_len > MAX_KEY_SIZE {
            return Err(Error::Parse(format!(
                "Key length {} exceeds maximum {}",
                key_len, MAX_KEY_SIZE
            )));
        }

        // Read value length
        if file.read_exact(&mut len_buf).is_err() {
            return Ok(None); // Incomplete record
        }
        let value_len = u32::from_be_bytes(len_buf) as usize;

        if value_len > MAX_VALUE_SIZE {
            return Err(Error::Parse(format!(
                "Value length {} exceeds maximum {}",
                value_len, MAX_VALUE_SIZE
            )));
        }

        // Read key
        let mut key = vec![0; key_len];
        if file.read_exact(&mut key).is_err() {
            return Ok(None); // Incomplete
        }

        // Check for commit marker
        if key == TX_COMMIT_MARKER {
            // Skip value (usually empty for commit marker)
            file.seek(SeekFrom::Current(value_len as i64))?;
            let bytes_consumed = LENGTH_FIELD_BYTES * 2 + key_len + value_len;
            return Ok(Some((Record::Commit, bytes_consumed)));
        }

        // Read value
        let mut value = vec![0; value_len];
        if file.read_exact(&mut value).is_err() {
            return Ok(None); // Incomplete
        }

        let bytes_consumed = LENGTH_FIELD_BYTES * 2 + key_len + value_len;
        Ok(Some((Record::Entry { key, value }, bytes_consumed)))
    }

    /// Validate that a byte range contains complete, valid records
    /// Returns the offset of the last complete record
    pub fn validate_range(
        file: &mut std::fs::File,
        start_offset: u64,
        end_offset: u64,
    ) -> Result<u64> {
        let mut pos = start_offset;

        while pos < end_offset {
            match Self::parse_record(file, pos)? {
                Some((_, bytes_consumed)) => {
                    pos += bytes_consumed as u64;
                    if pos > end_offset {
                        // This record extends past end_offset, so it's incomplete
                        return Ok(pos - bytes_consumed as u64);
                    }
                }
                None => {
                    // Incomplete record at pos
                    return Ok(pos);
                }
            }
        }

        Ok(end_offset)
    }

    /// Read valid_data_end from header (current storage format only)
    pub fn read_valid_data_end(file: &mut std::fs::File) -> Result<u64> {
        use std::io::{Read, Seek, SeekFrom};

        file.seek(SeekFrom::Start(0))?;
        let mut header = vec![0u8; STORAGE_HEADER_SIZE];
        file.read_exact(&mut header)?;

        // Check magic
        if &header[0..STORAGE_MAGIC.len()] != STORAGE_MAGIC {
            return Err(Error::Parse("Invalid magic in header".to_string()));
        }

        // Read version
        let version = u16::from_be_bytes([header[6], header[7]]);

        if version != STORAGE_FORMAT_VERSION {
            return Err(Error::Parse(format!(
                "Unsupported storage version {} (expected {})",
                version, STORAGE_FORMAT_VERSION
            )));
        }

        let valid_data_end = u64::from_be_bytes([
            header[21], header[22], header[23], header[24], header[25], header[26], header[27],
            header[28],
        ]);
        Ok(valid_data_end)
    }
}

impl Default for RecordParser {
    fn default() -> Self {
        Self::new()
    }
}

/// Parse a record from a buffer instead of file
pub fn parse_record(
    file: &mut std::fs::File,
    start_offset: u64,
) -> Result<Option<(Record, usize)>> {
    RecordParser::parse_record(file, start_offset)
}

/// Find the last commit marker offset in a file
pub fn find_last_commit_offset(file: &mut std::fs::File) -> Result<u64> {
    RecordParser::find_last_commit_offset(file)
}
