use super::error::{Error, Result};
use serde::{Deserialize, Serialize};
use std::fs;
use std::path::{Path, PathBuf};

/// Replication state tracking file metadata and progress
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ReplicationState {
    /// Device ID (for detecting file replacement)
    pub device: u64,
    /// Inode number (for detecting file replacement)
    pub inode: u64,
    /// Absolute path to database file
    pub db_path: PathBuf,
    /// Base snapshot ID (timestamp string)
    pub base_id: Option<String>,
    /// Last committed offset (safe to upload up to here)
    pub last_committed_offset: u64,
    /// ETag of last uploaded segment (for validation)
    pub last_uploaded_etag: Option<String>,
    /// Last seen file size
    pub last_seen_size: u64,
}

impl ReplicationState {
    /// Load state from file, or create new if missing
    pub fn load_or_create(state_path: &Path, db_path: &Path) -> Result<Self> {
        if state_path.exists() {
            let content = fs::read_to_string(state_path)?;
            let state: ReplicationState = toml::from_str(&content)
                .map_err(|e| Error::InvalidState(format!("Failed to parse state file: {}", e)))?;

            // Validate that the state matches the current file
            let current_meta = Self::get_file_metadata(db_path)?;
            if state.device != current_meta.0 || state.inode != current_meta.1 {
                // File was replaced (compaction or rotation)
                return Ok(Self::new(db_path));
            }

            Ok(state)
        } else {
            Ok(Self::new(db_path))
        }
    }

    /// Create new state from database file
    pub fn new(db_path: &Path) -> Self {
        let (device, inode) = Self::get_file_metadata(db_path).unwrap_or((0, 0));
        let last_seen_size = fs::metadata(db_path).map(|m| m.len()).unwrap_or(0);

        Self {
            device,
            inode,
            db_path: db_path.to_path_buf(),
            base_id: None,
            last_committed_offset: 64, // STORAGE_HEADER_SIZE
            last_uploaded_etag: None,
            last_seen_size,
        }
    }

    /// Get device and inode for a file
    #[cfg(unix)]
    fn get_file_metadata(path: &Path) -> Result<(u64, u64)> {
        use std::os::unix::fs::MetadataExt;
        let meta = fs::metadata(path)?;
        Ok((meta.dev(), meta.ino()))
    }

    #[cfg(not(unix))]
    fn get_file_metadata(path: &Path) -> Result<(u64, u64)> {
        // On non-UNIX systems, use file path and modified time as identifiers
        // This is less reliable but works for basic rotation detection
        let meta = fs::metadata(path)?;
        let modified = meta.modified()?;
        let mtime = modified
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();
        // Use a hash of path + mtime as "device", mtime as "inode"
        use std::hash::{Hash, Hasher};
        let mut hasher = std::collections::hash_map::DefaultHasher::new();
        path.hash(&mut hasher);
        mtime.hash(&mut hasher);
        Ok((hasher.finish(), mtime))
    }

    /// Save state to file atomically
    pub fn save(&self, state_path: &Path) -> Result<()> {
        let content = toml::to_string_pretty(self)
            .map_err(|e| Error::InvalidState(format!("Failed to serialize state: {}", e)))?;

        // Atomic write: write to temp file, then rename
        let temp_path = state_path.with_extension("tmp");
        fs::write(&temp_path, content)?;
        fs::rename(&temp_path, state_path)?;

        Ok(())
    }

    /// Check if file has been rotated (compacted or replaced)
    pub fn check_rotation(&mut self) -> Result<bool> {
        let (device, inode) = Self::get_file_metadata(&self.db_path)?;
        let current_size = fs::metadata(&self.db_path)?.len();

        if device != self.device || inode != self.inode || current_size < self.last_seen_size {
            // File was rotated - reset state
            self.device = device;
            self.inode = inode;
            self.base_id = None;
            self.last_committed_offset = 64; // STORAGE_HEADER_SIZE
            self.last_uploaded_etag = None;
            self.last_seen_size = current_size;
            return Ok(true);
        }

        self.last_seen_size = current_size;
        Ok(false)
    }

    /// Update state after successful segment upload
    pub fn update_after_upload(&mut self, offset: u64, etag: Option<String>) {
        self.last_committed_offset = offset;
        self.last_uploaded_etag = etag;
    }

    /// Update state after base snapshot creation
    pub fn update_after_base(&mut self, base_id: String, offset: u64) {
        self.base_id = Some(base_id);
        self.last_committed_offset = offset;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_state_serialization() {
        let db_path = PathBuf::from("/tmp/test.teg");
        let state = ReplicationState::new(&db_path);
        let state_str = toml::to_string(&state).unwrap();
        let _parsed: ReplicationState = toml::from_str(&state_str).unwrap();
    }
}
