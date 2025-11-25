use super::error::{Error, Result};
use serde::{Deserialize, Serialize};
use std::path::PathBuf;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Config {
    /// Path to TegDB database file
    pub database_path: PathBuf,

    /// S3 configuration
    #[serde(rename = "s3")]
    pub s3: S3Config,

    /// Retention policy
    #[serde(default)]
    pub retention: RetentionConfig,

    /// Base snapshot creation policy
    #[serde(rename = "base", default)]
    pub base: BaseConfig,

    /// Segment upload policy
    #[serde(rename = "segment", default)]
    pub segment: SegmentConfig,

    /// Enable gzip compression for segments
    #[serde(default = "default_true")]
    pub gzip: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct S3Config {
    /// S3 bucket name
    pub bucket: String,
    /// S3 prefix/namespace (e.g., "dbs/mydb")
    pub prefix: String,
    /// AWS region
    pub region: String,
    /// Custom endpoint URL (e.g., "http://localhost:9000" for MinIO)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub endpoint: Option<String>,
    /// Access key ID (for MinIO or AWS)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub access_key_id: Option<String>,
    /// Secret access key (for MinIO or AWS)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub secret_access_key: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RetentionConfig {
    /// Keep last N base snapshots
    #[serde(default = "default_retention_bases")]
    pub bases: usize,

    /// Maximum total size of segments to keep (bytes)
    #[serde(default = "default_max_segments_bytes")]
    pub max_segments_bytes: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BaseConfig {
    /// Create new base snapshot every N minutes
    #[serde(default = "default_base_interval")]
    pub interval_minutes: u64,

    /// Create new base after N MB of segments
    #[serde(default = "default_base_segment_mb")]
    pub segment_size_mb: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SegmentConfig {
    /// Minimum segment size to upload (bytes)
    #[serde(default = "default_segment_min_bytes")]
    pub min_bytes: u64,

    /// Debounce delay in milliseconds
    #[serde(default = "default_debounce_ms")]
    pub debounce_ms: u64,
}

impl Default for RetentionConfig {
    fn default() -> Self {
        Self {
            bases: default_retention_bases(),
            max_segments_bytes: default_max_segments_bytes(),
        }
    }
}

impl Default for BaseConfig {
    fn default() -> Self {
        Self {
            interval_minutes: default_base_interval(),
            segment_size_mb: default_base_segment_mb(),
        }
    }
}

impl Default for SegmentConfig {
    fn default() -> Self {
        Self {
            min_bytes: default_segment_min_bytes(),
            debounce_ms: default_debounce_ms(),
        }
    }
}

fn default_true() -> bool {
    true
}

fn default_retention_bases() -> usize {
    3
}

fn default_max_segments_bytes() -> u64 {
    100 * 1024 * 1024 * 1024 // 100 GB
}

fn default_base_interval() -> u64 {
    60 // 1 hour
}

fn default_base_segment_mb() -> u64 {
    100 // 100 MB
}

fn default_segment_min_bytes() -> u64 {
    1024 // 1 KB
}

fn default_debounce_ms() -> u64 {
    2000 // 2 seconds
}

impl Config {
    /// Load config from TOML file
    pub fn from_file(path: &std::path::Path) -> Result<Self> {
        let content = std::fs::read_to_string(path)?;
        let config: Config = toml::from_str(&content)
            .map_err(|e| Error::Config(format!("Failed to parse config: {}", e)))?;

        // Validate
        if !config.database_path.exists() {
            return Err(Error::Config(format!(
                "Database file does not exist: {}",
                config.database_path.display()
            )));
        }

        Ok(config)
    }

    /// Get state file path (adjacent to database with .stream.toml suffix)
    pub fn state_file_path(&self) -> PathBuf {
        // Replace .teg extension with .stream.toml, or append .stream.toml
        if let Some(stem) = self.database_path.file_stem() {
            self.database_path
                .parent()
                .unwrap_or_else(|| std::path::Path::new("."))
                .join(format!("{}.stream.toml", stem.to_string_lossy()))
        } else {
            self.database_path.with_extension("stream.toml")
        }
    }

    /// Get S3 key prefix for this database
    pub fn s3_prefix(&self) -> String {
        format!(
            "{}/{}",
            self.s3.prefix,
            self.database_path.file_name().unwrap().to_string_lossy()
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_config_defaults() {
        let config = Config {
            database_path: PathBuf::from("/tmp/test.teg"),
            s3: S3Config {
                bucket: "my-bucket".to_string(),
                prefix: "dbs".to_string(),
                region: "us-east-1".to_string(),
                endpoint: None,
                access_key_id: None,
                secret_access_key: None,
            },
            retention: RetentionConfig::default(),
            base: BaseConfig::default(),
            segment: SegmentConfig::default(),
            gzip: true,
        };

        assert_eq!(config.retention.bases, 3);
        assert_eq!(config.base.interval_minutes, 60);
    }
}
