use super::config::Config;
use super::error::{Error, Result};
use super::parser::{find_last_commit_offset, RecordParser};
use super::s3_backend::S3Backend;
use super::state::ReplicationState;
use chrono::Utc;
use flate2::write::GzEncoder;
use flate2::Compression;
use std::fs::OpenOptions;
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::Path;
use std::time::{Duration, Instant};
use tokio::time::sleep;

/// Tailer continuously replicates TegDB files to S3
pub struct Tailer {
    config: Config,
    state: ReplicationState,
    backend: S3Backend,
    state_path: std::path::PathBuf,
}

impl Tailer {
    /// Create new tailer
    pub async fn new(config: Config) -> Result<Self> {
        let state_path = config.state_file_path();
        let state = ReplicationState::load_or_create(&state_path, &config.database_path)?;
        let backend = S3Backend::new(&config.s3).await?;

        Ok(Self {
            config,
            state,
            backend,
            state_path,
        })
    }

    /// Run the tailer loop (continuously monitor and upload)
    pub async fn run(&mut self) -> Result<()> {
        let mut last_base_time = Instant::now();
        let mut last_segment_check = Instant::now();
        let mut last_upload_offset = self.state.last_committed_offset;

        loop {
            // Check for file rotation
            if self.state.check_rotation()? {
                eprintln!("Database file rotated, creating new base snapshot...");
                self.create_base_snapshot().await?;
                last_base_time = Instant::now();
                last_upload_offset = self.state.last_committed_offset;
                continue;
            }

            // Check if we should create a new base snapshot
            let should_create_base = {
                let time_elapsed = last_base_time.elapsed();
                let time_threshold = Duration::from_secs(self.config.base.interval_minutes * 60);
                time_elapsed >= time_threshold
            };

            if should_create_base {
                eprintln!("Creating periodic base snapshot...");
                self.create_base_snapshot().await?;
                last_base_time = Instant::now();
                last_upload_offset = self.state.last_committed_offset;
                continue;
            }

            // Check for new committed data
            let debounce_delay = Duration::from_millis(self.config.segment.debounce_ms);
            if last_segment_check.elapsed() < debounce_delay {
                sleep(Duration::from_millis(100)).await;
                continue;
            }

            // Find last commit offset
            let mut file = OpenOptions::new()
                .read(true)
                .open(&self.config.database_path)?;

            let current_commit_offset = find_last_commit_offset(&mut file)?;

            if current_commit_offset > last_upload_offset {
                let segment_size = current_commit_offset - last_upload_offset;
                if segment_size >= self.config.segment.min_bytes {
                    eprintln!(
                        "Uploading segment: {} bytes (offset {} to {})",
                        segment_size, last_upload_offset, current_commit_offset
                    );
                    self.upload_segment(last_upload_offset, current_commit_offset)
                        .await?;
                    last_upload_offset = current_commit_offset;
                }
            }

            last_segment_check = Instant::now();
            self.state.save(&self.state_path)?;
        }
    }

    /// Create a base snapshot and upload it
    async fn create_base_snapshot(&mut self) -> Result<()> {
        let base_id = Utc::now().format("%Y%m%dT%H%M%S").to_string();
        let s3_key = format!("{}/base/{}.snap", self.config.s3_prefix(), base_id);

        let mut file = OpenOptions::new()
            .read(true)
            .open(&self.config.database_path)?;

        // Get valid_data_end from header (only copy valid data, not preallocated space)
        let valid_data_end = RecordParser::read_valid_data_end(&mut file)?;

        // Copy only valid data to temp location for upload
        let temp_path = self.config.database_path.with_extension("snapshot.tmp");
        let mut temp_file = std::fs::File::create(&temp_path)?;
        file.seek(SeekFrom::Start(0))?;
        let mut limited_reader = file.take(valid_data_end);
        std::io::copy(&mut limited_reader, &mut temp_file)?;
        drop(temp_file);

        // Re-open file for finding commit offset
        let mut file = OpenOptions::new()
            .read(true)
            .open(&self.config.database_path)?;

        // Compress if enabled
        let upload_path = if self.config.gzip {
            let compressed_path = temp_path.with_extension("snapshot.tmp.gz");
            self.compress_file(&temp_path, &compressed_path)?;
            std::fs::remove_file(&temp_path)?;
            compressed_path
        } else {
            temp_path.clone()
        };

        // Get current commit offset before closing file
        file.seek(SeekFrom::Start(0))?;
        let current_commit_offset = find_last_commit_offset(&mut file)?;

        // Upload
        let etag = if self.config.gzip {
            self.backend
                .upload_file(&upload_path, &format!("{}.gz", s3_key))
                .await?
        } else {
            self.backend.upload_file(&upload_path, &s3_key).await?
        };

        std::fs::remove_file(&upload_path)?;

        // Update latest pointer
        self.update_latest_pointer(&base_id, current_commit_offset, &etag)
            .await?;

        // Update state
        self.state
            .update_after_base(base_id.clone(), current_commit_offset);
        self.state.save(&self.state_path)?;

        eprintln!("Base snapshot created: {}", base_id);
        Ok(())
    }

    /// Upload a segment (incremental range)
    async fn upload_segment(&mut self, start_offset: u64, end_offset: u64) -> Result<()> {
        let base_id = self.state.base_id.as_ref().ok_or_else(|| {
            Error::InvalidState("No base_id set. Create base snapshot first.".to_string())
        })?;

        let segment_key = format!(
            "{}/segments/{}/{}-{}.seg",
            self.config.s3_prefix(),
            base_id,
            start_offset,
            end_offset
        );

        // Read segment data
        let mut file = OpenOptions::new()
            .read(true)
            .open(&self.config.database_path)?;

        file.seek(SeekFrom::Start(start_offset))?;
        let segment_size = (end_offset - start_offset) as usize;
        let mut data = vec![0; segment_size];
        file.read_exact(&mut data)?;

        // Compress if enabled
        let upload_data = if self.config.gzip {
            let mut encoder = GzEncoder::new(Vec::new(), Compression::default());
            encoder.write_all(&data)?;
            encoder.finish()?
        } else {
            data
        };

        // Upload
        let content_type = if self.config.gzip {
            Some("application/gzip")
        } else {
            Some("application/octet-stream")
        };

        let etag = if self.config.gzip {
            self.backend
                .upload_data(upload_data, &format!("{}.gz", segment_key), content_type)
                .await?
        } else {
            self.backend
                .upload_data(upload_data, &segment_key, content_type)
                .await?
        };

        // Update state
        self.state.update_after_upload(end_offset, Some(etag));
        self.state.save(&self.state_path)?;

        Ok(())
    }

    /// Update the latest.json pointer
    async fn update_latest_pointer(
        &self,
        base_id: &str,
        last_offset: u64,
        etag: &str,
    ) -> Result<()> {
        use serde_json::json;

        let pointer_data = json!({
            "base_id": base_id,
            "last_offset": last_offset,
            "created_at": Utc::now().to_rfc3339(),
            "checksums": {
                "base_etag": etag
            }
        });

        let key = format!("{}/latest.json", self.config.s3_prefix());
        let json_bytes = serde_json::to_vec(&pointer_data)?;
        self.backend
            .upload_data(json_bytes, &key, Some("application/json"))
            .await?;

        Ok(())
    }

    /// Compress a file using gzip
    fn compress_file(&self, input_path: &Path, output_path: &Path) -> Result<()> {
        let mut input = std::fs::File::open(input_path)?;
        let mut encoder =
            GzEncoder::new(std::fs::File::create(output_path)?, Compression::default());
        std::io::copy(&mut input, &mut encoder)?;
        encoder.finish()?;
        Ok(())
    }

    /// Create a one-off snapshot (manual trigger)
    pub async fn snapshot_once(&mut self) -> Result<()> {
        self.create_base_snapshot().await
    }
}
