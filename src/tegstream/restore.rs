use super::config::S3Config;
use super::error::{Error, Result};
use super::s3_backend::S3Backend;
use flate2::read::GzDecoder;
use serde_json::Value;
use std::io::{Read, Write};
use std::path::Path;

/// Restore a database from S3 snapshots and segments
pub struct Restore {
    backend: S3Backend,
    prefix: String,
}

#[derive(Debug)]
struct LatestPointer {
    base_id: String,
    last_offset: u64,
    #[allow(dead_code)]
    created_at: String,
}

impl Restore {
    /// Create new restore instance
    pub async fn new(s3_config: &S3Config, prefix: &str) -> Result<Self> {
        let backend = S3Backend::new(s3_config).await?;
        Ok(Self {
            backend,
            prefix: prefix.to_string(),
        })
    }

    /// Restore database to a local path
    pub async fn restore_to(&self, output_path: &Path, at_offset: Option<u64>) -> Result<()> {
        // Get latest pointer
        let pointer = self.load_latest_pointer().await?;
        let target_offset = at_offset.unwrap_or(pointer.last_offset);

        eprintln!(
            "Restoring from base: {}, target offset: {}",
            pointer.base_id, target_offset
        );

        // Download base snapshot
        let base_key = format!("{}/base/{}.snap", self.prefix, pointer.base_id);
        let base_key_gz = format!("{}.gz", base_key);

        let base_data = if self
            .backend
            .list_objects(&base_key_gz)
            .await?
            .contains(&base_key_gz)
        {
            self.download_and_decompress(&base_key_gz).await?
        } else {
            self.backend.download_data(&base_key).await?
        };

        // Write base to output
        let mut output_file = std::fs::File::create(output_path)?;
        output_file.write_all(&base_data)?;

        // Download and apply segments up to target offset
        if target_offset > base_data.len() as u64 {
            self.apply_segments(
                &mut output_file,
                &pointer.base_id,
                base_data.len() as u64,
                target_offset,
            )
            .await?;
        }

        output_file.sync_all()?;
        eprintln!("Restore complete: {}", output_path.display());
        Ok(())
    }

    /// Load latest.json pointer
    async fn load_latest_pointer(&self) -> Result<LatestPointer> {
        let key = format!("{}/latest.json", self.prefix);
        let data = self.backend.download_data(&key).await?;
        let json: Value = serde_json::from_slice(&data)
            .map_err(|e| Error::Restore(format!("Failed to parse latest.json: {}", e)))?;

        Ok(LatestPointer {
            base_id: json["base_id"]
                .as_str()
                .ok_or_else(|| Error::Restore("Missing base_id in latest.json".to_string()))?
                .to_string(),
            last_offset: json["last_offset"]
                .as_u64()
                .ok_or_else(|| Error::Restore("Missing last_offset in latest.json".to_string()))?,
            created_at: json["created_at"].as_str().unwrap_or("").to_string(),
        })
    }

    /// Download and decompress a gzipped file
    async fn download_and_decompress(&self, key: &str) -> Result<Vec<u8>> {
        let compressed = self.backend.download_data(key).await?;
        let mut decoder = GzDecoder::new(&compressed[..]);
        let mut decompressed = Vec::new();
        decoder.read_to_end(&mut decompressed)?;
        Ok(decompressed)
    }

    /// Apply segments to restore database
    async fn apply_segments(
        &self,
        output_file: &mut std::fs::File,
        base_id: &str,
        start_offset: u64,
        target_offset: u64,
    ) -> Result<()> {
        let segment_prefix = format!("{}/segments/{}/", self.prefix, base_id);
        let mut segment_keys = self.backend.list_objects(&segment_prefix).await?;

        // Filter and sort segments
        segment_keys.retain(|k| {
            if let Some(stripped) = k.strip_suffix(".gz") {
                stripped.starts_with(&segment_prefix) && stripped.ends_with(".seg")
            } else {
                k.starts_with(&segment_prefix) && k.ends_with(".seg")
            }
        });

        // Parse segment ranges and sort by start offset
        let mut segments: Vec<(u64, u64, String)> = segment_keys
            .iter()
            .filter_map(|k| {
                // Extract offset range from filename like "prefix/segments/base/123-456.seg" or ".gz"
                let name = k.strip_suffix(".gz").unwrap_or(k);
                let name = name.strip_prefix(&segment_prefix)?;
                let name = name.strip_suffix(".seg")?;
                let parts: Vec<&str> = name.split('-').collect();
                if parts.len() == 2 {
                    let start = parts[0].parse::<u64>().ok()?;
                    let end = parts[1].parse::<u64>().ok()?;
                    if start >= start_offset && end <= target_offset {
                        Some((start, end, k.clone()))
                    } else {
                        None
                    }
                } else {
                    None
                }
            })
            .collect();

        segments.sort_by_key(|(start, _, _)| *start);

        // Apply segments in order
        let mut current_offset = start_offset;
        for (seg_start, seg_end, seg_key) in segments {
            if seg_start != current_offset {
                return Err(Error::Restore(format!(
                    "Segment gap: expected offset {}, got {}",
                    current_offset, seg_start
                )));
            }

            eprintln!(
                "Applying segment: {} (offset {} to {})",
                seg_key, seg_start, seg_end
            );

            let segment_data = if seg_key.ends_with(".gz") {
                self.download_and_decompress(&seg_key).await?
            } else {
                self.backend.download_data(&seg_key).await?
            };

            // Verify segment size matches range
            if segment_data.len() as u64 != (seg_end - seg_start) {
                return Err(Error::Restore(format!(
                    "Segment size mismatch: expected {}, got {}",
                    seg_end - seg_start,
                    segment_data.len()
                )));
            }

            output_file.write_all(&segment_data)?;
            current_offset = seg_end;

            if current_offset >= target_offset {
                break;
            }
        }

        if current_offset < target_offset {
            eprintln!(
                "Warning: Restored to offset {}, but target was {}",
                current_offset, target_offset
            );
        }

        Ok(())
    }

    /// List available bases and segments
    pub async fn list(&self) -> Result<()> {
        let base_prefix = format!("{}/base/", self.prefix);
        let bases = self.backend.list_objects(&base_prefix).await?;

        println!("Base snapshots:");
        for base in bases {
            if base.ends_with(".snap") || base.ends_with(".snap.gz") {
                println!("  {}", base);
            }
        }

        let segment_prefix = format!("{}/segments/", self.prefix);
        let segments = self.backend.list_objects(&segment_prefix).await?;

        println!("\nSegments: {}", segments.len());
        for seg in segments.iter().take(10) {
            println!("  {}", seg);
        }
        if segments.len() > 10 {
            println!("  ... ({} more)", segments.len() - 10);
        }

        Ok(())
    }
}
