use super::config::S3Config;
use super::error::{Error, Result};
use aws_config::Region;
use aws_sdk_s3::Client;
use chrono::Utc;
use std::path::Path;

/// S3 backend for uploading/downloading database snapshots and segments
pub struct S3Backend {
    client: Client,
    bucket: String,
    prefix: String,
}

impl S3Backend {
    /// Create new S3 backend
    #[allow(deprecated)]
    pub async fn new(config: &S3Config) -> Result<Self> {
        let region = Region::new(config.region.clone());
        let aws_config = aws_config::from_env().region(region).load().await;

        let client = Client::new(&aws_config);

        Ok(Self {
            client,
            bucket: config.bucket.clone(),
            prefix: config.prefix.clone(),
        })
    }

    /// Upload a file to S3
    pub async fn upload_file(&self, local_path: &Path, s3_key: &str) -> Result<String> {
        let body = aws_sdk_s3::primitives::ByteStream::from_path(local_path)
            .await
            .map_err(|e| Error::S3(format!("Failed to read file for upload: {}", e)))?;

        let mut request = self
            .client
            .put_object()
            .bucket(&self.bucket)
            .key(s3_key)
            .body(body);

        // Add metadata
        request = request.metadata("uploaded_at", Utc::now().to_rfc3339());

        let output = request
            .send()
            .await
            .map_err(|e| Error::S3(format!("Upload failed: {}", e)))?;

        let etag = output
            .e_tag()
            .map(|s| s.trim_matches('"').to_string())
            .ok_or_else(|| Error::S3("No ETag in upload response".to_string()))?;

        Ok(etag)
    }

    /// Upload data from memory
    pub async fn upload_data(
        &self,
        data: Vec<u8>,
        s3_key: &str,
        content_type: Option<&str>,
    ) -> Result<String> {
        let body = aws_sdk_s3::primitives::ByteStream::from(data);

        let mut request = self
            .client
            .put_object()
            .bucket(&self.bucket)
            .key(s3_key)
            .body(body);

        if let Some(ct) = content_type {
            request = request.content_type(ct);
        }

        let output = request
            .send()
            .await
            .map_err(|e| Error::S3(format!("Upload failed: {}", e)))?;

        let etag = output
            .e_tag()
            .map(|s| s.trim_matches('"').to_string())
            .ok_or_else(|| Error::S3("No ETag in upload response".to_string()))?;

        Ok(etag)
    }

    /// Download file from S3
    pub async fn download_file(&self, s3_key: &str, local_path: &Path) -> Result<()> {
        let output = self
            .client
            .get_object()
            .bucket(&self.bucket)
            .key(s3_key)
            .send()
            .await
            .map_err(|e| Error::S3(format!("Download failed: {}", e)))?;

        let mut data = Vec::new();
        let mut body = output.body;
        #[allow(unused_imports)]
        use futures::StreamExt;
        while let Some(chunk) = body.next().await {
            let chunk = chunk.map_err(|e| Error::S3(format!("Stream error: {}", e)))?;
            data.extend_from_slice(&chunk);
        }

        std::fs::write(local_path, data)?;
        Ok(())
    }

    /// Download file data to memory
    pub async fn download_data(&self, s3_key: &str) -> Result<Vec<u8>> {
        let output = self
            .client
            .get_object()
            .bucket(&self.bucket)
            .key(s3_key)
            .send()
            .await
            .map_err(|e| Error::S3(format!("Download failed: {}", e)))?;

        let mut data = Vec::new();
        let mut body = output.body;
        #[allow(unused_imports)]
        use futures::StreamExt;
        while let Some(chunk) = body.next().await {
            let chunk = chunk.map_err(|e| Error::S3(format!("Stream error: {}", e)))?;
            data.extend_from_slice(&chunk);
        }

        Ok(data)
    }

    /// List objects with prefix
    pub async fn list_objects(&self, prefix: &str) -> Result<Vec<String>> {
        let mut keys = Vec::new();
        let mut continuation_token = None;

        loop {
            let mut request = self
                .client
                .list_objects_v2()
                .bucket(&self.bucket)
                .prefix(prefix);

            if let Some(token) = continuation_token {
                request = request.continuation_token(token);
            }

            let output = request
                .send()
                .await
                .map_err(|e| Error::S3(format!("List failed: {}", e)))?;

            for obj in output.contents() {
                if let Some(key) = obj.key() {
                    keys.push(key.to_string());
                }
            }

            continuation_token = output.next_continuation_token().map(|s| s.to_string());
            if continuation_token.is_none() {
                break;
            }
        }

        Ok(keys)
    }

    /// Delete object
    pub async fn delete_object(&self, s3_key: &str) -> Result<()> {
        self.client
            .delete_object()
            .bucket(&self.bucket)
            .key(s3_key)
            .send()
            .await
            .map_err(|e| Error::S3(format!("Delete failed: {}", e)))?;

        Ok(())
    }

    /// Get full S3 key for a given path component
    pub fn key(&self, component: &str) -> String {
        if self.prefix.is_empty() {
            component.to_string()
        } else {
            format!("{}/{}", self.prefix, component)
        }
    }

    /// Get prefix for listing
    pub fn list_prefix(&self, component: &str) -> String {
        self.key(component)
    }
}
