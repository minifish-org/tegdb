//! Embedding generation module for TegDB
//!
//! This module provides text-to-vector embedding functionality using Ollama
//! for real semantic embeddings.

use crate::Result;
use std::str::FromStr;

/// Supported embedding models
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EmbeddingModel {
    /// Ollama-based semantic embeddings (real AI models via local Ollama server)
    Ollama,
}

impl FromStr for EmbeddingModel {
    type Err = crate::Error;

    fn from_str(s: &str) -> Result<Self> {
        match s.to_lowercase().as_str() {
            "ollama" => Ok(Self::Ollama),
            other => Err(crate::Error::Other(format!(
                "Unknown embedding model: {other}. Only 'ollama' is supported."
            ))),
        }
    }
}

impl EmbeddingModel {
    /// Get the dimension of embeddings produced by this model
    pub fn dimension(&self) -> usize {
        match self {
            Self::Ollama => 768, // nomic-embed-text produces 768-dimensional vectors
        }
    }
}

/// Generate an embedding vector from text using Ollama
pub fn embed(text: &str, model: EmbeddingModel) -> Result<Vec<f64>> {
    match model {
        EmbeddingModel::Ollama => ollama_embed(text),
    }
}

/// Calculate cosine similarity between two embeddings
pub fn cosine_similarity(a: &[f64], b: &[f64]) -> Result<f64> {
    if a.len() != b.len() {
        return Err(crate::Error::Other(format!(
            "Vector dimension mismatch: {} vs {}",
            a.len(),
            b.len()
        )));
    }

    if a.is_empty() {
        return Ok(0.0);
    }

    let dot_product: f64 = a.iter().zip(b.iter()).map(|(x, y)| x * y).sum();

    let magnitude_a: f64 = a.iter().map(|x| x * x).sum::<f64>().sqrt();
    let magnitude_b: f64 = b.iter().map(|x| x * x).sum::<f64>().sqrt();

    if magnitude_a == 0.0 || magnitude_b == 0.0 {
        return Ok(0.0);
    }

    Ok(dot_product / (magnitude_a * magnitude_b))
}

/// Generate embedding using Ollama's embedding API
/// This uses the local Ollama server with nomic-embed-text model
fn ollama_embed(text: &str) -> Result<Vec<f64>> {
    use std::process::Command;

    // Call Ollama API via curl command
    let output = Command::new("curl")
        .arg("-s")
        .arg("http://localhost:11434/api/embeddings")
        .arg("-d")
        .arg(format!(
            r#"{{"model":"nomic-embed-text","prompt":"{}"}}"#,
            text.replace('"', r#"\""#)
        ))
        .output()
        .map_err(|e| crate::Error::Other(format!("Failed to call Ollama: {}", e)))?;

    if !output.status.success() {
        return Err(crate::Error::Other(format!(
            "Ollama command failed: {}",
            String::from_utf8_lossy(&output.stderr)
        )));
    }

    // Parse JSON response
    let response_text = String::from_utf8(output.stdout)
        .map_err(|e| crate::Error::Other(format!("Invalid UTF-8 from Ollama: {}", e)))?;

    let json: serde_json::Value = serde_json::from_str(&response_text)
        .map_err(|e| crate::Error::Other(format!("Failed to parse Ollama response: {}", e)))?;

    // Extract embedding array
    let embedding_array = json
        .get("embedding")
        .and_then(|v| v.as_array())
        .ok_or_else(|| crate::Error::Other("No embedding in Ollama response".to_string()))?;

    // Convert to Vec<f64>
    let embedding: Vec<f64> = embedding_array.iter().filter_map(|v| v.as_f64()).collect();

    if embedding.len() != 768 {
        return Err(crate::Error::Other(format!(
            "Expected 768-dimensional embedding, got {}",
            embedding.len()
        )));
    }

    Ok(embedding)
}

/// Synchronous version of ollama_embed for use in non-async contexts
pub fn ollama_embed_sync(text: &str) -> Result<Vec<f64>> {
    ollama_embed(text)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_cosine_similarity() {
        let a = vec![1.0, 0.0, 0.0];
        let b = vec![1.0, 0.0, 0.0];
        let similarity = cosine_similarity(&a, &b).unwrap();
        assert!((similarity - 1.0).abs() < 1e-10);

        let c = vec![1.0, 0.0, 0.0];
        let d = vec![0.0, 1.0, 0.0];
        let similarity = cosine_similarity(&c, &d).unwrap();
        assert!((similarity - 0.0).abs() < 1e-10);
    }

    #[test]
    fn test_embedding_model_from_str() {
        assert_eq!(
            "ollama".parse::<EmbeddingModel>().unwrap(),
            EmbeddingModel::Ollama
        );
        assert!("invalid".parse::<EmbeddingModel>().is_err());
    }
}
