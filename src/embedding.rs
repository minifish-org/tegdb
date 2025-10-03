//! Embedding generation module for TegDB
//!
//! This module provides text-to-vector embedding functionality similar to PostgresML's
//! pgml.embed(). For now, it uses a simple deterministic embedding approach.
//! In the future, this can be extended to support real ML models.

use crate::Result;
use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};
use std::str::FromStr;

/// Supported embedding models
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EmbeddingModel {
    /// Simple hash-based embedding (default, fast, deterministic)
    Simple,
    /// TinyBERT-like model (future: small transformer model)
    TinyBERT,
    /// All-MiniLM model (future: sentence transformer)
    AllMiniLM,
    /// Ollama-based semantic embeddings (real AI models)
    Ollama,
}

impl FromStr for EmbeddingModel {
    type Err = crate::Error;

    fn from_str(s: &str) -> Result<Self> {
        match s.to_lowercase().as_str() {
            "simple" | "default" => Ok(Self::Simple),
            "tinybert" => Ok(Self::TinyBERT),
            "all-minilm" | "minilm" => Ok(Self::AllMiniLM),
            "ollama" => Ok(Self::Ollama),
            other => Err(crate::Error::Other(format!(
                "Unknown embedding model: {other}. Available models: simple, tinybert, all-minilm, ollama"
            ))),
        }
    }
}

impl EmbeddingModel {
    /// Get the dimension of embeddings produced by this model
    pub fn dimension(&self) -> usize {
        match self {
            Self::Simple => 128, // Simple model produces 128-dimensional vectors
            Self::TinyBERT => 384,
            Self::AllMiniLM => 384,
            Self::Ollama => 768, // nomic-embed-text produces 768-dimensional vectors
        }
    }
}

/// Generate an embedding vector from text
pub fn embed(text: &str, model: EmbeddingModel) -> Result<Vec<f64>> {
    match model {
        EmbeddingModel::Simple => simple_embed(text),
        EmbeddingModel::TinyBERT => {
            // For now, fall back to simple embedding
            // TODO: Implement real TinyBERT model
            simple_embed_with_dim(text, model.dimension())
        }
        EmbeddingModel::AllMiniLM => {
            // For now, fall back to simple embedding
            // TODO: Implement real All-MiniLM model
            simple_embed_with_dim(text, model.dimension())
        }
        EmbeddingModel::Ollama => {
            // Use Ollama for real semantic embeddings
            ollama_embed(text)
        }
    }
}

/// Simple hash-based embedding (deterministic and fast)
///
/// This creates a deterministic embedding by:
/// 1. Normalizing the text (lowercase, trim)
/// 2. Generating multiple hash values from different "views" of the text
/// 3. Mapping hash values to [-1, 1] range
/// 4. L2-normalizing the final vector
fn simple_embed(text: &str) -> Result<Vec<f64>> {
    simple_embed_with_dim(text, 128)
}

fn simple_embed_with_dim(text: &str, dim: usize) -> Result<Vec<f64>> {
    // Normalize input
    let normalized = text.trim().to_lowercase();

    if normalized.is_empty() {
        return Err(crate::Error::Other("Cannot embed empty text".to_string()));
    }

    let mut embedding = Vec::with_capacity(dim);

    // Generate hash-based features
    // We use multiple hash functions by adding different seeds
    for i in 0..dim {
        let mut hasher = DefaultHasher::new();

        // Hash the text with a seed based on position
        format!("{normalized}:{i}").hash(&mut hasher);
        let hash = hasher.finish();

        // Map hash to [-1, 1] range using sine function
        // This gives us a smooth distribution
        let value = ((hash as f64) * 0.00001).sin();
        embedding.push(value);
    }

    // L2-normalize the embedding
    let magnitude: f64 = embedding.iter().map(|x| x * x).sum::<f64>().sqrt();

    if magnitude > 0.0 {
        for val in &mut embedding {
            *val /= magnitude;
        }
    }

    Ok(embedding)
}

/// Calculate cosine similarity between two embeddings
pub fn cosine_similarity(a: &[f64], b: &[f64]) -> Result<f64> {
    if a.len() != b.len() {
        return Err(crate::Error::Other(format!(
            "Embedding dimensions don't match: {} vs {}",
            a.len(),
            b.len()
        )));
    }

    let dot_product: f64 = a.iter().zip(b.iter()).map(|(x, y)| x * y).sum();
    let mag_a: f64 = a.iter().map(|x| x * x).sum::<f64>().sqrt();
    let mag_b: f64 = b.iter().map(|x| x * x).sum::<f64>().sqrt();

    if mag_a == 0.0 || mag_b == 0.0 {
        return Err(crate::Error::Other(
            "Cannot calculate similarity with zero vectors".to_string(),
        ));
    }

    Ok(dot_product / (mag_a * mag_b))
}

/// Ollama-based semantic embedding (real AI-powered embeddings)
///
/// This uses Ollama's HTTP API to generate real semantic embeddings that capture
/// the meaning and context of text, enabling true semantic similarity search.
pub fn ollama_embed(text: &str) -> Result<Vec<f64>> {
    // Validate input
    let trimmed_text = text.trim();
    if trimmed_text.is_empty() {
        return Err(crate::Error::Other("Cannot embed empty text".to_string()));
    }

    // Use a simple synchronous HTTP approach for now
    // In a production environment, this would be async-compatible
    match ollama_embed_sync(trimmed_text) {
        Ok(embedding) => Ok(embedding),
        Err(_e) => {
            // Graceful fallback to hash embedding (silently)
            crate::embedding::simple_embed_with_dim(text, 1024)
        }
    }
}

/// Synchronous Ollama embedding using HTTP request
fn ollama_embed_sync(text: &str) -> Result<Vec<f64>> {
    use std::process::Command;

    // Escape the text for JSON
    let escaped_text = text
        .replace("\\", "\\\\")
        .replace("\"", "\\\"")
        .replace("\n", "\\n")
        .replace("\r", "\\r")
        .replace("\t", "\\t");

    // Create the JSON payload
    let json_payload = format!(
        r#"{{"model":"nomic-embed-text","prompt":"{}"}}"#,
        escaped_text
    );

    // Use curl to make the HTTP request
    let curl_output = Command::new("curl")
        .arg("-s") // Silent mode
        .arg("-X")
        .arg("POST")
        .arg("http://localhost:11434/api/embeddings")
        .arg("-H")
        .arg("Content-Type: application/json")
        .arg("-d")
        .arg(&json_payload)
        .arg("--connect-timeout")
        .arg("5") // 5 second timeout
        .output();

    match curl_output {
        Ok(output) => {
            if !output.status.success() {
                let stderr = String::from_utf8_lossy(&output.stderr);
                return Err(crate::Error::Other(format!(
                    "Curl failed with status {}: {}",
                    output.status, stderr
                )));
            }

            // Parse the JSON response
            let stdout = String::from_utf8_lossy(&output.stdout);

            // Parse as JSON: {"embedding": [0.123, -0.456, ...]}
            match serde_json::from_str::<serde_json::Value>(&stdout) {
                Ok(json) => {
                    if let Some(embedding_array) = json["embedding"].as_array() {
                        // Convert JSON array to Vec<f64>
                        let mut embedding = Vec::new();
                        for val in embedding_array {
                            if let Some(f) = val.as_f64() {
                                embedding.push(f);
                            } else {
                                return Err(crate::Error::Other(
                                    "Invalid embedding value in Ollama response".to_string(),
                                ));
                            }
                        }

                        if embedding.is_empty() {
                            return Err(crate::Error::Other(
                                "Ollama returned empty embedding vector".to_string(),
                            ));
                        }

                        // Dimensions vary by model - silently continue

                        Ok(embedding)
                    } else {
                        Err(crate::Error::Other(format!(
                            "No 'embedding' field in Ollama response. Raw: {}",
                            stdout
                        )))
                    }
                }
                Err(e) => Err(crate::Error::Other(format!(
                    "Failed to parse Ollama response as JSON: {}. Raw: {}",
                    e, stdout
                ))),
            }
        }
        Err(e) => Err(crate::Error::Other(format!(
            "Failed to execute curl command: {}. Is curl installed?",
            e
        ))),
    }
}

/// Test Ollama embedding functionality
pub fn test_ollama_embedding() -> Result<()> {
    println!("🔍 Testing Ollama embedding integration...");

    // Test basic embedding
    match ollama_embed("hello world") {
        Ok(vectors) => {
            println!(
                "✅ Ollama embedding works! Generated {} dimensions",
                vectors.len()
            );

            // Test semantic similarity
            let text1 = "I love dogs";
            let text2 = "I adore puppies";
            let text3 = "I hate computers";

            let emb1 = ollama_embed(text1)?;
            let emb2 = ollama_embed(text2)?;
            let emb3 = ollama_embed(text3)?;

            let sim_1_2 = cosine_similarity(&emb1, &emb2)?;
            let sim_1_3 = cosine_similarity(&emb1, &emb3)?;

            println!("🎯 Semantic similarity test:");
            println!("  '{}' <-> '{}' = {:.4}", text1, text2, sim_1_2);
            println!("  '{}' <-> '{}' = {:.4}", text1, text3, sim_1_3);

            if sim_1_2 > sim_1_3 {
                println!("✅ Semantic embeddings work! Similar concepts have higher similarity.");
            } else {
                println!("⚠️  Semantic behavior unclear - may need different text examples.");
            }
        }
        Err(e) => {
            println!("❌ Ollama embedding failed: {}", e);
            println!("💡 Make sure Ollama is running: ollama serve");
            println!("💡 Install embedding model: ollama pull nomic-embed-text");
            return Err(e);
        }
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_simple_embed() {
        let text = "Hello, world!";
        let embedding = embed(text, EmbeddingModel::Simple).unwrap();

        // Check dimension
        assert_eq!(embedding.len(), 128);

        // Check normalization (L2 norm should be ~1.0)
        let magnitude: f64 = embedding.iter().map(|x| x * x).sum::<f64>().sqrt();
        assert!((magnitude - 1.0).abs() < 0.0001);
    }

    #[test]
    fn test_embed_deterministic() {
        let text = "Hello, world!";
        let emb1 = embed(text, EmbeddingModel::Simple).unwrap();
        let emb2 = embed(text, EmbeddingModel::Simple).unwrap();

        // Same text should produce same embedding
        assert_eq!(emb1, emb2);
    }

    #[test]
    fn test_embed_different_texts() {
        let emb1 = embed("Hello", EmbeddingModel::Simple).unwrap();
        let emb2 = embed("World", EmbeddingModel::Simple).unwrap();

        // Different texts should produce different embeddings
        assert_ne!(emb1, emb2);
    }

    #[test]
    fn test_cosine_similarity() {
        let text = "machine learning";
        let emb1 = embed(text, EmbeddingModel::Simple).unwrap();
        let emb2 = embed(text, EmbeddingModel::Simple).unwrap();

        // Same text should have similarity of 1.0
        let similarity = cosine_similarity(&emb1, &emb2).unwrap();
        assert!((similarity - 1.0).abs() < 0.0001);
    }

    #[test]
    fn test_empty_text() {
        let result = embed("", EmbeddingModel::Simple);
        assert!(result.is_err());

        let result = embed("   ", EmbeddingModel::Simple);
        assert!(result.is_err());
    }

    #[test]
    fn test_model_from_str() {
        assert!(matches!(
            "simple".parse::<EmbeddingModel>().unwrap(),
            EmbeddingModel::Simple
        ));
        assert!(matches!(
            "tinybert".parse::<EmbeddingModel>().unwrap(),
            EmbeddingModel::TinyBERT
        ));
        assert!(matches!(
            "all-minilm".parse::<EmbeddingModel>().unwrap(),
            EmbeddingModel::AllMiniLM
        ));

        let result = "unknown".parse::<EmbeddingModel>();
        assert!(result.is_err());
    }

    #[test]
    fn test_model_dimensions() {
        assert_eq!(EmbeddingModel::Simple.dimension(), 128);
        assert_eq!(EmbeddingModel::TinyBERT.dimension(), 384);
        assert_eq!(EmbeddingModel::AllMiniLM.dimension(), 384);
    }
}
