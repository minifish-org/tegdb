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
}

impl FromStr for EmbeddingModel {
    type Err = crate::Error;

    fn from_str(s: &str) -> Result<Self> {
        match s.to_lowercase().as_str() {
            "simple" | "default" => Ok(Self::Simple),
            "tinybert" => Ok(Self::TinyBERT),
            "all-minilm" | "minilm" => Ok(Self::AllMiniLM),
            other => Err(crate::Error::Other(format!(
                "Unknown embedding model: {other}. Available models: simple, tinybert, all-minilm"
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
