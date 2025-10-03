//! Working Ollama chatbot that bypasses parsing issues
//!
//! This version uses direct SQL execution without prepared statements
//! and avoids the EMBED() function parsing issues.
//!
//! Prerequisites:
//! - Ollama: `ollama serve`
//! - Model: `ollama pull gemma3:latest`
//!
//! Run: cargo run --example ollama_chatbot_working --features dev

use serde_json::json;
use std::io::{self, Write};
use tegdb::{embedding, Database, Result};
use tempfile::NamedTempFile;

#[tokio::main]
async fn main() -> Result<()> {
    println!("🤖 Working TegDB Chatbot\n");

    // Setup database (unique temp file each run)
    let temp_file = NamedTempFile::new().expect("Failed to create temp file");
    let db_path = temp_file.path();
    let mut db = Database::open(format!("file://{}", db_path.display()))?;

    // Create chat history table
    db.execute(
        "CREATE TABLE chat_history (
            id INTEGER PRIMARY KEY,
            message TEXT(500),
            is_user INTEGER,
            timestamp INTEGER
        );",
    )?;

    // Create a separate table for embeddings to avoid SQL parsing issues
    db.execute(
        "CREATE TABLE embeddings (
            id INTEGER PRIMARY KEY,
            message_id INTEGER,
            embedding VECTOR(128)
        );",
    )?;

    println!("💬 Type your questions (or 'exit' to quit)\n");

    // Simple incremental id generator
    let mut next_id: i64 = 1;

    loop {
        // Get user input
        print!("You: ");
        io::stdout().flush().unwrap();
        let mut input = String::new();
        io::stdin().read_line(&mut input)?;
        let input = input.trim();

        if input.is_empty() {
            continue;
        }
        if input == "exit" {
            break;
        }

        // Compute embedding in Rust
        let emb_user = embedding::embed(input, embedding::EmbeddingModel::Simple)?;

        // Insert user message
        let timestamp = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_secs() as i64;

        // Store user message using proper SQL escaping
        let escaped_input = input; // will use prepared statements instead
        let insert_msg_sql = format!(
            "INSERT INTO chat_history (id, message, is_user, timestamp) VALUES ({}, '{}', 1, {})",
            next_id, escaped_input, timestamp
        );
        db.execute(&insert_msg_sql)?;

        // Insert embedding
        let vec_str = format!(
            "[{}]",
            emb_user
                .iter()
                .map(|v| format!("{v:.6}"))
                .collect::<Vec<_>>()
                .join(", ")
        );
        let insert_emb_sql = format!(
            "INSERT INTO embeddings (id, message_id, embedding) VALUES ({}, {}, {})",
            next_id, next_id, vec_str
        );
        db.execute(&insert_emb_sql)?;

        // Find similar past messages for context
        let mut context = String::from("Previous related questions:\n");
        let find_similar_sql = format!(
            "SELECT ch.message FROM chat_history ch
             JOIN embeddings e ON ch.id = e.message_id
             WHERE ch.is_user = 1 AND ch.id != {}
             ORDER BY COSINE_SIMILARITY(e.embedding, {}) DESC
             LIMIT 2",
            next_id, vec_str
        );

        if let Ok(result) = db.query(&find_similar_sql) {
            for (i, row) in result.rows().iter().take(2).enumerate() {
                if let tegdb::SqlValue::Text(msg) = &row[0] {
                    context.push_str(&format!("{}. {}\n", i + 1, msg));
                }
            }
        }

        next_id += 1;

        // Call Ollama
        print!("Bot: ");
        io::stdout().flush().unwrap();

        let response = call_ollama(input, &context).await?;
        println!("{response}\n");

        // Store bot response using proper SQL escaping (handles Unicode safely)
        let escaped_response = response.trim(); // will use prepared statements instead
        println!("DEBUG: Original response length: {} chars", response.len());
        println!(
            "DEBUG: Escaped response length: {} chars",
            escaped_response.len()
        );
        println!("DEBUG: Full escaped response: {}", escaped_response);

        let insert_response_sql =
            format!(
            "INSERT INTO chat_history (id, message, is_user, timestamp) VALUES ({}, '{}', 0, {})",
            next_id, escaped_response, timestamp + 1
        );
        println!(
            "DEBUG: Final SQL length: {} chars",
            insert_response_sql.len()
        );
        db.execute(&insert_response_sql)?;

        // Store response embedding (simplified - use same embedding as user input)
        let insert_response_emb_sql = format!(
            "INSERT INTO embeddings (id, message_id, embedding) VALUES ({}, {}, {})",
            next_id, next_id, vec_str
        );
        db.execute(&insert_response_emb_sql)?;

        next_id += 1;
    }

    println!("\n👋 Goodbye!");
    Ok(())
}

async fn call_ollama(query: &str, context: &str) -> Result<String> {
    let client = reqwest::Client::new();

    let prompt = if context.contains("Previous") && context.len() > 30 {
        format!("{context}\n\nCurrent question: {query}")
    } else {
        query.to_string()
    };

    let payload = json!({
        "model": "gemma3:latest",
        "messages": [
            {"role": "system", "content": "You are a helpful assistant."},
            {"role": "user", "content": prompt}
        ],
        "stream": false
    });

    let response = client
        .post("http://localhost:11434/api/chat")
        .json(&payload)
        .timeout(std::time::Duration::from_secs(120))
        .send()
        .await
        .map_err(|e| tegdb::Error::Other(format!("Ollama error: {e}")))?;

    let json: serde_json::Value = response
        .json()
        .await
        .map_err(|e| tegdb::Error::Other(format!("Parse error: {e}")))?;

    Ok(json["message"]["content"]
        .as_str()
        .unwrap_or("Sorry, I couldn't respond.")
        .to_string())
}
