//! Simple Ollama chatbot with TegDB - Minimal example
//!
//! A simpler version focusing on the core concepts:
//! 1. Store messages with embeddings using EMBED()
//! 2. Search similar past conversations
//! 3. Generate context-aware responses
//!
//! Prerequisites:
//! - Ollama: `ollama serve`
//! - Model: `ollama pull gemma3:latest`
//!
//! Run: cargo run --example ollama_chatbot_simple --features dev

use serde_json::json;
use std::io::{self, Write};
use tegdb::{embedding, Database, Result, SqlValue};
use tempfile::NamedTempFile;

#[tokio::main]
async fn main() -> Result<()> {
    println!("🤖 Simple TegDB Chatbot\n");

    // Setup database (unique temp file each run)
    let temp_file = NamedTempFile::new().expect("Failed to create temp file");
    let mut pb = std::path::PathBuf::from(temp_file.path());
    pb.set_extension("teg");
    let mut db = Database::open(format!("file://{}", pb.display()))?;

    // Create chat history table
    db.execute(
        "CREATE TABLE chat_history (
            id INTEGER PRIMARY KEY,
            message TEXT(500),
            is_user INTEGER,
            embedding VECTOR(128)
        );",
    )?;

    println!("💬 Type your questions (or 'exit' to quit)\n");

    // Simple incremental id generator since the engine requires explicit ids
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

        // Compute embedding in Rust (avoid SQL function to maximize compatibility)
        let emb_user = embedding::embed(input, embedding::EmbeddingModel::Ollama)?;
        let _emb_user_str = format!(
            "[{}]",
            emb_user
                .iter()
                .map(|v| format!("{v:.6}"))
                .collect::<Vec<_>>()
                .join(", ")
        );

        // Store user message with embedding using prepared statement
        let insert_sql =
            "INSERT INTO chat_history (id, message, is_user, embedding) VALUES (?1, ?2, ?3, ?4);";
        let stmt = db.prepare(insert_sql)?;
        db.execute_prepared(
            &stmt,
            &[
                SqlValue::Integer(next_id),
                SqlValue::Text(input.to_string()),
                SqlValue::Integer(1),
                SqlValue::Vector(emb_user.clone()),
            ],
        )?;
        next_id += 1;

        // Find similar past messages for context
        // Similar messages via prepared SELECT
        let select_sql = "SELECT message FROM chat_history WHERE is_user = ?1 ORDER BY COSINE_SIMILARITY(embedding, ?2) DESC LIMIT 3;";
        let stmt_sel = db.prepare(select_sql)?;
        let result = db.query_prepared(
            &stmt_sel,
            &[SqlValue::Integer(1), SqlValue::Vector(emb_user.clone())],
        )?;

        // Build context from similar messages
        let mut context = String::from("Previous related questions:\n");
        for (i, row) in result.rows().iter().skip(1).take(2).enumerate() {
            if let tegdb::SqlValue::Text(msg) = &row[0] {
                context.push_str(&format!("{}. {}\n", i + 1, msg));
            }
        }

        // Call Ollama
        print!("Bot: ");
        io::stdout().flush().unwrap();

        let response = call_ollama(input, &context).await?;
        println!("{response}\n");

        // Store bot response (reuse user's embedding as a simple proxy)
        // Store bot response using prepared INSERT (reuse same statement text)
        let stmt2 = db.prepare(insert_sql)?;
        db.execute_prepared(
            &stmt2,
            &[
                SqlValue::Integer(next_id),
                SqlValue::Text(response),
                SqlValue::Integer(0),
                SqlValue::Vector(emb_user),
            ],
        )?;
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
