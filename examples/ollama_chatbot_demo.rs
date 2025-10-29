//! Ollama-powered chatbot with TegDB memory
//!
//! This example demonstrates:
//! 1. Using TegDB's EMBED() function to store conversation history
//! 2. Vector similarity search to find relevant past conversations
//! 3. RAG (Retrieval Augmented Generation) with Ollama's gemma3:latest
//! 4. Context-aware responses using chat history
//!
//! Prerequisites:
//! - Ollama running locally: `ollama serve`
//! - gemma3:latest model: `ollama pull gemma3:latest`
//!
//! Run with: cargo run --example ollama_chatbot_demo --features dev

use serde_json::json;
use std::io::{self, Write};
use tegdb::{Database, Result, SqlValue};

const OLLAMA_API: &str = "http://localhost:11434/api/chat";
const MODEL: &str = "gemma3:latest";

#[tokio::main]
async fn main() -> Result<()> {
    println!("🤖 TegDB + Ollama Chatbot Demo");
    println!("================================\n");

    // Setup database
    let db_path = std::env::temp_dir()
        .join("chatbot_memory")
        .with_extension("teg");
    let _ = std::fs::remove_file(&db_path);
    let mut db = Database::open(db_path.to_string_lossy())?;

    println!("📦 Setting up conversation memory...");
    setup_memory(&mut db)?;

    // Check Ollama connection
    if !check_ollama_connection().await {
        eprintln!("❌ Cannot connect to Ollama at {OLLAMA_API}");
        eprintln!("   Please ensure Ollama is running: ollama serve");
        eprintln!("   And gemma3:latest is installed: ollama pull gemma3:latest");
        return Ok(());
    }

    println!("✅ Connected to Ollama ({MODEL})\n");
    println!("💬 Chat with the bot! (type 'exit' to quit, 'memory' to see history)\n");

    // Chat loop
    let mut conversation_history = Vec::new();
    loop {
        print!("You: ");
        io::stdout().flush().unwrap();

        let mut user_input = String::new();
        io::stdin().read_line(&mut user_input)?;
        let user_input = user_input.trim();

        if user_input.is_empty() {
            continue;
        }

        if user_input.eq_ignore_ascii_case("exit") {
            println!("\n👋 Goodbye! Your conversation has been saved.");
            break;
        }

        if user_input.eq_ignore_ascii_case("memory") {
            show_memory(&mut db)?;
            continue;
        }

        // Store user message with embedding
        store_message(&mut db, "user", user_input)?;
        conversation_history.push(json!({
            "role": "user",
            "content": user_input
        }));

        // Find relevant past conversations for context
        let context = find_relevant_context(&mut db, user_input, 3)?;

        // Generate response with context
        print!("Bot: ");
        io::stdout().flush().unwrap();

        let response =
            generate_response_with_context(&conversation_history, &context, user_input).await?;

        println!("{response}\n");

        // Store bot response
        store_message(&mut db, "assistant", &response)?;
        conversation_history.push(json!({
            "role": "assistant",
            "content": response
        }));

        // Keep conversation history manageable (last 10 messages)
        if conversation_history.len() > 10 {
            let len = conversation_history.len();
            conversation_history.drain(0..(len - 10));
        }
    }

    Ok(())
}

/// Setup the conversation memory database
fn setup_memory(db: &mut Database) -> Result<()> {
    // Create table for storing conversations
    db.execute(
        "CREATE TABLE conversations (
            id INTEGER PRIMARY KEY,
            timestamp INTEGER,
            role TEXT(20),
            message TEXT(1000),
            embedding VECTOR(128)
        )",
    )?;

    // Create vector index for fast similarity search
    db.execute("CREATE INDEX idx_embedding ON conversations USING HNSW (embedding)")?;

    Ok(())
}

/// Store a message with its embedding
fn store_message(db: &mut Database, role: &str, message: &str) -> Result<()> {
    let timestamp = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_secs();

    let sql = format!(
        "INSERT INTO conversations (timestamp, role, message, embedding) 
         VALUES ({}, '{}', '{}', EMBED('{}'))",
        timestamp,
        role,
        message.replace('\'', "''"), // Escape single quotes
        message.replace('\'', "''")
    );

    db.execute(&sql)?;
    Ok(())
}

/// Find relevant past conversations using vector similarity
fn find_relevant_context(db: &mut Database, query: &str, limit: usize) -> Result<Vec<String>> {
    let sql = format!(
        "SELECT message, role, 
                COSINE_SIMILARITY(embedding, EMBED('{}')) as similarity
         FROM conversations
         WHERE role = 'user'
         ORDER BY similarity DESC
         LIMIT {}",
        query.replace('\'', "''"),
        limit + 1 // +1 because current message might be included
    );

    let result = db.query(&sql)?;
    let mut context = Vec::new();

    for (i, row) in result.rows().iter().enumerate() {
        if i == 0 {
            continue; // Skip the most similar (likely the current query)
        }

        if let SqlValue::Text(msg) = &row[0] {
            // Get similarity score
            let similarity = match &row[2] {
                SqlValue::Real(s) => *s,
                _ => 0.0,
            };

            // Only include if similarity is above threshold
            if similarity > 0.3 {
                context.push(format!("Past question: {msg}"));
            }
        }
    }

    Ok(context)
}

/// Generate response using Ollama with context
async fn generate_response_with_context(
    conversation_history: &[serde_json::Value],
    context: &[String],
    _current_query: &str,
) -> Result<String> {
    let mut messages = Vec::new();

    // Add system prompt with context
    let mut system_prompt =
        "You are a helpful assistant with access to conversation history. ".to_string();

    if !context.is_empty() {
        system_prompt.push_str("Here are some relevant past conversations:\n");
        for ctx in context {
            system_prompt.push_str(&format!("- {ctx}\n"));
        }
        system_prompt.push_str("\nUse this context if relevant to the current question.\n");
    }

    messages.push(json!({
        "role": "system",
        "content": system_prompt
    }));

    // Add conversation history (last few messages)
    messages.extend_from_slice(conversation_history);

    // Call Ollama API
    let client = reqwest::Client::new();
    let payload = json!({
        "model": MODEL,
        "messages": messages,
        "stream": false
    });

    let response = client
        .post(OLLAMA_API)
        .json(&payload)
        .send()
        .await
        .map_err(|e| tegdb::Error::Other(format!("Ollama API error: {e}")))?;

    let response_json: serde_json::Value = response
        .json()
        .await
        .map_err(|e| tegdb::Error::Other(format!("Failed to parse response: {e}")))?;

    let content = response_json["message"]["content"]
        .as_str()
        .unwrap_or("I'm sorry, I couldn't generate a response.")
        .to_string();

    Ok(content)
}

/// Check if Ollama is running and accessible
async fn check_ollama_connection() -> bool {
    let client = reqwest::Client::new();
    let payload = json!({
        "model": MODEL,
        "messages": [{"role": "user", "content": "test"}],
        "stream": false
    });

    client
        .post(OLLAMA_API)
        .json(&payload)
        .timeout(std::time::Duration::from_secs(5))
        .send()
        .await
        .is_ok()
}

/// Show conversation memory
fn show_memory(db: &mut Database) -> Result<()> {
    println!("\n📚 Conversation Memory:");
    println!("=====================\n");

    let result = db.query(
        "SELECT role, message, timestamp FROM conversations ORDER BY timestamp DESC LIMIT 10",
    )?;

    if result.rows().is_empty() {
        println!("No conversations stored yet.\n");
        return Ok(());
    }

    for row in result.rows() {
        let role = match &row[0] {
            SqlValue::Text(r) => r,
            _ => "unknown",
        };
        let message = match &row[1] {
            SqlValue::Text(m) => m,
            _ => "",
        };

        let icon = if role == "user" { "👤" } else { "🤖" };
        println!("{icon} {role}: {message}");
    }

    println!();
    Ok(())
}
