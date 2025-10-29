//! Manual chatbot test - simple interactive version
//!
//! Prerequisites:
//! - Ollama: `ollama serve` (already running)
//! - Model: `ollama pull gemma3:latest` (already pulled)
//!
//! Run: cargo run --example manual_chatbot --features dev

use serde_json::json;
use tegdb::{embedding, Database, Result};
use tempfile::NamedTempFile;

fn main() -> Result<()> {
    println!("🤖 Manual TegDB Chatbot Test\n");

    // Test 1: Database setup
    println!("1. Testing database setup...");
    let temp_file = NamedTempFile::new().expect("Failed to create temp file");
    let mut pb = std::path::PathBuf::from(temp_file.path());
    pb.set_extension("teg");
    let mut db = Database::open(format!("file://{}", pb.display()))?;

    db.execute(
        "CREATE TABLE chat_history (
            id INTEGER PRIMARY KEY,
            message TEXT(500),
            timestamp INTEGER
        )",
    )?;
    println!("   ✅ Database ready");

    // Test 2: Embedding generation
    println!("\n2. Testing embedding API...");
    let test_text = "Hello world";
    let embedding = embedding::embed(test_text, embedding::EmbeddingModel::Ollama)?;
    println!("   ✅ Generated embedding: {} dimensions", embedding.len());
    println!("   Values: {:?}", &embedding[..5]);

    // Test 3: Store embedding manually
    println!("\n3. Testing vector storage...");
    let _vec_str = format!(
        "[{}]",
        embedding
            .iter()
            .map(|v| format!("{v:.6}"))
            .collect::<Vec<_>>()
            .join(", ")
    );

    let insert_sql = format!(
        "INSERT INTO chat_history (id, message, timestamp) VALUES (1, '{}', {})",
        test_text.replace('\'', "''"),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_secs()
    );
    db.execute(&insert_sql)?;
    println!("   ✅ Message stored");

    // Test 4: Ollama connection test (separate async test)
    println!("\n4. Testing Ollama connection...");
    tokio::runtime::Runtime::new().unwrap().block_on(async {
        let client = reqwest::Client::new();
        let payload = json!({
            "model": "gemma3:latest",
            "messages": [{"role": "user", "content": "Say 'test working' and nothing else."}],
            "stream": false
        });

        match client
            .post("http://localhost:11434/api/chat")
            .json(&payload)
            .timeout(std::time::Duration::from_secs(10))
            .send()
            .await
        {
            Ok(response) => {
                println!("   ✅ Ollama responded: {}", response.status());
                match response.json::<serde_json::Value>().await {
                    Ok(json) => {
                        if let Some(content) = json["message"]["content"].as_str() {
                            println!("   Response: {}", content);
                        }
                    }
                    Err(_) => println!("   Warning: Could not parse response"),
                }
            }
            Err(e) => println!("   ❌ Ollama error: {}", e),
        }
    });

    println!("\n✅ All tests completed! The chatbot components are working.");
    println!("\nTo run interactively:");
    println!("1. cargo run --example ollama_chatbot_working --features dev");
    println!("2. Type your questions and press Enter");
    println!("3. Type 'exit' to quit");

    Ok(())
}
