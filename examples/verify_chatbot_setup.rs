//! Verification script for chatbot examples
//!
//! This script verifies that:
//! 1. TegDB EMBED() function works
//! 2. Vector search works
//! 3. Ollama API is accessible
//! 4. gemma3:latest model works
//!
//! Run: cargo run --example verify_chatbot_setup --features dev

use serde_json::json;
use tegdb::{Database, Result, SqlValue};

#[tokio::main]
async fn main() -> Result<()> {
    println!("🔍 Verifying Chatbot Setup");
    println!("==========================\n");

    let mut all_ok = true;

    // Test 1: TegDB EMBED() function
    print!("1. Testing TegDB EMBED() function... ");
    match test_embed_function() {
        Ok(_) => println!("✅ PASS"),
        Err(e) => {
            println!("❌ FAIL: {e}");
            all_ok = false;
        }
    }

    // Test 2: Vector search with COSINE_SIMILARITY
    print!("2. Testing vector search... ");
    match test_vector_search() {
        Ok(_) => println!("✅ PASS"),
        Err(e) => {
            println!("❌ FAIL: {e}");
            all_ok = false;
        }
    }

    // Test 3: Ollama connection
    print!("3. Testing Ollama connection... ");
    match test_ollama_connection().await {
        Ok(_) => println!("✅ PASS"),
        Err(e) => {
            println!("❌ FAIL: {e}");
            all_ok = false;
        }
    }

    // Test 4: gemma3:latest model
    print!("4. Testing gemma3:latest model... ");
    match test_gemma3_model().await {
        Ok(_) => println!("✅ PASS"),
        Err(e) => {
            println!("❌ FAIL: {e}");
            all_ok = false;
        }
    }

    // Test 5: End-to-end chatbot functionality
    print!("5. Testing end-to-end chatbot flow... ");
    match test_chatbot_flow().await {
        Ok(_) => println!("✅ PASS"),
        Err(e) => {
            println!("❌ FAIL: {e}");
            all_ok = false;
        }
    }

    println!("\n================================");
    if all_ok {
        println!("✅ All tests passed!");
        println!("\nYou can now run:");
        println!("  cargo run --example ollama_chatbot_simple --features dev");
        println!("  cargo run --example ollama_chatbot_demo --features dev");
    } else {
        println!("❌ Some tests failed. Please check the errors above.");
    }

    Ok(())
}

fn test_embed_function() -> Result<()> {
    let db_path = std::env::temp_dir().join("test_embed").with_extension("teg");
    let _ = std::fs::remove_file(&db_path);
    let mut db = Database::open(db_path.to_string_lossy())?;

    // Test EMBED() in SELECT
    let result = db.query("SELECT EMBED('test') as embedding")?;
    let rows = result.rows();

    if rows.is_empty() {
        return Err(tegdb::Error::Other("No rows returned".to_string()));
    }

    match &rows[0][0] {
        SqlValue::Vector(v) => {
            if v.len() != 128 {
                return Err(tegdb::Error::Other(format!(
                    "Expected 128 dimensions, got {}",
                    v.len()
                )));
            }
        }
        _ => return Err(tegdb::Error::Other("Expected vector value".to_string())),
    }

    Ok(())
}

fn test_vector_search() -> Result<()> {
    let db_path = std::env::temp_dir().join("test_search").with_extension("teg");
    let _ = std::fs::remove_file(&db_path);
    let mut db = Database::open(db_path.to_string_lossy())?;

    // Create table and insert test data
    db.execute(
        "CREATE TABLE test (id INTEGER PRIMARY KEY, text TEXT(100), embedding VECTOR(128))",
    )?;

    db.execute("INSERT INTO test VALUES (1, 'hello', EMBED('hello'))")?;
    db.execute("INSERT INTO test VALUES (2, 'world', EMBED('world'))")?;
    db.execute("INSERT INTO test VALUES (3, 'hello world', EMBED('hello world'))")?;

    // Test similarity search
    let result = db.query(
        "SELECT text, COSINE_SIMILARITY(embedding, EMBED('hello')) as sim 
         FROM test 
         ORDER BY sim DESC 
         LIMIT 1",
    )?;

    let rows = result.rows();
    if rows.is_empty() {
        return Err(tegdb::Error::Other("No search results".to_string()));
    }

    // Check that we got a similarity score
    match &rows[0][1] {
        SqlValue::Real(sim) => {
            if *sim < 0.0 || *sim > 1.0 {
                return Err(tegdb::Error::Other(format!(
                    "Invalid similarity score: {sim}"
                )));
            }
        }
        _ => {
            return Err(tegdb::Error::Other(
                "Expected real value for similarity".to_string(),
            ))
        }
    }

    Ok(())
}

async fn test_ollama_connection() -> Result<()> {
    let client = reqwest::Client::new();
    let response = client
        .get("http://localhost:11434/api/tags")
        .timeout(std::time::Duration::from_secs(5))
        .send()
        .await
        .map_err(|e| tegdb::Error::Other(format!("Connection failed: {e}")))?;

    if !response.status().is_success() {
        return Err(tegdb::Error::Other(format!(
            "Ollama returned error: {}",
            response.status()
        )));
    }

    Ok(())
}

async fn test_gemma3_model() -> Result<()> {
    let client = reqwest::Client::new();

    let payload = json!({
        "model": "gemma3:latest",
        "messages": [
            {"role": "user", "content": "Say 'test' and nothing else"}
        ],
        "stream": false
    });

    let response = client
        .post("http://localhost:11434/api/chat")
        .json(&payload)
        .timeout(std::time::Duration::from_secs(30))
        .send()
        .await
        .map_err(|e| tegdb::Error::Other(format!("Request failed: {e}")))?;

    if !response.status().is_success() {
        return Err(tegdb::Error::Other(format!(
            "Model returned error: {}",
            response.status()
        )));
    }

    let json: serde_json::Value = response
        .json()
        .await
        .map_err(|e| tegdb::Error::Other(format!("Parse failed: {e}")))?;

    // Check if we got a valid response
    if json["message"]["content"].as_str().is_none() {
        return Err(tegdb::Error::Other("No content in response".to_string()));
    }

    Ok(())
}

async fn test_chatbot_flow() -> Result<()> {
    let db_path = std::env::temp_dir().join("test_chatbot").with_extension("teg");
    let _ = std::fs::remove_file(&db_path);
    let mut db = Database::open(db_path.to_string_lossy())?;

    // Setup chatbot database
    db.execute(
        "CREATE TABLE chat (id INTEGER PRIMARY KEY, message TEXT(500), embedding VECTOR(128))",
    )?;

    // Store a message with embedding
    db.execute("INSERT INTO chat VALUES (1, 'What is AI?', EMBED('What is AI?'))")?;

    // Search for similar message
    let result = db.query(
        "SELECT message FROM chat 
         ORDER BY COSINE_SIMILARITY(embedding, EMBED('What is artificial intelligence?')) DESC 
         LIMIT 1",
    )?;

    if result.rows().is_empty() {
        return Err(tegdb::Error::Other(
            "Search returned no results".to_string(),
        ));
    }

    // Test Ollama response
    let client = reqwest::Client::new();
    let payload = json!({
        "model": "gemma3:latest",
        "messages": [
            {"role": "system", "content": "You are a helpful assistant."},
            {"role": "user", "content": "What is AI? Answer in 10 words or less."}
        ],
        "stream": false
    });

    let response = client
        .post("http://localhost:11434/api/chat")
        .json(&payload)
        .timeout(std::time::Duration::from_secs(30))
        .send()
        .await
        .map_err(|e| tegdb::Error::Other(format!("Ollama request failed: {e}")))?;

    let json: serde_json::Value = response
        .json()
        .await
        .map_err(|e| tegdb::Error::Other(format!("Response parse failed: {e}")))?;

    let content = json["message"]["content"]
        .as_str()
        .ok_or_else(|| tegdb::Error::Other("No response content".to_string()))?;

    // Store response
    let escaped_content = content.replace('\'', "''");
    db.execute(&format!(
        "INSERT INTO chat VALUES (2, '{escaped_content}', EMBED('What is AI?'))"
    ))?;

    // Verify both messages stored
    let count_result = db.query("SELECT COUNT(*) FROM chat")?;
    match &count_result.rows()[0][0] {
        SqlValue::Integer(count) => {
            if *count != 2 {
                return Err(tegdb::Error::Other(format!(
                    "Expected 2 messages, found {count}"
                )));
            }
        }
        _ => return Err(tegdb::Error::Other("Invalid count result".to_string())),
    }

    Ok(())
}
