use tegdb::{Database};
use std::io::{self, Write};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    use tegdb::Result;
    use reqwest;
    use serde_json::json;
    
    // Create temporary database file
    let temp_file = tempfile::NamedTempFile::new().expect("Failed to create temp file");
    let db_path = format!("file://{}", temp_file.path().to_str().unwrap());
    
    let mut db = tegdb::Database::open(&db_path)?;
    
    db.execute("CREATE TABLE knowledge (id INTEGER PRIMARY KEY, topic TEXT(64), fact TEXT(512), embed VECTOR(768))")?;

    println!("🤖 Personal Memory Assistant");
    println!("{}", "=".repeat(30));
    println!("I remember things about you and can chat about them!\n");
    
    // Sample personal knowledge (could be loaded from user input in a real app)
    let personal_facts = vec![
        ("Pets", "I have a golden retriever named Buddy"),
        ("Pets", "Buddy loves playing fetch and swimming"),
        ("Work", "I work as a software engineer at TechCorp Inc."),
        ("Work", "My favorite programming language is Rust"),
        ("Programming", "Rust is fast and memory-safe"),
        ("Family", "My sister Sarah works as a doctor in Boston"),
        ("Family", "My nephew Tom is 8 years old and loves LEGOs"),
        ("Hobbies", "I enjoy hiking in Yosemite National Park"),
        ("Studies", "I'm currently learning machine learning"),
        ("Goals", "I want to start my own tech company"),
    ];

    // Prepare statement once - use Ollama for real semantic embeddings!
    let insert_sql = "INSERT INTO knowledge (id, topic, fact, embed) VALUES (?1, ?2, ?3, EMBED(?4, 'ollama'))";
    let stmt = db.prepare(insert_sql)?;
    
    println!("🔄 Loading my memory...");
    let mut id_counter = 1;
    for (topic, fact) in personal_facts {
        // Ultra-clean API - perfect mixed types!
        db.execute_prepared_4(&stmt, id_counter, topic, fact, fact)?;
        id_counter += 1;
    }

    // Check Ollama connection
    let client = reqwest::Client::new();
    match client.get("http://localhost:11434/api/tags").send().await {
        Ok(_) => {
            println!("✅ Ready! What would you like to know?\n");
                
            // Interactive chatbot loop
            loop {
                print!("🤔 You: ");
                io::stdout().flush().unwrap();
                
                let mut input = String::new();
                io::stdin().read_line(&mut input).unwrap();
                let question = input.trim();
                
                if question == "quit" {
                    println!("👋 Goodbye! Hope you learned something about yourself!");
                    break;
                }
                
                if question.is_empty() {
                    continue;
                }
                
                if question == "help" {
                    println!("🗺️ Commands:");
                    println!("  'quit' - Say goodbye");
                    println!("  'help' - Show commands");
                    println!("  'remember' - See everything I know about you");
                    continue;
                }
                
                if question == "remember" {
                    println!("🧠 Here's everything I remember:");
                    if let Ok(all_result) = db.query("SELECT id, topic, fact FROM knowledge ORDER BY id") {
                        for row_data in all_result.rows_as_text() {
                            if row_data.len() >= 3 {
                                println!("  • {}: {}", row_data[1], row_data[2]);
                            }
                        }
                    }
                    println!();
                    continue;
                }
                
                // Perform real semantic vector search with Ollama embeddings!
                let escaped_question = tegdb::sql_utils::escape_sql_string(question);
                let relevant_facts = db.query(&format!(
                    "SELECT topic, fact FROM knowledge 
                     WHERE COSINE_SIMILARITY(embed, EMBED('{}', 'ollama')) > 0.25 
                     ORDER BY COSINE_SIMILARITY(embed, EMBED('{}', 'ollama')) DESC 
                     LIMIT 5",
                    escaped_question, escaped_question
                ))?;
                
                // Build context from relevant facts only
                let mut kb_context = String::new();
                let mut facts_count = 0;
                
                for row_data in relevant_facts.rows_as_text() {
                    if row_data.len() >= 2 {
                        let topic = &row_data[0];
                        let fact = &row_data[1];
                        kb_context.push_str(&format!("- {}: {}\n", topic, fact));
                        facts_count += 1;
                    }
                }
                
                if facts_count == 0 {
                    println!("🤖 I don't remember anything about that topic.");
                    continue;
                }
                
                // Create personalized prompt using ONLY relevant knowledge
                let personalized_prompt = format!(
                    "Based ONLY on these relevant facts from my personal knowledge:\n\n{}\n\nQuestion: {}\n\nRespond naturally using only the information above. If the answer isn't in these facts, say \"This information is not in my personal knowledge base, but I'd be happy to help if you tell me more!\"",
                    kb_context, question
                );
                
                let chat_payload = json!({
                    "model": "gemma3:latest",
                    "messages": [
                        {"role": "system", "content": "You are a friendly personal assistant. Respond naturally and casually based on the information provided."},
                        {"role": "user", "content": personalized_prompt}
                    ],
                    "stream": false
                });
                
                match client
                    .post("http://localhost:11434/api/chat")
                    .json(&chat_payload)
                    .send()
                    .await {
                    
                    Ok(response) => {
                        let result: serde_json::Value = response.json().await?;
                        let ai_answer = result["message"]["content"].as_str().unwrap_or("Hmm, let me think about that...");
                        println!("🤖 {}\n", ai_answer);
                    },
                    Err(_) => {
                        println!("🤖 Let me check my memories...\n");
                        println!("{}", kb_context.trim());
                    }
                }
            }
            
        },
        Err(_) => {
            println!("🤖 I'm here! (Running in memory-only mode since Ollama isn't connected)");
            println!("💡 For full AI responses, start Ollama with: ollama serve\n");
        }
    }

    Ok(())
}
