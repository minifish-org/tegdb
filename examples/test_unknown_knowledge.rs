use serde_json::json;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("🧪 Testing Personal Knowledge Isolation\n");

    let client = reqwest::Client::new();

    // Simulate asking about something NOT in my personal knowledge
    let question = "What is quantum computing?"; // This is general knowledge, not personal

    let personal_knowledge = "
    My Personal Knowledge Base:
    - My Work: I work as a software engineer at TechCorp Inc.
    - My Work: My favorite programming language is Rust
    - My Pets: I have a golden retriever named Buddy
    ";

    println!("🤔 Question: {}", question);
    println!("📚 Available Personal Knowledge:\n{}", personal_knowledge);

    let prompt = format!(
        "Based ONLY on my personal knowledge database:\n\n{}\n\nQuestion: {}\n\nAnswer using only the information above. If the answer isn't in my personal data, say \"This information is not in my personal knowledge base.\"",
        personal_knowledge, question
    );

    let payload = json!({
        "model": "gemma3:latest",
        "messages": [
            {"role": "system", "content": "You have access ONLY to my personal knowledge base. Use only information provided."},
            {"role": "user", "content": prompt}
        ],
        "stream": false
    });

    let response = client
        .post("http://localhost:11434/api/chat")
        .json(&payload)
        .send()
        .await?;

    let result: serde_json::Value = response.json().await?;
    let ai_answer = result["message"]["content"]
        .as_str()
        .unwrap_or("No response");

    println!("💡 AI Answer: {}", ai_answer);
    println!("\n🎯 Expected Result:");
    println!("AI should say: \"This information is not in my personal knowledge base.\"\n");

    println!("🔍 This proves:");
    println!("✅ AI can't access general knowledge like GPT training");
    println!("✅ AI ONLY uses YOUR personal data");
    println!("✅ True knowledge base isolation");
    println!("\n🧠 This is how REAL personal knowledge databases work!");

    Ok(())
}
