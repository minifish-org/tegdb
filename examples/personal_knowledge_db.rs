#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Create temporary database file
    let temp_file = tempfile::NamedTempFile::new().expect("Failed to create temp file");
    let db_path = format!("file://{}", temp_file.path().to_str().unwrap());

    let mut db = tegdb::Database::open(&db_path)?;

    db.execute("CREATE TABLE knowledge (id INTEGER PRIMARY KEY, topic TEXT(64), fact TEXT(512), embed VECTOR(768))")?;

    // Step 1: Insert your personal knowledge FIRST
    println!("📝 Adding personal knowledge to database\n");

    // Example personal facts with clearer topics for vector search testing
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
    let insert_sql =
        "INSERT INTO knowledge (id, topic, fact, embed) VALUES (?1, ?2, ?3, EMBED(?4, 'ollama'))";
    let stmt = db.prepare(insert_sql)?;

    let mut id_counter = 1;
    for (topic, fact) in personal_facts {
        println!("Adding: {} - {}", topic, fact);

        // Ultra-clean API - perfect mixed types!
        db.execute_prepared_4(&stmt, id_counter, topic, fact, fact)?;
        id_counter += 1;
    }

    println!("\n✅ Personal knowledge stored! Testing vector search...\n");

    // Show all data first
    println!("📊 All stored facts:");
    if let Ok(all_result) = db.query("SELECT id, topic, fact FROM knowledge ORDER BY id") {
        for row_data in all_result.rows_as_text() {
            if row_data.len() >= 3 {
                println!("  {}: {} - {}", row_data[0], row_data[1], row_data[2]);
            }
        }
    }
    println!();

    // Test vector search with different queries
    let test_queries = vec![
        "pets",
        "work",
        "programming",
        "family",
        "dog",
        "Rust programming",
    ];

    for query in test_queries {
        println!("🔍 Testing vector search for: '{}'", query);

        // Try real semantic vector search with Ollama embeddings!
        match db.query(&format!(
            "SELECT topic, fact FROM knowledge 
             WHERE COSINE_SIMILARITY(embed, EMBED('{}', 'ollama')) > 0.3 
             ORDER BY COSINE_SIMILARITY(embed, EMBED('{}', 'ollama')) DESC 
             LIMIT 3",
            query, query
        )) {
            Ok(result) => {
                let facts_found = result.rows_as_text();
                if facts_found.is_empty() {
                    println!("  ❌ No results found");
                } else {
                    println!("  ✅ Found {} relevant facts:", facts_found.len());
                    for (i, fact) in facts_found.iter().enumerate() {
                        if fact.len() >= 2 {
                            println!("    {}: {} - {}", i + 1, fact[0], fact[1]);
                        }
                    }
                }
            }
            Err(e) => {
                println!("  ❌ Vector search failed: {}", e);
            }
        }

        println!();
    }

    // Test if embeddings were actually created
    println!("🔎 Checking if embeddings were created:");
    if let Ok(embed_result) = db.query("SELECT id, topic, CASE WHEN embed IS NOT NULL THEN 'embedding exists' ELSE 'NULL' END as embed_status FROM knowledge") {
        for row_data in embed_result.rows_as_text() {
            if row_data.len() >= 3 {
                println!("  {}: {} - {}", row_data[0], row_data[1], row_data[2]);
            }
        }
    }

    println!("\n🏁 Vector search debug test complete!");

    Ok(())
}
