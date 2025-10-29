use std::io::{self, Write};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Create temporary database file
    let temp_file = tempfile::NamedTempFile::new().expect("Failed to create temp file");
    let mut pb = std::path::PathBuf::from(temp_file.path());
    pb.set_extension("teg");
    let db_path = format!("file://{}", pb.display());

    let mut db = tegdb::Database::open(&db_path)?;

    db.execute("CREATE TABLE knowledge (id INTEGER PRIMARY KEY, topic TEXT(64), fact TEXT(512), embed VECTOR(768))")?;

    println!("🤖 Personal Memory Assistant");
    println!("{}", "=".repeat(30));
    println!("I remember things about you and can chat about them!");
    println!("💡 Using Ollama for real semantic embeddings!\n");

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
    let insert_sql =
        "INSERT INTO knowledge (id, topic, fact, embed) VALUES (?1, ?2, ?3, EMBED(?4, 'ollama'))";
    let stmt = db.prepare(insert_sql)?;

    println!("🔄 Loading my memory...");
    let mut id_counter = 1;
    for (topic, fact) in personal_facts {
        // Bind mixed types via explicit SqlValue vector
        let params = vec![
            id_counter.into(),
            (*topic).into(),
            (*fact).into(),
            (*fact).into(),
        ];
        db.execute_prepared(&stmt, &params)?;
        id_counter += 1;
    }

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

        // Perform semantic vector search with Ollama embeddings
        let search_sql = format!(
            "SELECT topic, fact FROM knowledge WHERE COSINE_SIMILARITY(embed, EMBED('{}', 'ollama')) > 0.5 ORDER BY COSINE_SIMILARITY(embed, EMBED('{}', 'ollama')) DESC LIMIT 3",
            question.replace("'", "''"), question.replace("'", "''")
        );
        let relevant_facts = db.query(&search_sql)?;

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

        // Simple response based on relevant knowledge (no external LLM dependency)
        println!("🤖 Based on my memory, here's what I know:\n");
        println!("{}", kb_context.trim());
        println!("\n💡 This information comes from my personal knowledge base using Ollama semantic embeddings!");
    }

    Ok(())
}
