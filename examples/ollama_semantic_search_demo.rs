use serde_json::json;
use tegdb::Database;
use tempfile::NamedTempFile;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("=== TegDB + Ollama Semantic Search Demo ===\n");

    // Initialize TegDB
    let temp_file = NamedTempFile::new().expect("Failed to create temp file");
    let db_path = temp_file.path();
    let mut db = Database::open(format!("file://{}", db_path.display()))?;

    // Create table for documents with embeddings
    println!("1. Creating documents table with vector column...");
    db.execute(
        "CREATE TABLE embeddings (
            id INTEGER PRIMARY KEY,
            text TEXT(32),
            embedding VECTOR(768)
        )",
    )?;

    // Create vector index
    println!("2. Creating vector index...");
    db.execute("CREATE INDEX idx_hnsw ON embeddings(embedding) USING HNSW")?;

    // Sample documents
    let documents = [
        ("Introduction to Rust", "Rust is a systems programming language that focuses on safety, speed, and concurrency. It prevents segfaults and guarantees thread safety."),
        ("Machine Learning Basics", "Machine learning is a subset of artificial intelligence that enables computers to learn and make decisions from data without being explicitly programmed."),
        ("Web Development with React", "React is a JavaScript library for building user interfaces. It uses a virtual DOM and component-based architecture for efficient rendering."),
        ("Database Design Principles", "Good database design involves normalization, proper indexing, and understanding relationships between entities to ensure data integrity and performance."),
        ("Python for Data Science", "Python is widely used in data science due to libraries like NumPy, Pandas, and Scikit-learn that provide powerful tools for data manipulation and analysis."),
        ("Cloud Computing Overview", "Cloud computing provides on-demand access to computing resources over the internet, including storage, processing power, and software applications."),
        ("Cybersecurity Fundamentals", "Cybersecurity involves protecting computer systems, networks, and data from digital attacks, theft, and damage."),
        ("Mobile App Development", "Mobile app development involves creating applications for smartphones and tablets, with platforms like iOS and Android offering different development approaches."),
    ];

    // Generate embeddings using Ollama
    println!("3. Generating embeddings using Ollama...");
    let mut document_embeddings = Vec::new();

    for (i, (title, content)) in documents.iter().enumerate() {
        let text = format!("{title}: {content}");
        println!("   Generating embedding for: {title}");

        // Generate real embeddings using Ollama
        let embedding = generate_embedding(&text).await?;
        document_embeddings.push((i + 1, title, content, embedding));
    }

    // Insert documents with embeddings
    println!("4. Inserting documents with embeddings...");
    for (id, title, content, embedding) in document_embeddings {
        let embedding_str = format!(
            "[{}]",
            embedding
                .iter()
                .map(|v| format!("{v:.1}"))
                .collect::<Vec<_>>()
                .join(", ")
        );

        let text = format!("{title}: {content}");
        db.execute(&format!(
            "INSERT INTO embeddings (id, text, embedding) VALUES 
             ({id}, '{text}', {embedding_str})"
        ))?;
    }

    // Test semantic search queries
    println!("\n5. Testing semantic search queries...\n");

    // Query 1: Programming languages - use real Ollama embedding
    println!("Query 1: 'programming languages and development'");
    let query1_embedding = generate_embedding("programming languages and development").await?;
    let query1_str = format!(
        "[{}]",
        query1_embedding
            .iter()
            .map(|v| format!("{v:.6}"))
            .collect::<Vec<_>>()
            .join(", ")
    );

    let result1 = db.query(&format!(
        "SELECT text, COSINE_SIMILARITY(embedding, {query1_str}) 
         FROM embeddings 
         ORDER BY COSINE_SIMILARITY(embedding, {query1_str}) DESC 
         LIMIT 3"
    ))?;

    println!("Top 3 most similar documents:");
    for row in result1.rows() {
        let text = match &row[0] {
            tegdb::SqlValue::Text(t) => t,
            _ => "Unknown",
        };
        let similarity = match &row[1] {
            tegdb::SqlValue::Real(r) => *r,
            _ => 0.0,
        };
        println!("  - {text} (similarity: {similarity:.4})");
    }

    // Query 2: Data and analytics
    println!("\nQuery 2: 'data analysis and statistics'");
    let query2_embedding = generate_embedding("data analysis and statistics").await?;
    let query2_str = format!(
        "[{}]",
        query2_embedding
            .iter()
            .map(|v| format!("{v:.6}"))
            .collect::<Vec<_>>()
            .join(", ")
    );

    let result2 = db.query(&format!(
        "SELECT text, COSINE_SIMILARITY(embedding, {query2_str}) FROM embeddings ORDER BY COSINE_SIMILARITY(embedding, {query2_str}) DESC LIMIT 3"
    ))?;

    println!("Top 3 most similar documents:");
    for row in result2.rows() {
        let text = match &row[0] {
            tegdb::SqlValue::Text(t) => t,
            _ => "Unknown",
        };
        let similarity = match &row[1] {
            tegdb::SqlValue::Real(r) => *r,
            _ => 0.0,
        };
        println!("  - {text} (similarity: {similarity:.4})");
    }

    // Query 3: Similarity threshold
    println!("\nQuery 3: 'computer security and protection' (similarity > 0.7)");
    let query3_embedding = generate_embedding("computer security and protection").await?;
    let query3_str = format!(
        "[{}]",
        query3_embedding
            .iter()
            .map(|v| format!("{v:.6}"))
            .collect::<Vec<_>>()
            .join(", ")
    );

    let result3 = db.query(&format!(
        "SELECT text, COSINE_SIMILARITY(embedding, {query3_str}) FROM embeddings WHERE COSINE_SIMILARITY(embedding, {query3_str}) > 0.7 ORDER BY COSINE_SIMILARITY(embedding, {query3_str}) DESC"
    ))?;

    println!("Documents with similarity > 0.7:");
    for row in result3.rows() {
        let text = match &row[0] {
            tegdb::SqlValue::Text(t) => t,
            _ => "Unknown",
        };
        let similarity = match &row[1] {
            tegdb::SqlValue::Real(r) => *r,
            _ => 0.0,
        };
        println!("  - {text} (similarity: {similarity:.4})");
    }

    // Query 4: Different similarity function
    println!("\nQuery 4: 'web technologies' using Euclidean distance");
    let query4_embedding = generate_embedding("web technologies").await?;
    let query4_str = format!(
        "[{}]",
        query4_embedding
            .iter()
            .map(|v| format!("{v:.6}"))
            .collect::<Vec<_>>()
            .join(", ")
    );

    let result4 = db.query(&format!(
        "SELECT text, EUCLIDEAN_DISTANCE(embedding, {query4_str}) FROM embeddings ORDER BY EUCLIDEAN_DISTANCE(embedding, {query4_str}) ASC LIMIT 3"
    ))?;

    println!("Top 3 closest documents (Euclidean distance):");
    for row in result4.rows() {
        let text = match &row[0] {
            tegdb::SqlValue::Text(t) => t,
            _ => "Unknown",
        };
        let distance = match &row[1] {
            tegdb::SqlValue::Real(r) => *r,
            _ => 0.0,
        };
        println!("  - {text} (distance: {distance:.4})");
    }

    println!("\n✅ Semantic search with Ollama embeddings is working!");
    println!("   - Generated embeddings using nomic-embed-text model");
    println!("   - Performed semantic similarity search in TegDB");
    println!("   - Tested multiple similarity functions and thresholds");
    println!("   - Demonstrated real-world document search capabilities");
    println!("   - Successfully integrated with Ollama embedding service!");

    Ok(())
}

async fn generate_embedding(text: &str) -> Result<Vec<f64>, Box<dyn std::error::Error>> {
    let client = reqwest::Client::new();

    let payload = json!({
        "model": "nomic-embed-text:latest",
        "prompt": text
    });

    let response = client
        .post("http://localhost:11434/api/embeddings")
        .json(&payload)
        .send()
        .await?;

    if !response.status().is_success() {
        return Err(format!("Ollama API error: {}", response.status()).into());
    }

    let result: serde_json::Value = response.json().await?;

    if let Some(embedding) = result["embedding"].as_array() {
        let embedding_vec: Result<Vec<f64>, _> = embedding
            .iter()
            .map(|v| v.as_f64().ok_or("Invalid embedding value"))
            .collect();
        Ok(embedding_vec?)
    } else {
        Err("No embedding found in response".into())
    }
}
