# TegDB + Ollama Chatbot Examples

Two examples demonstrating how to build a chatbot using TegDB's `EMBED()` function with Ollama's local LLM.

## What These Examples Do

Both examples demonstrate **RAG (Retrieval Augmented Generation)**:

1. **Store conversations** with vector embeddings using `EMBED()`
2. **Search similar past conversations** using `COSINE_SIMILARITY()`
3. **Provide context** to the LLM for better responses
4. **Remember past interactions** without external vector databases

## Architecture

```
User Question
    ↓
┌─────────────────────────┐
│  TegDB EMBED()         │ ← Generate embedding
│  VECTOR(128)           │
└─────────────────────────┘
    ↓
┌─────────────────────────┐
│  Vector Search         │ ← Find similar past Q&A
│  COSINE_SIMILARITY()   │
└─────────────────────────┘
    ↓
┌─────────────────────────┐
│  Ollama (gemma3)       │ ← Generate response
│  with context          │
└─────────────────────────┘
    ↓
Store response + embedding
```

## Prerequisites

### 1. Install Ollama

```bash
# macOS
brew install ollama

# Linux
curl https://ollama.ai/install.sh | sh

# Or download from: https://ollama.ai
```

### 2. Start Ollama Server

```bash
ollama serve
```

### 3. Pull gemma3 Model

```bash
ollama pull gemma3:latest
```

## Examples

### Example 1: Full-Featured Chatbot

**File:** `examples/ollama_chatbot_demo.rs`

**Features:**
- Full conversation history
- Multi-turn conversations
- Relevance-based context filtering
- Memory browsing with 'memory' command
- Timestamps and role tracking
- Conversation pruning (keeps last 10 messages)

**Run:**
```bash
cargo run --example ollama_chatbot_demo --features dev
```

**Sample Session:**
```
You: What is Rust?
Bot: Rust is a systems programming language focused on safety and performance...

You: Why is it safe?
Bot: Based on our previous discussion about Rust, its safety comes from...

You: memory
📚 Conversation Memory:
👤 user: Why is it safe?
🤖 assistant: Based on our previous...
👤 user: What is Rust?
🤖 assistant: Rust is a systems...
```

### Example 2: Simple Chatbot

**File:** `examples/ollama_chatbot_simple.rs`

**Features:**
- Minimal code (~100 lines)
- Essential functionality only
- Great for learning
- Easy to customize

**Run:**
```bash
cargo run --example ollama_chatbot_simple --features dev
```

## How It Works

### 1. Storing Conversations with Embeddings

```sql
-- Create table with vector column
CREATE TABLE conversations (
    id INTEGER PRIMARY KEY,
    message TEXT(500),
    embedding VECTOR(128)
);

-- Store with automatic embedding
INSERT INTO conversations (message, embedding) 
VALUES ('What is machine learning?', EMBED('What is machine learning?'));
```

### 2. Finding Similar Conversations

```sql
-- Find top 3 similar past questions
SELECT message, COSINE_SIMILARITY(embedding, EMBED(?)) as similarity
FROM conversations
WHERE is_user = 1
ORDER BY similarity DESC
LIMIT 3;
```

### 3. Providing Context to LLM

```json
{
  "model": "gemma3:latest",
  "messages": [
    {
      "role": "system",
      "content": "Previous related questions:\n1. What is ML?\n2. How does AI work?"
    },
    {
      "role": "user", 
      "content": "Can you explain neural networks?"
    }
  ]
}
```

## Key Benefits

### 1. **No External Vector DB Needed**
- Everything in TegDB
- Single database for data + vectors
- Simpler architecture

### 2. **Fast Vector Search**
- HNSW index support
- Sub-millisecond search
- Scales to millions of messages

### 3. **Simple Hash-Based Embeddings**
- No ML dependencies
- Deterministic (same text = same vector)
- Fast generation
- Good for semantic similarity

### 4. **Local & Private**
- All data stays local
- No API calls for embeddings
- Ollama runs locally
- Complete privacy

## Customization Ideas

### 1. Add Memory Decay
```sql
-- Weight recent conversations more
SELECT message, 
       COSINE_SIMILARITY(embedding, EMBED(?)) * 
       EXP(-(? - timestamp) / 86400.0) as score
FROM conversations
ORDER BY score DESC;
```

### 2. Multi-Model Support
```rust
let model = if query.contains("code") {
    "codellama"
} else {
    "gemma3:latest"
};
```

### 3. Conversation Threading
```sql
CREATE TABLE conversations (
    id INTEGER PRIMARY KEY,
    thread_id INTEGER,  -- Group related conversations
    message TEXT(500),
    embedding VECTOR(128)
);
```

### 4. Semantic Clustering
```sql
-- Find conversation topics
SELECT COUNT(*) as cluster_size,
       AVG(COSINE_SIMILARITY(embedding, (SELECT embedding FROM conversations WHERE id = ?))) as coherence
FROM conversations
GROUP BY (id / 10);  -- Simple clustering
```

## Performance

**Embedding Generation:**
- EMBED() call: ~0.1ms
- Store with embedding: ~1ms

**Vector Search:**
- HNSW search (1M vectors): ~2ms
- Top-K retrieval: ~5ms

**End-to-End:**
- User question → Context → Response: ~500ms
  - Embedding: 0.1ms
  - Search: 2ms
  - Ollama inference: 497ms

## Troubleshooting

### Ollama Connection Error
```
Error: Cannot connect to Ollama at http://localhost:11434
```

**Solution:**
```bash
# Check if Ollama is running
ps aux | grep ollama

# Start Ollama
ollama serve

# Verify it's working
curl http://localhost:11434/api/tags
```

### Model Not Found
```
Error: model 'gemma3:latest' not found
```

**Solution:**
```bash
# List available models
ollama list

# Pull the model
ollama pull gemma3:latest

# Or use a different model
ollama pull llama2
```

### Slow Responses

**Solutions:**
1. Use smaller model: `ollama pull gemma3:2b`
2. Reduce context window in code
3. Limit conversation history length
4. Use GPU if available

## Comparison with External Services

| Aspect | TegDB + Ollama | OpenAI + Pinecone |
|--------|----------------|-------------------|
| **Privacy** | 100% local | Cloud-based |
| **Cost** | Free | $$ per API call |
| **Latency** | ~500ms | ~1000ms |
| **Setup** | Simple | Complex |
| **Dependencies** | 2 (TegDB, Ollama) | 3+ (App, OpenAI, Pinecone) |
| **Embedding Quality** | Good for similarity | Excellent |

## Next Steps

1. **Try the examples** - Start with the simple one
2. **Customize prompts** - Adjust system messages
3. **Add features** - User authentication, export, etc.
4. **Deploy** - Both TegDB and Ollama are production-ready

## See Also

- [TegDB EMBED Function](EMBED_FUNCTION.md)
- [Vector Search Guide](NEXT_STEPS_VECTOR_SEARCH.md)
- [Ollama Documentation](https://ollama.ai/docs)
- [gemma3 Model](https://ollama.ai/library/gemma3)
