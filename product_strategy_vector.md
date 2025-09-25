# TegDB Vector Story – Product Marketing Fit

## Positioning
“A lightweight vector search engine for agents, apps, and edge devices.”

## Target Users
- AI agent builders who need a local memory store
- Personalization and recommendation teams embedding inference inside their apps
- On-device ML pipelines (mobile, IoT, robotics) wanting similarity search without a managed service
- Indie SaaS projects that can’t justify Pinecone/Weaviate overhead but still need semantic lookup

## Value Pillars
1. **Built-in ANN Indexes** – Native HNSW/IVF/LSH backends exposed through SQL so developers get approximate nearest-neighbor search without separate services.
2. **Local Inference Friendly** – Embeddings generated on-device and queried instantly; no network round-trips, no data exfiltration.
3. **Hybrid Tables** – Structured filters and vector distances operate in the same embedded engine, enabling “WHERE category = 'shoes' ORDER BY cosine_similarity(...)” patterns.

## Messaging Hook
“Bring semantic search to your app in a single dependency—no cloud, no ops.”

## Launch Tactics
- Ship starter kits for personalized feeds, RAG memory, and on-device agent memory.
- Publish benchmarks comparing TegDB’s vector mode with Faiss + SQLite piping, highlighting simplicity and footprint.
- Showcase integrations with LangChain/LlamaIndex and popular embedding providers.
- Emphasize licensing and resource advantages over managed vector DBs for startups and edge deployments.

