# TegDB Review Overview

## Overview
- Single-threaded embedded SQL engine with layered API: `Database` orchestrates parser/planner/executor over the WAL-backed storage engine (`src/lib.rs:49-110`, `src/planner.rs:217-257`, `src/storage_engine.rs:81-118`).
- Supports DDL/DML, transactions, prepared statements, secondary indexes, fixed-width row storage, and vector math helpers (`src/database.rs:38-210`, `src/query_processor.rs:820-877`, `src/storage_format.rs:43-112`, `src/parser.rs:400-468`).
- Regression and integration suites exercise CRUD, ACID, planner paths, and newer vector features (`tests/integration/sql_integration_tests.rs:1-120`, `tests/integration/vector_search_tests.rs:1-200`).

## Strengths
- WAL plus undo log provides atomic transactions and crash recovery; commit markers are replayed during startup to discard uncommitted writes (`src/storage_engine.rs:311-364`, `src/backends/file_log_backend.rs:86-119`).
- Rule-based planner picks primary-key lookups, range scans, or secondary-index scans before falling back to table scans, then wraps ORDER BY as needed (`src/planner.rs:217-319`, `src/planner.rs:940-1015`).
- Prepared statements cache parameter counts and optional plan templates for fast re-use, giving a compact embedded API similar to SQLite (`src/database.rs:38-210`, `src/database.rs:592-707`).
- Fixed-width storage format yields deterministic offsets and zero-copy reads while the catalog injects offsets/type tags at DDL time (`src/catalog.rs:314-345`, `src/storage_format.rs:43-112`).
- Vector similarity functions (cosine, Euclidean, dot, normalization) are first-class expressions, enabling semantic scoring in plain SQL (`src/parser.rs:400-468`).

## Recent Improvements
- Parameter binding preserves ORDER BY and other expressions in prepared SELECTs (`src/database.rs:584-639`).
- Aggregate execution materializes rows once, supports multiple aggregates, and handles mixed numeric types without lossy casts (`src.query_processor.rs:1211-1440`).
- Secondary indexes persist engine type metadata, enforce `UNIQUE` for BTree indexes, and stay consistent through UPDATE/DELETE/DROP flows (`src.catalog.rs:258-305`, `src.query_processor.rs:854-915`, `src.query_processor.rs:2135-2174`).
- CREATE INDEX accepts `USING <type>` and DDL enforces fixed-width TEXT/VECTOR definitions to match storage constraints (`src.parser.rs:789-828`, `src.parser.rs:1109-1126`).

## Remaining Gaps & Risks
- Durability remains best-effort: commits rely on lazy fsync and the latest writes can be lost after power failures (`src/storage_engine.rs:81-161`, `src/storage_engine.rs:311-339`).
- SQL surface remains narrow: no JOIN, GROUP BY, subqueries, or expression ordering beyond simple columns (`src.parser.rs:124-218`, `src.planner.rs:217-257`).

## Next Opportunities
1. Expand core SQL semantics (GROUP BY, HAVING, JOINs, subqueries) so TegDB can graduate from toy workloads to serious embedded OLTP use.
2. Turn the vector story into a differentiator: wire HNSW/IVF/LSH into the planner/executor and expose vector-specific query syntax to compete as a lightweight similarity search engine (`src.vector_index.rs:1-134`).
3. Lean into the embedded high-performance RDBMS niche by focusing on predictable single-node latency, snapshot-based durability, and tight language bindings.
4. Explore OLAP-lite extensions (columnar storage, simple aggregates over partitions) only if analytics-on-edge becomes a priority; otherwise keep the surface OLTP-focused to avoid dilution.
5. Broaden schema/storage options (variable-length text/vector encodings or overflow pages) while preserving the single-threaded simplicity that differentiates TegDB (`src.storage_format.rs:43-112`).
