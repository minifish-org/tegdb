# TegDB Architecture

This document describes the internal design of TegDB: how the layers fit together, how data is stored and recovered, and how queries are planned and executed.

## Layered Architecture

TegDB uses a clean four-layer design where each layer depends only on the one below it:

```
┌─────────────────────────────────────────────┐
│  Database API  (src/database.rs)            │
│  SQLite-like interface, schema caching,     │
│  prepared statements, transaction handles   │
├─────────────────────────────────────────────┤
│  SQL Executor  (src/query_processor.rs)     │
│  Query optimization, plan execution,        │
│  DDL + DML handlers, streaming results      │
├─────────────────────────────────────────────┤
│  SQL Parser    (src/parser.rs)              │
│  nom-based recursive-descent parser,        │
│  produces typed AST nodes                   │
├─────────────────────────────────────────────┤
│  Storage Engine  (src/storage_engine.rs)    │
│  BTreeMap index, append-only WAL,           │
│  transactions, compaction, cache            │
└─────────────────────────────────────────────┘
```

### Layer 1 – Storage Engine (`src/storage_engine.rs`, `src/log.rs`)

The foundation of TegDB is a key-value storage engine built on:

- **In-memory BTreeMap index** (`KeyMap`) mapping each key to a `ValuePointer` (disk offset + length + optional inline copy).
- **Append-only write-ahead log** (WAL) stored in a single `.teg` file, managed by `src/log.rs` through a pluggable `LogBackend` trait (`src/backends/`).
- **Inline value cache**: values ≤ `inline_value_threshold` bytes are kept in memory inside the `ValuePointer`; larger values are read from disk on demand.
- **Byte-capped LRU page/value cache** (`cache_size_bytes`) for hot larger values.

#### File Format

The `.teg` file starts with a 64-byte fixed header:

```
[0..6)   magic:          b"TEGDB\0"
[6..8)   version:        u16 BE  (current = 2)
[8..12)  flags:          u32 BE  (reserved, currently 0)
[12..16) max_key_size:   u32 BE
[16..20) max_value_size: u32 BE
[20..21) endian:         u8      (1 = big-endian)
[21..29) valid_data_end: u64 BE  (byte boundary of committed data)
[29..64) reserved:       zero padding
```

After the header, the file contains a sequence of log records. Each record stores a length-prefixed key and value. A special `__TX_COMMIT__` key marks the end of a committed transaction.

#### Durability

- **Default**: `DurabilityLevel::Immediate` — `fsync` is called after every transaction commit, guaranteeing no data loss on crash.
- **Group commit**: set `DurabilityLevel::GroupCommit` with a non-zero `group_commit_interval` to coalesce fsyncs for higher write throughput at the cost of a small recovery window.
- File locking (`fs2`) prevents multiple writers from opening the same database simultaneously.

#### Compaction

The WAL is append-only; deleted and overwritten keys accumulate dead entries. Compaction rewrites the live key set to a new file and atomically replaces the old one. Triggers (all three must be satisfied):

1. Absolute size: ≥ `compaction_absolute_threshold_bytes` (default 10 MiB) written since last compact.
2. Fragmentation ratio: `log_size / live_data_size ≥ compaction_ratio` (default 2.0).
3. Minimum delta: ≥ `compaction_min_delta_bytes` (default 2 MiB) since last compact.

#### Crash Recovery

On open, TegDB replays the WAL from the beginning. Only records that belong to a committed transaction (one ending with a `__TX_COMMIT__` marker before `valid_data_end`) are loaded into the index. Partial writes beyond `valid_data_end` are discarded.

#### Transactions

`StorageEngine::begin_transaction()` returns a `Transaction` object that buffers writes in a local overlay. On `commit()`, changes are flushed to the WAL with a commit marker and an fsync (per durability settings). On drop without commit, the transaction is silently discarded (automatic rollback).

### Layer 2 – SQL Parser (`src/parser.rs`)

A hand-written, `nom`-based recursive-descent parser converts SQL text into a typed AST. Supported statement types:

- DDL: `CREATE TABLE`, `DROP TABLE`
- DML: `SELECT`, `INSERT`, `UPDATE`, `DELETE`
- Extensions: `CREATE EXTENSION`, `CREATE INDEX` (vector index)

The parser produces typed `SqlValue` variants (`Integer`, `Real`, `Text`, `Vector`, `Null`, `Parameter`) and structured condition trees (`WhereClause`, `Condition`).

### Layer 3 – Query Planner (`src/planner.rs`)

A rule-based planner converts AST nodes into `ExecutionPlan` variants without cost estimation:

| Plan type | When chosen | Complexity |
|---|---|---|
| `PrimaryKeyLookup` | Equality filter on single PK column | O(log n) |
| `TableRangeScan` | Range condition on PK column | O(log n + k) |
| `TableScan` | No PK condition, or non-PK filter | O(n) |

The planner also handles `INSERT` conflict resolution (ignore / replace), `UPDATE` and `DELETE` scan plans, and vector similarity searches using HNSW-style approximate nearest-neighbor indexing (`src/vector_index.rs`).

### Layer 4 – Database API (`src/database.rs`, `src/catalog.rs`)

`Database` is the user-facing type, equivalent to a SQLite connection handle. It:

- Owns a `StorageEngine` instance.
- Maintains a `Catalog` of table schemas cached in memory.
- Exposes `execute(sql)` for DDL/DML and `query(sql)` for `SELECT` statements.
- Provides `begin_transaction()` for explicit multi-statement transactions.
- Supports `prepare(sql)` to parse and partially plan a statement once and bind parameters at execution time.

## Row Storage Format (`src/storage_format.rs`)

Rows are serialized into a compact fixed-width binary format:

- **Type codes**: `Integer` (8-byte i64 BE), `Real` (8-byte f64 BE), `TextFixed` (fixed-length, null-padded), `Vector` (f64 array).
- Each column has a pre-computed `storage_offset` and `storage_size` stored in the schema's `ColumnInfo`. Serialization and deserialization use direct byte-offset writes and reads — zero extra allocation for well-formed rows.
- The storage key for a row is the binary encoding of the primary key value with a 1-byte type tag prefix, ensuring correct BTree sort order.

## Extension System (`src/extension.rs`)

TegDB supports dynamically loaded extensions (`dlopen`/`LoadLibrary`) following a PostgreSQL-inspired plugin API. Extensions register named scalar and aggregate functions. Built-in extensions (always available) provide:

- **String functions**: `UPPER`, `LOWER`, `LENGTH`, `TRIM`, `SUBSTR`, `REPLACE`, `CONCAT`, `REVERSE`
- **Math functions**: `ABS`, `CEIL`, `FLOOR`, `ROUND`, `SQRT`, `POW`, `MOD`, `SIGN`

## Streaming Backup (`src/tgstream/`)

`tgstream` is a standalone background tool that replicates a `.teg` file to S3-compatible storage (AWS S3, MinIO, etc.). It monitors `valid_data_end` in the file header for new committed data, uploads incremental segments after each commit, and periodically uploads full base snapshots. Restore downloads a base snapshot plus all subsequent segments and replays them in order.

## Storage Backends (`src/backends/`)

The `LogBackend` trait abstracts the underlying I/O so the storage engine can use:

- `FileLogBackend` — the default, backed by a local `.teg` file.
- `RpcLogBackend` — experimental Cap'n Proto RPC backend (`tglogd` server), enabled with the `rpc` feature flag.

## Key Design Decisions

| Decision | Rationale |
|---|---|
| Single-threaded | Eliminates concurrency bugs; simplifies correctness guarantees |
| Append-only WAL | Simple crash recovery; no in-place overwrites |
| BTreeMap index in memory | Ordered scans; range queries without secondary structures |
| Fixed-width row format | Zero-copy column access; predictable storage sizing |
| fsync per commit (default) | Data safety first; group commit available for throughput |
| No foreign keys or JOINs (yet) | Keeps complexity low; planned for future releases |

## Source Map

| Path | Responsibility |
|---|---|
| `src/database.rs` | Top-level `Database` and `Transaction` API |
| `src/catalog.rs` | Schema catalog and index metadata |
| `src/query_processor.rs` | Plan execution, DDL/DML handlers |
| `src/planner.rs` | Rule-based query planner |
| `src/parser.rs` | SQL parser (nom-based) |
| `src/storage_engine.rs` | KV engine config, transaction wrapper |
| `src/log.rs` | WAL, `KeyMap`, `ValuePointer`, `LogBackend` trait |
| `src/storage_format.rs` | Binary row serialization/deserialization |
| `src/backends/` | `FileLogBackend`, optional `RpcLogBackend` |
| `src/vector_index.rs` | HNSW approximate nearest-neighbor index |
| `src/extension.rs` | Extension loading and function registry |
| `src/tgstream/` | Streaming backup/restore to S3-compatible storage |
| `src/bin/tg.rs` | Interactive REPL and one-shot SQL CLI |
| `src/bin/tgstream.rs` | Streaming backup CLI |
| `src/bin/tglogd.rs` | Cap'n Proto RPC log server |
