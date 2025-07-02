# TegDB Architecture Design

This document describes the actual architecture and implementation of TegDB based on the current source code.

## Overview

TegDB is a lightweight, embedded database with a layered architecture designed for simplicity and performance. It provides a SQL-like interface on top of a key-value storage engine with ACID transaction support.

## Architecture Layers

TegDB implements a clean 4-layer architecture:

```text
┌─────────────────────────────────────────────────────────────┐
│                    Database API Layer                       │
│  (High-level SQLite-like interface with schema caching)    │
├─────────────────────────────────────────────────────────────┤
│                    Executor Layer                          │
│    (SQL statement execution and optimization)              │
├─────────────────────────────────────────────────────────────┤
│                    Parser Layer                            │
│         (SQL parsing and AST generation)                   │
├─────────────────────────────────────────────────────────────┤
│                    Engine Layer                            │
│    (Key-value storage with transactions and WAL)           │
└─────────────────────────────────────────────────────────────┘
```

## Core Components

### 1. Engine Layer (`src/engine.rs`)

The foundational storage layer providing:

#### Engine
- **Storage**: BTreeMap-based in-memory key-value store with Arc-shared values
- **Persistence**: Append-only log file with write-ahead logging
- **File Locking**: Exclusive file locks to prevent concurrent access
- **Recovery**: Crash recovery using transaction commit markers
- **Compaction**: Automatic log compaction to reclaim disk space

```rust
pub struct Engine {
    log: Log,
    key_map: KeyMap,  // BTreeMap<Vec<u8>, Arc<[u8]>>
    config: EngineConfig,
}
```

#### Transaction System
- **Write-through transactions**: Changes immediately visible to other operations
- **Undo logging**: Records original values for rollback capability
- **Commit markers**: Empty commit markers written to log for crash recovery
- **Automatic rollback**: Transactions auto-rollback on drop if not committed

```rust
pub struct Transaction<'a> {
    engine: &'a mut Engine,
    undo_log: Option<Vec<UndoEntry>>, // Lazy initialization
    finalized: bool,
}
```

#### Key Features
- **Zero-copy reads**: Uses Arc<[u8]> for efficient value sharing
- **Crash recovery**: Replays log up to last commit marker
- **ACID compliance**: Atomicity, Consistency, Isolation, Durability

### 2. Parser Layer (`src/parser.rs`)

SQL parsing using the `nom` parser combinator library:

#### Supported SQL Statements
- `SELECT` with WHERE, ORDER BY, LIMIT
- `INSERT` with single and multiple value rows
- `UPDATE` with WHERE conditions
- `DELETE` with WHERE conditions
- `CREATE TABLE` with column constraints
- `DROP TABLE` with IF EXISTS support
- Transaction control: `BEGIN`, `COMMIT`, `ROLLBACK`

#### AST (Abstract Syntax Tree)
```rust
pub enum Statement {
    Select(SelectStatement),
    Insert(InsertStatement),
    Update(UpdateStatement),
    Delete(DeleteStatement),
    CreateTable(CreateTableStatement),
    DropTable(DropTableStatement),
    Begin,
    Commit,
    Rollback,
}
```

#### Data Types
- `INTEGER` (i64)
- `REAL` (f64)
- `TEXT` (String)
- `BLOB` (Vec<u8>)
- `NULL`

### 3. Executor Layer (`src/executor.rs`)

SQL execution engine with query optimization:

#### Index-Organized Tables (IOT)
- **Primary key optimization**: Direct key lookups for PK equality queries
- **Efficient storage**: Only non-PK columns stored in values
- **Row keys**: Generated from primary key values for direct access

#### Query Optimization
- **Primary key lookups**: O(log n) direct access for PK equality
- **Early termination**: LIMIT clauses processed during scan for memory efficiency
- **Memory optimization**: Streaming processing to avoid large intermediate results

#### Schema Management
- **Table schemas**: Stored as special keys in the engine
- **Binary serialization**: Efficient schema storage and retrieval
- **Constraint validation**: NOT NULL, UNIQUE, PRIMARY KEY enforcement

### 4. Database Layer (`src/database.rs`)

High-level SQLite-like interface:

#### Schema Caching
```rust
pub struct Database {
    engine: Engine,
    table_schemas: Arc<RwLock<HashMap<String, TableSchema>>>,
}
```

- **Shared cache**: Schemas loaded once and shared across operations
- **Automatic updates**: Cache updated on DDL operations (CREATE/DROP TABLE)
- **Thread-safe access**: Arc<RwLock<>> for safe concurrent schema access

#### Transaction Management
- **Implicit transactions**: Single statements auto-wrapped in transactions
- **Explicit transactions**: Begin/commit/rollback support
- **Error handling**: Automatic rollback on errors

## Storage Format

### Log File Structure
```text
[Entry 1][Entry 2][Entry 3]...[Commit Marker][Entry 4]...
```

### Entry Format
```text
[Key Length: 4 bytes][Value Length: 4 bytes][Key][Value]
```

### Recovery Process
1. **First pass**: Scan entire log to find last commit marker position
2. **Second pass**: Replay entries up to last commit marker
3. **Skip uncommitted**: Ignore entries after last commit marker

### Special Keys
- `__schema__:<table_name>`: Table schema storage
- `__TX_COMMIT__`: Transaction commit markers (empty values)
- `<table>:<pk_values>`: Row data with primary key in key

## Memory Management

### Efficient Value Sharing
- **Arc<[u8]>**: Reference-counted byte arrays for zero-copy value access
- **Lazy undo logs**: Only allocate undo log when first write operation occurs
- **Streaming queries**: Process LIMIT queries without loading entire result set

### Memory Layout
```rust
type KeyMap = BTreeMap<Vec<u8>, Arc<[u8]>>;
type ScanResult<'a> = Box<dyn Iterator<Item = (Vec<u8>, Arc<[u8]>)> + 'a>;
```

## Configuration

### Engine Configuration
```rust
pub struct EngineConfig {
    pub max_key_size: usize,      // Default: 1KB
    pub max_value_size: usize,    // Default: 256KB
    pub sync_on_write: bool,      // Default: false (performance priority)
    pub auto_compact: bool,       // Default: true
}
```

### Performance Trade-offs
- **No fsync on writes**: Prioritizes performance over durability
- **Sync on commit**: Ensures transaction durability
- **Single-threaded**: Simplified design, no concurrent access

## Error Handling

### Error Types
```rust
pub enum Error {
    Io(std::io::Error),
    KeyTooLarge(usize),
    ValueTooLarge(usize),
    FileLocked(String),
    Other(String),
}
```

### Error Strategy
- **Graceful degradation**: Continue operation where possible
- **Transaction safety**: Always maintain ACID properties
- **Resource cleanup**: Proper cleanup in Drop implementations

## Feature Flags

### Development Features
```toml
[features]
dev = []  # Exposes low-level APIs for testing and benchmarks
```

When `dev` feature is enabled:
- Exposes `Engine`, `Executor`, `Parser` types
- Allows direct low-level API access
- Enables comprehensive testing and benchmarking

## Performance Characteristics

### Time Complexity
- **Point queries (PK)**: O(log n)
- **Range scans**: O(log n + k) where k is result size
- **Inserts/Updates**: O(log n)
- **Schema operations**: O(1) with cached schemas

### Space Complexity
- **Memory usage**: O(n) for data + O(schemas) for cached schemas
- **Disk usage**: Append-only log with periodic compaction
- **Value sharing**: Multiple references to same data use single memory allocation

## Concurrency Model

### Single-Threaded Design
- **No locking overhead**: Simplified memory model
- **File-level locking**: Prevents multiple processes from corrupting data
- **Deterministic performance**: Predictable execution without race conditions

### Thread Safety
- **Database**: Not thread-safe, use from single thread
- **File locking**: Prevents concurrent process access
- **Worker pattern**: Recommended for multi-threaded applications

## Crash Recovery

### Recovery Algorithm
1. **File integrity**: Check log file for corruption
2. **Find last commit**: Scan for last transaction commit marker
3. **Replay committed**: Apply all entries up to last commit
4. **Discard uncommitted**: Ignore incomplete transactions
5. **Rebuild keymap**: Reconstruct in-memory state

### Durability Guarantees
- **Committed transactions**: Survive crashes if commit marker written
- **Uncommitted transactions**: Lost on crash (expected behavior)
- **File corruption**: Graceful handling with partial recovery

## Future Considerations

### Potential Enhancements
- **Indexes**: Secondary indexes for non-PK queries
- **Compression**: Value compression for large data
- **Streaming**: Large result set streaming
- **Replication**: Write-ahead log based replication
- **Multi-version**: MVCC for better isolation

### Backwards Compatibility
- **File format**: Stable log format across versions
- **API stability**: High-level Database API remains stable
- **Migration**: Automatic schema migration support
