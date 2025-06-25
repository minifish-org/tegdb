# TegDB

TegDB is a lightweight, embedded database engine with a SQL-like interface designed for simplicity, performance, and reliability. It provides ACID transactions, crash recovery, and efficient key-value storage.

> **Design Philosophy**: TegDB prioritizes simplicity and reliability over complexity. It uses a single-threaded design to eliminate concurrency bugs, reduce memory overhead, and provide predictable performance - making it ideal for embedded systems and applications where resource efficiency matters more than parallel processing.

## Architecture Overview

TegDB implements a clean layered architecture with four distinct layers:

```text
┌─────────────────────────────────────────────────────────────┐
│                    Database API                             │
│        (SQLite-like interface with schema caching)         │
├─────────────────────────────────────────────────────────────┤
│                    SQL Executor                            │
│    (Query optimization and statement execution)            │
├─────────────────────────────────────────────────────────────┤
│                    SQL Parser                              │
│         (nom-based SQL parsing to AST)                     │
├─────────────────────────────────────────────────────────────┤
│                    Storage Engine                          │
│  (Key-value store with WAL and transaction support)        │
└─────────────────────────────────────────────────────────────┘
```

### Core Components

- **Storage Engine**: BTreeMap-based in-memory storage with append-only log persistence
- **Transaction System**: Write-through transactions with undo logging and commit markers
- **SQL Support**: Full SQL parser and executor supporting DDL and DML operations
- **Index-Organized Tables**: Primary key optimization with direct key lookups
- **Schema Caching**: Database-level schema caching for improved performance
- **Crash Recovery**: WAL-based recovery using transaction commit markers

## Key Features

### 🚀 **Performance**
- Zero-copy value sharing with Arc<[u8]>
- Primary key optimized queries (O(log n) lookups)
- Streaming query processing with early LIMIT termination
- Efficient binary serialization

### 🔒 **ACID Transactions**
- Atomicity: All-or-nothing transaction execution
- Consistency: Schema validation and constraint enforcement  
- Isolation: Write-through with snapshot-like behavior
- Durability: Write-ahead logging with commit markers

### 🛡️ **Reliability**
- Crash recovery from write-ahead log
- File locking prevents concurrent access corruption
- Graceful handling of partial writes and corruption
- Automatic rollback on transaction drop

### 📦 **Simple Design**
- Single-threaded architecture eliminates race conditions
- Minimal dependencies (only `fs2` for file locking)
- Clean separation of concerns across layers
- Extensive test coverage including ACID compliance

## Quick Start

Add TegDB to your `Cargo.toml`:

```toml
[dependencies]
tegdb = "0.2.0"
```

### Basic Usage

```rust
use tegdb::{Database, Result};

fn main() -> Result<()> {
    // Open or create a database
    let mut db = Database::open("my_app.db")?;
    
    // Create a table
    db.execute("CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT, age INTEGER)")?;
    
    // Insert data
    db.execute("INSERT INTO users (id, name, age) VALUES (1, 'Alice', 30)")?;
    db.execute("INSERT INTO users (id, name, age) VALUES (2, 'Bob', 25)")?;
    
    // Query data
    let result = db.query("SELECT name, age FROM users WHERE age > 25")?;
    
    for row in result.iter() {
        let name = row.get("name").unwrap();
        let age = row.get("age").unwrap();
        println!("User: {:?}, Age: {:?}", name, age);
    }
    
    Ok(())
}
```

### Transaction Example

```rust
use tegdb::{Database, Result};

fn transfer_funds(db: &mut Database, from_id: i64, to_id: i64, amount: i64) -> Result<()> {
    // Begin explicit transaction
    let mut tx = db.begin_transaction()?;
    
    // Debit from source account
    tx.execute("UPDATE accounts SET balance = balance - ? WHERE id = ?")?;
    
    // Credit to destination account  
    tx.execute("UPDATE accounts SET balance = balance + ? WHERE id = ?")?;
    
    // Commit the transaction (or it will auto-rollback on drop)
    tx.commit()?;
    
    Ok(())
}
```

## SQL Support

TegDB supports a comprehensive subset of SQL:

### Data Definition Language (DDL)
```sql
-- Create tables with constraints
CREATE TABLE products (
    id INTEGER PRIMARY KEY,
    name TEXT NOT NULL,
    price REAL,
    category TEXT
);

-- Drop tables
DROP TABLE IF EXISTS old_table;
```

### Data Manipulation Language (DML)
```sql
-- Insert single or multiple rows
INSERT INTO products (id, name, price) VALUES (1, 'Widget', 19.99);
INSERT INTO products (id, name, price) VALUES 
    (2, 'Gadget', 29.99),
    (3, 'Tool', 39.99);

-- Update with conditions
UPDATE products SET price = 24.99 WHERE name = 'Widget';

-- Delete with conditions
DELETE FROM products WHERE price < 20.00;

-- Query with filtering, ordering, and limits
SELECT name, price FROM products 
WHERE category = 'Electronics' 
ORDER BY price DESC 
LIMIT 10;
```

### Transaction Control
```sql
BEGIN;
UPDATE accounts SET balance = balance - 100 WHERE id = 1;
UPDATE accounts SET balance = balance + 100 WHERE id = 2;
COMMIT;
-- or ROLLBACK;
```

### Supported Data Types
- `INTEGER` - 64-bit signed integers
- `REAL` - 64-bit floating point numbers  
- `TEXT` - UTF-8 strings
- `BLOB` - Binary data
- `NULL` - Null values

## Performance Characteristics

### Time Complexity
- **Primary key lookups**: O(log n)
- **Range scans**: O(log n + k) where k = result size
- **Inserts/Updates/Deletes**: O(log n)
- **Schema operations**: O(1) with caching

### Memory Usage
- **In-memory index**: BTreeMap with Arc-shared values
- **Zero-copy reads**: Multiple references share same memory
- **Lazy allocation**: Undo logs only allocated when needed
- **Streaming queries**: LIMIT processed without loading full result

### Storage Format
- **Append-only log**: Fast writes, no seek overhead
- **Binary serialization**: Compact data representation
- **Automatic compaction**: Reclaims space from old entries
- **Crash recovery**: Replay from last commit marker

## Configuration

```rust
use tegdb::EngineConfig;

let config = EngineConfig {
    max_key_size: 1024,        // 1KB max key size
    max_value_size: 256 * 1024, // 256KB max value size  
    sync_on_write: false,       // Performance over durability
    auto_compact: true,         // Auto-compact on open
};

// Note: Custom config requires dev feature and low-level API
```

## Advanced Usage

### Low-Level Engine API

For advanced use cases, enable the `dev` feature to access low-level APIs:

```toml
[dependencies]
tegdb = { version = "0.2", features = ["dev"] }
```

```rust
use tegdb::{Engine, EngineConfig};

// Direct key-value operations
let mut engine = Engine::new("data.db".into())?;
engine.set(b"key", b"value".to_vec())?;
let value = engine.get(b"key");

// Transaction control
let mut tx = engine.begin_transaction();
tx.set(b"key1", b"value1".to_vec())?;
tx.set(b"key2", b"value2".to_vec())?;
tx.commit()?;
```

## Benchmarks

Run performance benchmarks against other embedded databases:

```bash
cargo bench --features dev
```

Included benchmarks compare against:
- SQLite
- sled  
- redb

## Development

### Building

```bash
# Standard build
cargo build

# With development features
cargo build --features dev

# Run tests
cargo test --features dev

# Run benchmarks  
cargo bench --features dev
```

### Testing

TegDB includes comprehensive tests covering:
- ACID transaction properties
- Crash recovery scenarios  
- SQL parsing and execution
- Performance benchmarks
- Edge cases and error conditions

## Design Principles

1. **Simplicity First**: Prefer simple, understandable solutions
2. **Reliability**: Prioritize correctness over performance optimizations
3. **Standard Library**: Use std library when possible to minimize dependencies
4. **Single Threaded**: Eliminate concurrency complexity and bugs
5. **Resource Efficient**: Optimize for memory and CPU usage

## Architecture Details

See [ARCHITECTURE.md](ARCHITECTURE.md) for detailed information about:
- Layer-by-layer implementation details
- Storage format and recovery mechanisms  
- Memory management and performance optimizations
- Transaction system and ACID guarantees
- Query optimization and execution strategies

## Limitations

### Current Limitations
- **Single-threaded**: No concurrent access support
- **No secondary indexes**: Only primary key optimization
- **Limited SQL**: Subset of full SQL standard
- **No foreign keys**: Basic constraint support only
- **No joins**: Single table queries only

### Future Enhancements
- Secondary index support
- JOIN operation support  
- More SQL features (subqueries, aggregation)
- Compression for large values
- Streaming for very large result sets

## License

Licensed under AGPL-3.0. See [LICENSE](LICENSE) for details.

The AGPL-3.0 ensures that any modifications to TegDB remain open source and available to the community.

## Contributing

Contributions welcome! Please:

1. Follow the design principles above
2. Include comprehensive tests
3. Update documentation for new features
4. Ensure benchmarks still pass

See [CONTRIBUTING.md](CONTRIBUTING.md) for detailed guidelines.
