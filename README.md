# TegDB

The name TegridyDB (short for TegDB) is inspired by Tegridy Farm in South Park. It aims to address common database problems, including inadequate null handling and unintended type conversions.

## Design

TegDB is a straightforward key-value store optimized for speed and reliability. Its architecture consists of two layers:

- In-memory layer: A concurrent SkipList offering ultra-fast, parallel operations.
- Disk layer: A log file guaranteeing persistent, efficient writes.

It provides two core APIs:

- Engine API: A raw key-value interface.
- Database API: A transactional interface with serializable isolation.

Key components include Write-Ahead Logging, Snapshot Isolation, a lock manager with MVCC, and automated log compaction with garbage collection.

Below is an architectural overview:

````text
        +----------------+
        |  Database API  |
        +-------+--------+
                |
                v
        +----------------+
        |   Engine API   |
        +---+-------+----+
            |       |
            v       v
+----------------+  +----------------------+
| Log File (Disk)|  | Concurrent SkipList  |
| (Persistent)   |  | (In-Memory)          |
+----------------+  +----------------------+
````

Its transaction isolation level is serializable, ensuring robust consistency. To guarantee this, TegDB employs Write-Ahead Logging (WAL) to record every change and uses Snapshot Isolation to deliver consistent database views. When a transaction is committed, its changes are written to the log file and applied to memory; if aborted, the changes are discarded. Readers can capture a snapshot of the current memory state, ensuring consistency with the database at that moment. Each transaction operates on a unique snapshot, providing clear visibility and a definitive commit history.

Transactions in TegDB are initiated by assigning each transaction a unique transaction ID. Operating under serializable isolation, every transaction benefits from Multi-Version Concurrency Control (MVCC), ensuring that all operations view a consistent snapshot of the data.

A dedicated lock manager monitors key accesses and aborts any transaction when conflicting key orders are detected, thereby maintaining data integrity.

Upon startup, the database recovers its state by replaying the log file. During this process, any uncommitted transactions are asynchronously rolled back to guarantee a consistent starting point.

Real-time log compaction and garbage collection remove outdated data and reduce storage overhead.

If a transaction encounters an abort error, subsequent operations halt and a rollback is required. For efficiency, changes are recorded incrementally during the transaction, with the commit operation simply marking completion.

Upon startup, the database replays the log file to recover its state, continuing from the last assigned transaction ID. Uncommitted transactions are discarded during reads after checking transaction IDs and statuses.

Rolling back a transaction is achieved by invoking the raw KV Engine API to reverse its changes—with distinct markers to differentiate delete, update, and insert operations. GC identifies the oldest active transaction and removes data that is no longer accessible, including remnants from failed, uncommitted transactions.

Compaction shifts new writes to a fresh log file while processing the old one online to reduce overall database size.

## Features

- **Transaction Support**: ACID-compatible transactions with robust isolation guarantees
- **Write-Ahead Logging (WAL)**: Guarantees durability and aids crash recovery
- **Snapshot Isolation**: Preserves consistent database states
- **Async Support**: Employs Tokio for efficient asynchronous operations
- **Benchmarking**: Includes tests comparing performance with other databases (sled, redb, SQLite)

## Getting Started

Add this to your `Cargo.toml`:

```toml
[dependencies]
tegdb = "0.2.0"
```

## Usage Example

```rust
use tegdb::Database;

#[tokio::main]
async fn main() {
    let db = Database::new("path/to/db").await.unwrap();
    
    // Start a transaction
    let mut tx = db.begin().await.unwrap();
    
    // Perform operations
    tx.put("key", "value").await.unwrap();
    
    // Commit the transaction
    tx.commit().await.unwrap();
}
```

## Benchmarks

The project includes comprehensive benchmarks comparing performance against:

- sled
- redb
- SQLite

Run the benchmarks with:

```bash
cargo bench
```

## Rules

1. Keep it simple.
2. Use the standard library whenever possible.
3. Prioritize correctness and reliability.
4. Maintain strong performance without sacrificing safety.

## License

This project is licensed under the AGPL-3.0 License - see the [LICENSE](LICENSE) file for details.

## Contributing

Contributions are welcome! Please feel free to submit a Pull Request.
