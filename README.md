# TegDB

TegDB aims to address common database problems, including inadequate null handling and unintended type conversions. [^1]

The name TegridyDB (short for TegDB) is inspired by Tegridy Farm in South Park.

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

Its transaction isolation level is serializable, ensuring robust consistency. To guarantee this, TegDB employs Write-Ahead Logging (WAL) to record every change and uses Snapshot Isolation to deliver consistent database views.

When a transaction is committed, its changes are written to the log file and applied to memory; if aborted, the changes are discarded. Readers can capture a snapshot of the current memory state, ensuring consistency with the database at that moment.

Each transaction operates on a unique snapshot, providing clear visibility and a definitive commit history.

Upon startup, the database recovers its state by:

1. Checking the log file.
2. Continuing from the last assigned transaction ID to avoid duplicate IDs.
3. Asynchronously rolling back any uncommitted transactions when accessed by other transactions.
4. Discarding uncommitted transactions during reads after checking transaction IDs and statuses.

If a transaction encounters an abort error, subsequent operations report an error and a rollback is required. The rollback process efficiently reverses changes made during the transaction using the raw KV Engine API.

### Detailed Rollback Process

Upon startup, the database recovers its state by checking the log file and continuing from the last assigned transaction ID to avoid duplicated ID assignment. Any uncommitted transactions are asynchronously rolled back later when other transactions access them, to guarantee a consistent starting point. Uncommitted transactions are discarded during reads after checking transaction IDs and statuses.

If a transaction encounters an abort error, subsequent operations report an error and a rollback is required. For efficiency, changes are recorded incrementally during the transaction. The commit operation simply marks completion, and rollback has a similar approach. The rollback process is designed to be efficient, as it only needs to reverse the changes made during the transaction, rather than restoring the entire database state.

To roll back a transaction, the raw KV Engine API is used to reverse its changes, with markers to identify delete, update, and insert operations.

Real-time log compaction and garbage collection remove outdated data and reduce storage overhead. 

- **Log Compaction**: Shifts new writes to a fresh log file.
- **Background Processing**: Processes the old log file in the background to reduce overall database size.

Real-time log compaction and garbage collection remove outdated data and reduce storage overhead.

## Features

- **Transaction Support**: ACID-compatible transactions with robust isolation guarantees
- **Write-Ahead Logging (WAL)**: Guarantees durability and aids crash recovery
- **Snapshot Isolation**: Preserves consistent database states
- **Async Support**: Employs Tokio for efficient asynchronous operations
- **Benchmarking**: Includes tests comparing performance with other databases (sled, redb, SQLite)

## Getting Started

To start using TegDB in your project, add the following dependency to your `Cargo.toml`:

```toml
[dependencies]
tegdb = "0.2.0"
```

## Usage Example

The following example demonstrates how to create a new database, start a transaction, perform operations, and commit the transaction.

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

Benchmarking is crucial to evaluate the performance and efficiency of TegDB compared to other databases. The project includes comprehensive benchmarks comparing performance against:

- sled
- SQLite
- redb
- SQLite

Run the benchmarks with:

```bash
cargo bench
```

## Rules

The following rules are established to ensure the development of TegDB remains straightforward, reliable, and maintainable:

1. Keep it simple.
2. Use the standard library whenever possible.
3. Prioritize correctness and reliability.

## TODO

### Architecture Improvements

- [ ] Better separation between Engine API and Database API layers
- [ ] Clearer documentation of the two-layer architecture implementation
- [ ] Better organization of code structure to match architectural layers
- [ ] Separation of lock manager and MVCC into dedicated modules

### Transaction Management

- [ ] More robust testing for serializable isolation guarantees
- [ ] Optimization of transaction rollback process
- [ ] Better documentation of transaction recovery during startup
- [ ] More efficient snapshot management during transactions

### Write-Ahead Logging (WAL)

- [ ] Improved documentation of WAL's role in crash recovery
- [ ] More efficient log compaction process
- [ ] Better implementation of background processing for log compaction
- [ ] Optimization of garbage collection process

### Documentation

- [ ] Comprehensive documentation of implementation details
- [ ] Better documentation of component relationships
- [ ] More detailed API documentation
- [ ] Documentation of configuration options

### Error Handling

- [ ] More robust and consistent error handling across the codebase
- [ ] Better handling of abort errors and edge cases
- [ ] Improved error reporting and recovery mechanisms

### Testing

- [ ] Comprehensive tests for ACID properties
- [ ] Expanded benchmarking tests
- [ ] Crash recovery tests
- [ ] Performance optimization tests

### Dependencies

- [ ] Review and optimize external dependencies
- [ ] Replace some dependencies with standard library implementations
- [ ] Update dependency versions where needed

### Configuration

- [ ] Add configuration options for performance tuning
- [ ] Make hardcoded values configurable
- [ ] Add configuration documentation

### API Design

- [ ] More ergonomic and user-friendly API
- [ ] Optimization for common use cases
- [ ] Better API documentation

## License

This project is licensed under the AGPL-3.0 License. The AGPL-3.0 License is a strong copyleft license that ensures any modifications to the code are shared with the community. It was chosen to promote open collaboration and ensure that improvements to the project remain freely available.

See the [LICENSE](LICENSE) file for details.

## Contributing

Contributions are welcome! Please feel free to submit a Pull Request.
