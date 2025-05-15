# TegDB

TegDB aims to address common database problems, including inadequate null handling and unintended type conversions. [^1]

The name TegridyDB (short for TegDB) is inspired by Tegridy Farm in South Park.

> **Design Note**: TegDB is specifically designed as a single-threaded database to optimize for resource-constrained environments like embedded systems and to maintain code simplicity. While modern databases often emphasize concurrent operations, TegDB prioritizes reliability, simplicity, and minimal resource usage for scenarios where these qualities are more important than parallel processing capabilities.

## Design

TegDB is a straightforward key-value store optimized for speed, reliability, and resource efficiency. Its architecture consists of two layers:

- In-memory layer: A single-threaded B-Tree offering fast operations with minimal memory overhead.
- Disk layer: A log file guaranteeing persistent, efficient writes.

It provides two core APIs:

- Engine API: A raw key-value interface.
- Database API: A transactional interface with serializable isolation.

Key components include Write-Ahead Logging, Snapshot Isolation, and automated log compaction with garbage collection.

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
+----------------+  +-------------------+
| Log File (Disk)|  | B-Tree            |
| (Persistent)   |  | (In-Memory)       |
+----------------+  +-------------------+
````

Its transaction isolation level is serializable, ensuring robust consistency. To guarantee this, TegDB employs Write-Ahead Logging (WAL) to record every change and uses Snapshot Isolation to deliver consistent database views.

When a transaction is committed, its changes are written to the log file and applied to memory; if aborted, the changes are discarded. Readers can capture a snapshot of the current memory state, ensuring consistency with the database at that moment.

Each transaction operates on a unique snapshot, providing clear visibility and a definitive commit history.

Upon startup, the database recovers its state by:

1. Checking the log file.
2. Continuing from the last assigned transaction ID to avoid duplicate IDs.
3. Rolling back any uncommitted transactions systematically.
4. Discarding uncommitted transactions during reads after checking transaction IDs and statuses.

If a transaction encounters an abort error, subsequent operations report an error and a rollback is required. The rollback process efficiently reverses changes made during the transaction using the raw KV Engine API.

### Detailed Rollback Process

Upon startup, the database recovers its state by checking the log file and continuing from the last assigned transaction ID to avoid duplicated ID assignment. Any uncommitted transactions are systematically rolled back to guarantee a consistent starting point. Uncommitted transactions are discarded during reads after checking transaction IDs and statuses.

If a transaction encounters an abort error, subsequent operations report an error and a rollback is required. For efficiency, changes are recorded incrementally during the transaction. The commit operation simply marks completion, and rollback has a similar approach. The rollback process is designed to be efficient, as it only needs to reverse the changes made during the transaction, rather than restoring the entire database state.

To roll back a transaction, the raw KV Engine API is used to reverse its changes, with markers to identify delete, update, and insert operations.

On demanding log compaction when shutting down the Engine to reduce storage overhead.

## Features

- **Transaction Support**: ACID-compatible transactions with robust isolation guarantees
- **Write-Ahead Logging (WAL)**: Guarantees durability and aids crash recovery
- **Async Support**: Efficient asynchronous operations
- **Benchmarking**: Includes tests comparing performance with other databases (sled, redb, SQLite)
- **Resource Efficiency**: Single-threaded design optimized for embedded systems and devices with limited resources

## Getting Started

To start using TegDB in your project, add the following dependency to your `Cargo.toml`:

```toml
[dependencies]
tegdb = "0.2.0"
```

## Usage Example

The following example demonstrates how to create a new database, start a transaction, perform operations, and commit the transaction using the single-threaded design.

```rust
use tegdb::Database;
use std::path::PathBuf;

#[tokio::main]
async fn main() {
    // Create database with thread-local reference
    let path = PathBuf::from("path/to/db");
    let db_ref = Database::new_ref(path).await;
    
    // Start a transaction
    let mut tx = {
        let db = db_ref.borrow();
        db.begin().await
    };
    
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

## Thread Safety

TegDB is designed to be used in a single-threaded environment. This decision prioritizes simplicity, reliability, and resource efficiency - especially important for embedded systems and devices with limited resources.

### Thread-Local Reference Types

TegDB provides thread-local reference types to ensure proper single-threaded access:

- `EngineRef`: A thread-local reference to an Engine instance
- `DatabaseRef`: A thread-local reference to a Database instance

```rust
// Creating a thread-local Engine reference
let engine_ref = Engine::new_ref(path);
let engine = engine_ref.borrow(); // borrow for operations

// Creating a thread-local Database reference
let db_ref = Database::new_ref(path).await;
let db = db_ref.borrow(); // borrow for operations
```

### Thread Safety Considerations

- The Engine is designed for use within a single thread
- All database operations should be performed from the same thread
- Use `Engine::new_ref()` and `Database::new_ref()` to create thread-local references
- For multi-threaded applications, implement a worker pattern where database operations are delegated to a dedicated thread

### Multi-Threaded Applications

For applications that need to access the database from multiple threads, implement a worker pattern:

1. Create a dedicated database thread
2. Send operations to that thread via channels
3. Return results to caller threads via response channels

See the `thread_safe_usage.rs` example for a complete implementation.

### Benefits of Single-Threaded Design

By enforcing a single-threaded design, TegDB provides several benefits:

1. Simplified codebase with fewer concurrency bugs
2. Reduced memory footprint without synchronization overhead
3. More predictable performance characteristics
4. Lower resource usage suitable for embedded environments

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
- [ ] Optimization of single-threaded operations

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

- [ ] Add comprehensive API documentation for all public types and functions
  - Document all public methods in `Database`, `Transaction`, and `Engine` structs
  - Add examples for common use cases and edge cases
  - Include performance characteristics and memory usage considerations
  - Document error conditions and recovery strategies

- [ ] Create detailed architecture documentation
  - Document the two-layer architecture (Engine API and Database API)
  - Explain the interaction between components (SkipList, WAL, Transaction Manager)
  - Detail the transaction isolation mechanisms
  - Document the garbage collection and log compaction processes

- [ ] Add configuration documentation
  - Document all configurable parameters in `constants.rs`
  - Explain the impact of different configuration values
  - Provide recommendations for different use cases
  - Include performance tuning guidelines

- [ ] Create troubleshooting guide
  - Document common issues and their solutions
  - Add debugging tips for transaction conflicts
  - Include performance optimization guidelines
  - Document recovery procedures for different failure scenarios

- [ ] Improve code examples
  - Add more comprehensive examples for different use cases
  - Create examples demonstrating transaction isolation
  - Add examples for error handling and recovery
  - Include examples for performance optimization

- [ ] Add design rationale documentation
  - Explain key design decisions and their trade-offs
  - Document the reasoning behind architectural choices
  - Compare with alternative approaches
  - Explain performance considerations

- [ ] Create deployment guide
  - Document system requirements
  - Provide installation instructions
  - Include configuration recommendations
  - Add monitoring and maintenance guidelines

- [ ] Add contribution guidelines
  - Document coding standards
  - Explain the testing requirements
  - Provide guidelines for documentation updates
  - Include pull request process

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
