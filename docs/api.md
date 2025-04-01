# TegDB API Documentation

## Overview

TegDB provides two main APIs:

- **Database API**: High-level transactional interface
- **Engine API**: Low-level key-value operations

## Database API

### Database

The `Database` struct provides the main interface for interacting with TegDB.

#### Database Methods

##### `new(path: &str) -> Result<Database>`

Creates a new database instance.

**Parameters:**

- `path`: Path to the database directory

**Returns:**

- `Result<Database>`: New database instance or error

**Example:**

```rust
let db = Database::new("path/to/db").await?;
```

##### `new_transaction() -> Result<Transaction>`

Creates a new transaction.

**Returns:**

- `Result<Transaction>`: New transaction instance or error

**Example:**

```rust
let mut tx = db.new_transaction().await?;
```

##### `shutdown() -> Result<()>`

Shuts down the database.

**Returns:**

- `Result<()>`: Success or error

**Example:**

```rust
db.shutdown().await?;
```

### Transaction

The `Transaction` struct represents a database transaction.

#### Transaction Methods

##### `insert(key: &[u8], value: &[u8]) -> Result<()>`

Inserts a key-value pair.

**Parameters:**

- `key`: Key to insert
- `value`: Value to insert

**Returns:**

- `Result<()>`: Success or error

**Example:**

```rust
tx.insert(b"key", b"value").await?;
```

##### `update(key: &[u8], value: &[u8]) -> Result<()>`

Updates an existing key-value pair.

**Parameters:**

- `key`: Key to update
- `value`: New value

**Returns:**

- `Result<()>`: Success or error

**Example:**

```rust
tx.update(b"key", b"new_value").await?;
```

##### `select(key: &[u8]) -> Result<Option<Vec<u8>>>`

Retrieves a value by key.

**Parameters:**

- `key`: Key to look up

**Returns:**

- `Result<Option<Vec<u8>>>`: Value if found, None if not found, or error

**Example:**

```rust
let value = tx.select(b"key").await?;
```

##### `delete(key: &[u8]) -> Result<()>`

Deletes a key-value pair.

**Parameters:**

- `key`: Key to delete

**Returns:**

- `Result<()>`: Success or error

**Example:**

```rust
tx.delete(b"key").await?;
```

##### `commit() -> Result<()>`

Commits the transaction.

**Returns:**

- `Result<()>`: Success or error

**Example:**

```rust
tx.commit().await?;
```

## Engine API

### Engine

The `Engine` struct provides low-level key-value operations.

#### Engine Methods

##### `new(path: &str) -> Result<Engine>`

Creates a new engine instance.

**Parameters:**

- `path`: Path to the engine directory

**Returns:**

- `Result<Engine>`: New engine instance or error

**Example:**

```rust
let engine = Engine::new("path/to/engine").await?;
```

##### `get(key: &[u8]) -> Result<Option<Vec<u8>>>`

Retrieves a value by key.

**Parameters:**

- `key`: Key to look up

**Returns:**

- `Result<Option<Vec<u8>>>`: Value if found, None if not found, or error

**Example:**

```rust
let value = engine.get(b"key").await?;
```

##### `set(key: &[u8], value: &[u8]) -> Result<()>`

Sets a key-value pair.

**Parameters:**

- `key`: Key to set
- `value`: Value to set

**Returns:**

- `Result<()>`: Success or error

**Example:**

```rust
engine.set(b"key", b"value").await?;
```

##### `del(key: &[u8]) -> Result<()>`

Deletes a key-value pair.

**Parameters:**

- `key`: Key to delete

**Returns:**

- `Result<()>`: Success or error

**Example:**

```rust
engine.del(b"key").await?;
```

##### `scan(start: &[u8], end: &[u8]) -> Result<Vec<(Vec<u8>, Vec<u8>)>>`

Scans key-value pairs in a range.

**Parameters:**

- `start`: Start key (inclusive)
- `end`: End key (exclusive)

**Returns:**

- `Result<Vec<(Vec<u8>, Vec<u8>)>>`: Vector of key-value pairs or error

**Example:**

```rust
let pairs = engine.scan(b"a", b"z").await?;
```

##### `reverse_scan(start: &[u8], end: &[u8]) -> Result<Vec<(Vec<u8>, Vec<u8>)>>`

Scans key-value pairs in reverse order.

**Parameters:**

- `start`: Start key (inclusive)
- `end`: End key (exclusive)

**Returns:**

- `Result<Vec<(Vec<u8>, Vec<u8>)>>`: Vector of key-value pairs or error

**Example:**

```rust
let pairs = engine.reverse_scan(b"z", b"a").await?;
```

## Error Types

### Error

The `Error` struct represents various error conditions.

#### Variants

- `Other(String)`: General error
- `InvalidInput(String)`: Invalid input error
- `NotFound`: Key not found
- `AlreadyExists`: Key already exists
- `WouldBlock`: Operation would block
- `Interrupted`: Operation interrupted

**Example:**

```rust
match result {
    Ok(value) => println!("Success: {:?}", value),
    Err(Error::NotFound) => println!("Key not found"),
    Err(Error::InvalidInput(msg)) => println!("Invalid input: {}", msg),
    Err(e) => println!("Other error: {:?}", e),
}
```

## Performance Considerations

### Memory Usage

- Transaction snapshots consume memory
- SkipList nodes have overhead
- WAL buffers use memory

### Disk Usage

- WAL files grow with write operations
- Log compaction reduces disk usage
- Temporary files during compaction

### Concurrency

- Multiple readers can access simultaneously
- Writers are serialized
- Transaction conflicts may occur

## Recovery Strategies

### Transaction Abort

1. Roll back changes
2. Release locks
3. Clean up resources

### Crash Recovery

1. Read WAL
2. Replay committed transactions
3. Roll back uncommitted transactions

### Lock Timeout

1. Release locks
2. Abort transaction
3. Notify client
