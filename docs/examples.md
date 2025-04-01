# TegDB Code Examples

## Overview

This document provides comprehensive examples demonstrating various features and use cases of TegDB.

## Basic Usage

### Database Creation

```rust
use tegdb::Database;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Create a new database
    let db = Database::new("path/to/db").await?;
    
    // Use the database...
    
    Ok(())
}
```

### Basic Operations

```rust
use tegdb::Database;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let db = Database::new("path/to/db").await?;
    
    // Insert a key-value pair
    db.put("key", "value").await?;
    
    // Read a value
    let value = db.get("key").await?;
    
    // Delete a key
    db.delete("key").await?;
    
    Ok(())
}
```

## Transaction Examples

### Transaction Isolation

```rust
use tegdb::Database;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let db = Database::new("path/to/db").await?;
    
    // Start a transaction
    let mut tx1 = db.new_transaction().await?;
    let mut tx2 = db.new_transaction().await?;
    
    // Transaction 1 writes
    tx1.put("key", "value1").await?;
    
    // Transaction 2 reads (sees old value)
    let value = tx2.get("key").await?;
    
    // Commit transaction 1
    tx1.commit().await?;
    
    // Transaction 2 still sees old value
    let value = tx2.get("key").await?;
    
    Ok(())
}
```

### Transaction Rollback

```rust
use tegdb::Database;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let db = Database::new("path/to/db").await?;
    
    // Start a transaction
    let mut tx = db.new_transaction().await?;
    
    // Perform operations
    tx.put("key1", "value1").await?;
    tx.put("key2", "value2").await?;
    
    // Roll back on error
    if let Err(e) = tx.put("key3", "value3").await {
        // Roll back all changes
        tx.rollback().await?;
        return Err(e.into());
    }
    
    // Commit if successful
    tx.commit().await?;
    
    Ok(())
}
```

### Concurrent Transactions

```rust
use tegdb::Database;
use std::sync::Arc;
use tokio::task;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let db = Arc::new(Database::new("path/to/db").await?);
    
    // Spawn multiple transactions
    let mut handles = vec![];
    for i in 0..10 {
        let db = db.clone();
        handles.push(task::spawn(async move {
            let mut tx = db.new_transaction().await?;
            tx.put(format!("key{}", i), format!("value{}", i)).await?;
            tx.commit().await
        }));
    }
    
    // Wait for all transactions
    for handle in handles {
        handle.await??;
    }
    
    Ok(())
}
```

## Batch Operations

### Batch Insert

```rust
use tegdb::Database;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let db = Database::new("path/to/db").await?;
    
    // Start a transaction
    let mut tx = db.new_transaction().await?;
    
    // Batch insert
    let batch = vec![
        ("key1", "value1"),
        ("key2", "value2"),
        ("key3", "value3"),
    ];
    
    for (key, value) in batch {
        tx.put(key, value).await?;
    }
    
    // Commit all changes
    tx.commit().await?;
    
    Ok(())
}
```

### Batch Update

```rust
use tegdb::Database;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let db = Database::new("path/to/db").await?;
    
    // Start a transaction
    let mut tx = db.new_transaction().await?;
    
    // Batch update
    let updates = vec![
        ("key1", "new_value1"),
        ("key2", "new_value2"),
        ("key3", "new_value3"),
    ];
    
    for (key, value) in updates {
        if let Some(_) = tx.get(key).await? {
            tx.put(key, value).await?;
        }
    }
    
    // Commit all changes
    tx.commit().await?;
    
    Ok(())
}
```

## Error Handling

### Transaction Error Handling

```rust
use tegdb::Database;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let db = Database::new("path/to/db").await?;
    
    // Start a transaction
    let mut tx = db.new_transaction().await?;
    
    // Handle errors
    match tx.put("key", "value").await {
        Ok(_) => println!("Operation successful"),
        Err(e) => {
            println!("Error: {}", e);
            tx.rollback().await?;
        }
    }
    
    Ok(())
}
```

### Recovery Example

```rust
use tegdb::Database;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let db = Database::new("path/to/db").await?;
    
    // Start a transaction
    let mut tx = db.new_transaction().await?;
    
    // Perform operations
    tx.put("key1", "value1").await?;
    tx.put("key2", "value2").await?;
    
    // Simulate crash
    std::process::exit(1);
    
    // Recovery happens automatically on next start
    Ok(())
}
```

## Performance Optimization

### Efficient Batch Processing

```rust
use tegdb::Database;
use futures::stream::{self, StreamExt};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let db = Database::new("path/to/db").await?;
    
    // Process in chunks
    let chunk_size = 1000;
    let mut tx = db.new_transaction().await?;
    
    for chunk in data.chunks(chunk_size) {
        for (key, value) in chunk {
            tx.put(key, value).await?;
        }
        tx.commit().await?;
        tx = db.new_transaction().await?;
    }
    
    Ok(())
}
```

### Optimized Read Operations

```rust
use tegdb::Database;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let db = Database::new("path/to/db").await?;
    
    // Use snapshot for consistent reads
    let mut tx = db.new_transaction().await?;
    
    // Read multiple values
    let values = vec![
        tx.get("key1").await?,
        tx.get("key2").await?,
        tx.get("key3").await?,
    ];
    
    Ok(())
}
```

## Advanced Features

### Range Scans

```rust
use tegdb::Database;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let db = Database::new("path/to/db").await?;
    
    // Scan a range of keys
    let mut tx = db.new_transaction().await?;
    let range = tx.scan("a", "z").await?;
    
    for (key, value) in range {
        println!("{}: {}", key, value);
    }
    
    Ok(())
}
```

### Snapshot Isolation

```rust
use tegdb::Database;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let db = Database::new("path/to/db").await?;
    
    // Create a snapshot
    let mut tx = db.new_transaction().await?;
    
    // Read consistent view
    let value1 = tx.get("key1").await?;
    
    // Other transactions can modify data
    let mut tx2 = db.new_transaction().await?;
    tx2.put("key1", "new_value").await?;
    tx2.commit().await?;
    
    // Original transaction still sees old value
    let value2 = tx.get("key1").await?;
    assert_eq!(value1, value2);
    
    Ok(())
}
```

## Best Practices

### Resource Management

```rust
use tegdb::Database;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let db = Database::new("path/to/db").await?;
    
    // Use scope for automatic cleanup
    {
        let mut tx = db.new_transaction().await?;
        tx.put("key", "value").await?;
        tx.commit().await?;
    }
    
    // Resources are automatically cleaned up
    
    Ok(())
}
```

### Error Recovery

```rust
use tegdb::Database;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let db = Database::new("path/to/db").await?;
    
    // Implement retry logic
    let mut retries = 0;
    let max_retries = 3;
    
    while retries < max_retries {
        let mut tx = db.new_transaction().await?;
        
        match tx.put("key", "value").await {
            Ok(_) => {
                tx.commit().await?;
                break;
            }
            Err(e) => {
                retries += 1;
                if retries == max_retries {
                    return Err(e.into());
                }
            }
        }
    }
    
    Ok(())
}
``` 