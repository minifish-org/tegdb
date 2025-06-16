# Database Design

## Suggested Architecture Organization

### 1. Create Unified Database Connection Interface

````rust
// src/database.rs
use crate::{Engine, executor::Executor, parser::parse_sql, Result};

/// Database connection, similar to sqlite::Connection
pub struct Database {
    engine: Engine,
}

impl Database {
    /// Create or open database
    pub fn open<P: AsRef<std::path::Path>>(path: P) -> Result<Self> {
        let engine = Engine::new(path.as_ref().to_path_buf())?;
        Ok(Self { engine })
    }
    
    /// Execute SQL statement, return number of affected rows
    pub fn execute(&mut self, sql: &str) -> Result<usize> {
        let (_, statement) = parse_sql(sql)
            .map_err(|e| crate::Error::Other(format!("SQL parse error: {:?}", e)))?;
        
        let transaction = self.engine.begin_transaction();
        let mut executor = Executor::new(transaction);
        
        let result = executor.execute(statement)?;
        executor.transaction_mut().commit()?;
        
        match result {
            crate::executor::ResultSet::Insert { rows_affected } => Ok(rows_affected),
            crate::executor::ResultSet::Update { rows_affected } => Ok(rows_affected),
            crate::executor::ResultSet::Delete { rows_affected } => Ok(rows_affected),
            _ => Ok(0),
        }
    }
    
    /// Execute query, return result set
    pub fn query(&mut self, sql: &str) -> Result<QueryResult> {
        let (_, statement) = parse_sql(sql)
            .map_err(|e| crate::Error::Other(format!("SQL parse error: {:?}", e)))?;
        
        let transaction = self.engine.begin_transaction();
        let mut executor = Executor::new(transaction);
        
        let result = executor.execute(statement)?;
        executor.transaction_mut().commit()?;
        
        match result {
            crate::executor::ResultSet::Select { columns, rows } => {
                Ok(QueryResult { columns, rows })
            }
            _ => Err(crate::Error::Other("Expected SELECT result".to_string())),
        }
    }
    
    /// Begin transaction
    pub fn begin_transaction(&mut self) -> Result<Transaction> {
        let tx = self.engine.begin_transaction();
        Ok(Transaction::new(tx))
    }
}
````

### 2. Create Query Result Types

````rust
// src/database.rs continued
use crate::parser::SqlValue;

/// Query result, similar to sqlite result set
#[derive(Debug)]
pub struct QueryResult {
    pub columns: Vec<String>,
    pub rows: Vec<Vec<SqlValue>>,
}

impl QueryResult {
    /// Get column names
    pub fn columns(&self) -> &[String] {
        &self.columns
    }
    
    /// Get all rows
    pub fn rows(&self) -> &[Vec<SqlValue>] {
        &self.rows
    }
    
    /// Iterate over rows
    pub fn iter(&self) -> impl Iterator<Item = Row> {
        self.rows.iter().enumerate().map(move |(index, row)| {
            Row {
                columns: &self.columns,
                values: row,
                index,
            }
        })
    }
}

/// Single row data
pub struct Row<'a> {
    columns: &'a [String],
    values: &'a [SqlValue],
    index: usize,
}

impl<'a> Row<'a> {
    /// Get value by column name
    pub fn get(&self, column: &str) -> Option<&SqlValue> {
        self.columns.iter()
            .position(|c| c == column)
            .and_then(|i| self.values.get(i))
    }
    
    /// Get value by index
    pub fn get_by_index(&self, index: usize) -> Option<&SqlValue> {
        self.values.get(index)
    }
    
    /// Get row index
    pub fn index(&self) -> usize {
        self.index
    }
}
````

### 3. Create Transaction Interface

````rust
// src/database.rs continued
/// Database transaction
pub struct Transaction<'a> {
    transaction: crate::Transaction<'a>,
    executor: Executor<'a>,
}

impl<'a> Transaction<'a> {
    fn new(transaction: crate::Transaction<'a>) -> Self {
        let executor = Executor::new(transaction);
        Self { transaction, executor }
    }
    
    /// Execute SQL in transaction
    pub fn execute(&mut self, sql: &str) -> Result<usize> {
        let (_, statement) = parse_sql(sql)
            .map_err(|e| crate::Error::Other(format!("SQL parse error: {:?}", e)))?;
        
        let result = self.executor.execute(statement)?;
        
        match result {
            crate::executor::ResultSet::Insert { rows_affected } => Ok(rows_affected),
            crate::executor::ResultSet::Update { rows_affected } => Ok(rows_affected),
            crate::executor::ResultSet::Delete { rows_affected } => Ok(rows_affected),
            _ => Ok(0),
        }
    }
    
    /// Execute query in transaction
    pub fn query(&mut self, sql: &str) -> Result<QueryResult> {
        let (_, statement) = parse_sql(sql)
            .map_err(|e| crate::Error::Other(format!("SQL parse error: {:?}", e)))?;
        
        let result = self.executor.execute(statement)?;
        
        match result {
            crate::executor::ResultSet::Select { columns, rows } => {
                Ok(QueryResult { columns, rows })
            }
            _ => Err(crate::Error::Other("Expected SELECT result".to_string())),
        }
    }
    
    /// Commit transaction
    pub fn commit(self) -> Result<()> {
        self.transaction.commit()
    }
    
    /// Rollback transaction
    pub fn rollback(self) -> Result<()> {
        self.transaction.rollback()
    }
}
````

### 4. Modify lib.rs Exports

````rust
// src/lib.rs
mod engine;
mod error;
pub mod parser;
pub mod executor;
mod database;

pub use engine::{Engine, EngineConfig, Entry};
pub use error::{Error, Result};
pub use database::{Database, QueryResult, Row, Transaction};

// Keep low-level API for advanced users
pub mod low_level {
    pub use crate::engine::{Engine, Transaction as EngineTransaction};
    pub use crate::executor::{Executor, ResultSet};
    pub use crate::parser::{parse_sql, Statement, SqlValue};
}
````

### 5. Usage Example

````rust
// examples/sqlite_like_usage.rs
use tegdb::{Database, Result};

fn main() -> Result<()> {
    // Create/open database, similar to SQLite
    let mut db = Database::open("my_database.db")?;
    
    // Create table
    db.execute("CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT NOT NULL, age INTEGER)")?;
    
    // Insert data
    let affected = db.execute("INSERT INTO users (id, name, age) VALUES (1, 'Alice', 30)")?;
    println!("Inserted {} rows", affected);
    
    // Batch insert
    db.execute("INSERT INTO users (id, name, age) VALUES (2, 'Bob', 25), (3, 'Carol', 35)")?;
    
    // Query data
    let result = db.query("SELECT * FROM users WHERE age > 25")?;
    
    println!("Columns: {:?}", result.columns());
    for row in result.iter() {
        println!("User: {} (ID: {:?}, Age: {:?})", 
            row.get("name").unwrap(),
            row.get("id").unwrap(),
            row.get("age").unwrap()
        );
    }
    
    // Use transaction
    {
        let mut tx = db.begin_transaction()?;
        tx.execute("UPDATE users SET age = age + 1 WHERE name = 'Alice'")?;
        tx.execute("DELETE FROM users WHERE age < 25")?;
        tx.commit()?; // Commit transaction
    }
    
    Ok(())
}
````

### 6. Connection Pool Support (Optional)

````rust
// src/pool.rs
use std::sync::{Arc, Mutex};
use std::collections::VecDeque;

/// Database connection pool
pub struct ConnectionPool {
    connections: Arc<Mutex<VecDeque<Database>>>,
    max_size: usize,
    db_path: std::path::PathBuf,
}

impl ConnectionPool {
    pub fn new<P: AsRef<std::path::Path>>(path: P, max_size: usize) -> Result<Self> {
        let db_path = path.as_ref().to_path_buf();
        let connections = Arc::new(Mutex::new(VecDeque::new()));
        Ok(Self { connections, max_size, db_path })
    }
    
    pub fn get_connection(&self) -> Result<PooledConnection> {
        let mut conns = self.connections.lock().unwrap();
        
        let db = if let Some(db) = conns.pop_front() {
            db
        } else {
            Database::open(&self.db_path)?
        };
        
        Ok(PooledConnection {
            database: Some(db),
            pool: self.connections.clone(),
        })
    }
}

pub struct PooledConnection {
    database: Option<Database>,
    pool: Arc<Mutex<VecDeque<Database>>>,
}

impl Drop for PooledConnection {
    fn drop(&mut self) {
        if let Some(db) = self.database.take() {
            let mut conns = self.pool.lock().unwrap();
            conns.push_back(db);
        }
    }
}

impl std::ops::Deref for PooledConnection {
    type Target = Database;
    
    fn deref(&self) -> &Self::Target {
        self.database.as_ref().unwrap()
    }
}

impl std::ops::DerefMut for PooledConnection {
    fn deref_mut(&mut self) -> &mut Self::Target {
        self.database.as_mut().unwrap()
    }
}
````

## Advantages

1. **SQLite-style API** - Users can use TegDB just like SQLite
2. **Automatic transaction management** - Regular operations handle transactions automatically, advanced users can control manually
3. **Type safety** - Provides compile-time safety through Rust's type system
4. **Backward compatibility** - Preserves low-level API for advanced users
5. **Extensible** - Supports advanced features like connection pooling

This architecture allows TegDB to maintain low-level flexibility while providing high-level ease of use, enabling users to use TegDB as simply as they would SQLite.