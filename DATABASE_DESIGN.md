## 建议的架构组织

### 1. 创建统一的数据库连接接口

````rust
// src/database.rs
use crate::{Engine, executor::Executor, parser::parse_sql, Result};

/// 数据库连接，类似于 sqlite::Connection
pub struct Database {
    engine: Engine,
}

impl Database {
    /// 创建或打开数据库
    pub fn open<P: AsRef<std::path::Path>>(path: P) -> Result<Self> {
        let engine = Engine::new(path.as_ref().to_path_buf())?;
        Ok(Self { engine })
    }
    
    /// 执行 SQL 语句，返回受影响的行数
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
    
    /// 执行查询，返回结果集
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
    
    /// 开始事务
    pub fn begin_transaction(&mut self) -> Result<Transaction> {
        let tx = self.engine.begin_transaction();
        Ok(Transaction::new(tx))
    }
}
````

### 2. 创建查询结果类型

````rust
// src/database.rs 续
use crate::parser::SqlValue;

/// 查询结果，类似于 sqlite 的结果集
#[derive(Debug)]
pub struct QueryResult {
    pub columns: Vec<String>,
    pub rows: Vec<Vec<SqlValue>>,
}

impl QueryResult {
    /// 获取列名
    pub fn columns(&self) -> &[String] {
        &self.columns
    }
    
    /// 获取所有行
    pub fn rows(&self) -> &[Vec<SqlValue>] {
        &self.rows
    }
    
    /// 迭代行
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

/// 单行数据
pub struct Row<'a> {
    columns: &'a [String],
    values: &'a [SqlValue],
    index: usize,
}

impl<'a> Row<'a> {
    /// 通过列名获取值
    pub fn get(&self, column: &str) -> Option<&SqlValue> {
        self.columns.iter()
            .position(|c| c == column)
            .and_then(|i| self.values.get(i))
    }
    
    /// 通过索引获取值
    pub fn get_by_index(&self, index: usize) -> Option<&SqlValue> {
        self.values.get(index)
    }
    
    /// 获取行索引
    pub fn index(&self) -> usize {
        self.index
    }
}
````

### 3. 创建事务接口

````rust
// src/database.rs 续
/// 数据库事务
pub struct Transaction<'a> {
    transaction: crate::Transaction<'a>,
    executor: Executor<'a>,
}

impl<'a> Transaction<'a> {
    fn new(transaction: crate::Transaction<'a>) -> Self {
        let executor = Executor::new(transaction);
        Self { transaction, executor }
    }
    
    /// 在事务中执行 SQL
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
    
    /// 在事务中执行查询
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
    
    /// 提交事务
    pub fn commit(self) -> Result<()> {
        self.transaction.commit()
    }
    
    /// 回滚事务
    pub fn rollback(self) -> Result<()> {
        self.transaction.rollback()
    }
}
````

### 4. 修改 lib.rs 导出

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

// 保留低级 API 用于高级用户
pub mod low_level {
    pub use crate::engine::{Engine, Transaction as EngineTransaction};
    pub use crate::executor::{Executor, ResultSet};
    pub use crate::parser::{parse_sql, Statement, SqlValue};
}
````

### 5. 使用示例

````rust
// examples/sqlite_like_usage.rs
use tegdb::{Database, Result};

fn main() -> Result<()> {
    // 创建/打开数据库，类似 SQLite
    let mut db = Database::open("my_database.db")?;
    
    // 创建表
    db.execute("CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT NOT NULL, age INTEGER)")?;
    
    // 插入数据
    let affected = db.execute("INSERT INTO users (id, name, age) VALUES (1, 'Alice', 30)")?;
    println!("Inserted {} rows", affected);
    
    // 批量插入
    db.execute("INSERT INTO users (id, name, age) VALUES (2, 'Bob', 25), (3, 'Carol', 35)")?;
    
    // 查询数据
    let result = db.query("SELECT * FROM users WHERE age > 25")?;
    
    println!("Columns: {:?}", result.columns());
    for row in result.iter() {
        println!("User: {} (ID: {:?}, Age: {:?})", 
            row.get("name").unwrap(),
            row.get("id").unwrap(),
            row.get("age").unwrap()
        );
    }
    
    // 使用事务
    {
        let mut tx = db.begin_transaction()?;
        tx.execute("UPDATE users SET age = age + 1 WHERE name = 'Alice'")?;
        tx.execute("DELETE FROM users WHERE age < 25")?;
        tx.commit()?; // 提交事务
    }
    
    Ok(())
}
````

### 6. 连接池支持（可选）

````rust
// src/pool.rs
use std::sync::{Arc, Mutex};
use std::collections::VecDeque;

/// 数据库连接池
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

## 优势

1. **SQLite 风格的 API** - 用户可以像使用 SQLite 一样使用 TegDB
2. **自动事务管理** - 普通操作自动处理事务，高级用户可以手动控制
3. **类型安全** - 通过 Rust 类型系统提供编译时安全性
4. **向后兼容** - 保留低级 API 供高级用户使用
5. **可扩展** - 支持连接池等高级功能

这样的架构让 TegDB 既保持了底层的灵活性，又提供了高层的易用性，让用户可以像使用 SQLite 一样简单地使用 TegDB。