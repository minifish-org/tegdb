//! High-level database interface
//!
//! This module provides a SQLite-like interface for TegDB, making it easy for users
//! to interact with the database without dealing with low-level engine details.

use crate::{Engine, executor::Executor, parser::{parse_sql, SqlValue}, Result};
use std::path::Path;

/// Database connection, similar to sqlite::Connection
pub struct Database {
    engine: Engine,
}

impl Database {
    /// Create or open database
    pub fn open<P: AsRef<Path>>(path: P) -> Result<Self> {
        let engine = Engine::new(path.as_ref().to_path_buf())?;
        Ok(Self { engine })
    }
    
    /// Execute SQL statement, return number of affected rows
    pub fn execute(&mut self, sql: &str) -> Result<usize> {
        let (_, statement) = parse_sql(sql)
            .map_err(|e| crate::Error::Other(format!("SQL parse error: {:?}", e)))?;
        
        // Use a single transaction for this operation
        let transaction = self.engine.begin_transaction();
        let mut executor = Executor::new(transaction);
        
        // Start an implicit transaction
        executor.execute(crate::parser::Statement::Begin)?;
        let result = executor.execute(statement)?;
        executor.execute(crate::parser::Statement::Commit)?;
        
        // Actually commit the engine transaction
        executor.transaction_mut().commit()?;
        
        match result {
            crate::executor::ResultSet::Insert { rows_affected } => Ok(rows_affected),
            crate::executor::ResultSet::Update { rows_affected } => Ok(rows_affected),
            crate::executor::ResultSet::Delete { rows_affected } => Ok(rows_affected),
            crate::executor::ResultSet::CreateTable { .. } => Ok(0),
            _ => Ok(0),
        }
    }
    
    /// Execute query, return result set
    pub fn query(&mut self, sql: &str) -> Result<QueryResult> {
        let (_, statement) = parse_sql(sql)
            .map_err(|e| crate::Error::Other(format!("SQL parse error: {:?}", e)))?;
        
        let transaction = self.engine.begin_transaction();
        let mut executor = Executor::new(transaction);
        
        // Start an implicit transaction
        executor.execute(crate::parser::Statement::Begin)?;
        let result = executor.execute(statement)?;
        executor.execute(crate::parser::Statement::Commit)?;
        
        // Actually commit the engine transaction
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

/// Database transaction
pub struct Transaction<'a> {
    executor: Executor<'a>,
}

impl<'a> Transaction<'a> {
    fn new(transaction: crate::Transaction<'a>) -> Self {
        let mut executor = Executor::new(transaction);
        // Start the transaction immediately
        let _ = executor.execute(crate::parser::Statement::Begin);
        Self { executor }
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
            crate::executor::ResultSet::CreateTable { .. } => Ok(0),
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
    pub fn commit(mut self) -> Result<()> {
        self.executor.execute(crate::parser::Statement::Commit)?;
        // Actually commit the underlying engine transaction
        self.executor.transaction_mut().commit()?;
        Ok(())
    }
    
    /// Rollback transaction
    pub fn rollback(mut self) -> Result<()> {
        self.executor.execute(crate::parser::Statement::Rollback)?;
        // Actually rollback the underlying engine transaction
        self.executor.transaction_mut().rollback();
        Ok(())
    }
    
    /// Get mutable reference to the underlying transaction for low-level access
    pub fn transaction_mut(&mut self) -> &mut crate::Transaction<'a> {
        self.executor.transaction_mut()
    }
}
