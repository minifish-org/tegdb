//! High-level database interface
//!
//! This module provides a SQLite-like interface for TegDB, making it easy for users
//! to interact with the database without dealing with low-level engine details.

use crate::{engine::Engine, executor::{TableSchema, Executor}, parser::{parse_sql, SqlValue}, Result};
use crate::planner::QueryPlanner;
use std::{path::Path, collections::HashMap, sync::{Arc, RwLock}};

/// Database connection, similar to sqlite::Connection
/// 
/// This struct maintains a schema cache at the database level to avoid
/// repeated schema loading from disk for every executor creation.
/// Schemas are loaded once when the database is opened and kept in sync
/// with DDL operations (CREATE TABLE, DROP TABLE).
pub struct Database {
    engine: Engine,
    /// Shared table schemas cache, loaded once and shared across executors
    /// Uses Arc<RwLock<>> for thread-safe access with multiple readers
    table_schemas: Arc<RwLock<HashMap<String, TableSchema>>>,
}

impl Database {
    /// Create or open database
    pub fn open<P: AsRef<Path>>(path: P) -> Result<Self> {
        let engine = Engine::new(path.as_ref().to_path_buf())?;
        
        // Load all table schemas at database initialization
        let mut table_schemas = HashMap::new();
        
        // Load schemas directly from engine (no transaction needed for reads)
        Self::load_schemas_from_engine(&engine, &mut table_schemas)?;
        
        Ok(Self { 
            engine, 
            table_schemas: Arc::new(RwLock::new(table_schemas)),
        })
    }
    
    /// Load schemas from engine into the provided HashMap
    fn load_schemas_from_engine(
        engine: &Engine,
        schemas: &mut HashMap<String, TableSchema>
    ) -> Result<()> {
        // Scan for all schema keys
        let schema_prefix = "__schema__:".as_bytes().to_vec();
        let schema_end = "__schema__~".as_bytes().to_vec(); // '~' comes after ':'
        
        let schema_entries = engine.scan(schema_prefix..schema_end)?;
        
        for (key, value) in schema_entries {
            // Extract table name from key
            let key_str = String::from_utf8_lossy(&key);
            if let Some(table_name) = key_str.strip_prefix("__schema__:") {
                // Deserialize schema
                if let Ok(mut schema) = Self::deserialize_schema(&value) {
                    schema.name = table_name.to_string(); // Set the actual table name
                    schemas.insert(table_name.to_string(), schema);
                }
            }
        }
        
        Ok(())
    }
    
    /// Deserialize table schema from bytes (copied from Executor)
    fn deserialize_schema(data: &[u8]) -> Result<TableSchema> {
        let data_str = String::from_utf8_lossy(data);
        let mut columns = Vec::new();

        for column_part in data_str.split('|') {
            if column_part.is_empty() {
                continue;
            }

            let components: Vec<&str> = column_part.splitn(3, ':').collect();
            if components.len() >= 2 {
                let column_name = components[0].to_string();
                let data_type_str = components[1];
                let constraints_str = if components.len() > 2 { components[2] } else { "" };

                let data_type = match data_type_str {
                    "Integer" => crate::parser::DataType::Integer,
                    "Text" => crate::parser::DataType::Text,
                    "Real" => crate::parser::DataType::Real,
                    "Blob" => crate::parser::DataType::Blob,
                    // Also accept uppercase for backward compatibility
                    "INTEGER" => crate::parser::DataType::Integer,
                    "TEXT" => crate::parser::DataType::Text,
                    "REAL" => crate::parser::DataType::Real,
                    "BLOB" => crate::parser::DataType::Blob,
                    _ => crate::parser::DataType::Text, // Default fallback
                };

                let constraints = if constraints_str.is_empty() {
                    Vec::new()
                } else {
                    constraints_str
                        .split(',')
                        .filter_map(|c| match c {
                            "PRIMARY_KEY" => Some(crate::parser::ColumnConstraint::PrimaryKey),
                            "NOT_NULL" => Some(crate::parser::ColumnConstraint::NotNull),
                            "UNIQUE" => Some(crate::parser::ColumnConstraint::Unique),
                            _ => None,
                        })
                        .collect()
                };

                columns.push(crate::executor::ColumnInfo {
                    name: column_name,
                    data_type,
                    constraints,
                });
            }
        }

        Ok(TableSchema { 
            name: "unknown".to_string(), // Will be set by caller
            columns 
        })
    }
    
    /// Execute SQL statement, return number of affected rows
    pub fn execute(&mut self, sql: &str) -> Result<usize> {
        let (_, statement) = parse_sql(sql)
            .map_err(|e| crate::Error::Other(format!("SQL parse error: {:?}", e)))?;
        
        // Clone schemas for the executor
        let schemas = self.table_schemas.read().unwrap().clone();
        
        // Use a single transaction for this operation
        let transaction = self.engine.begin_transaction();
        
        // Use the new planner pipeline with executor
        let planner = QueryPlanner::new(schemas.clone());
        let mut executor = Executor::new_with_schemas(transaction, schemas.clone());
        
        // Generate and execute the plan (no need to begin transaction as it's already started)
        let plan = planner.plan(statement.clone())?;
        let result = executor.execute_plan(plan)?;
        
        // Update our shared schemas cache for DDL operations
        match &statement {
            crate::parser::Statement::CreateTable(create_table) => {
                let schema = crate::executor::TableSchema {
                    name: create_table.table.clone(),
                    columns: create_table.columns.iter().map(|col| crate::executor::ColumnInfo {
                        name: col.name.clone(),
                        data_type: col.data_type.clone(),
                        constraints: col.constraints.clone(),
                    }).collect(),
                };
                self.table_schemas.write().unwrap().insert(create_table.table.clone(), schema);
            }
            crate::parser::Statement::DropTable(drop_table) => {
                // Remove table schema from cache when table is dropped
                self.table_schemas.write().unwrap().remove(&drop_table.table);
            }
            _ => {} // No schema changes for other statements
        }
        
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

    /// Execute SQL query, return streaming query results
    /// Returns an iterator that yields rows as they are found, similar to SQLite's approach
    /// This provides memory efficiency and early termination for large datasets
    pub fn query(&mut self, sql: &str) -> Result<QueryIterator> {
        let (_, statement) = parse_sql(sql)
            .map_err(|e| crate::Error::Other(format!("SQL parse error: {:?}", e)))?;
        
        // Clone schemas for the executor
        let schemas = self.table_schemas.read().unwrap().clone();
        
        // Use a single transaction for this operation
        let transaction = self.engine.begin_transaction();
        
        // Use the new planner pipeline with executor
        let planner = QueryPlanner::new(schemas.clone());
        let mut executor = Executor::new_with_schemas(transaction, schemas.clone());
        
        // Generate and execute the plan using streaming API for better performance
        let plan = planner.plan(statement)?;
        let streaming_result = executor.execute_plan_streaming(plan)?;
        
        // No need to commit for read operations
        
        // Return streaming result directly as iterator
        match streaming_result {
            crate::executor::StreamingResult::Select(streaming_set) => {
                // For now, we'll collect the streaming results because we can't easily
                // return the iterator with the current lifetime constraints
                // TODO: Implement true streaming by restructuring the transaction lifecycle
                let columns = streaming_set.columns.clone();
                let rows = streaming_set.collect_rows()?;
                
                Ok(QueryIterator::new(columns, rows))
            }
            crate::executor::StreamingResult::Other(result) => {
                match result {
                    crate::executor::ResultSet::Select { columns, rows } => {
                        Ok(QueryIterator::new(columns, rows))
                    }
                    _ => Err(crate::Error::Other("Expected SELECT result".to_string())),
                }
            }
        }
    }
    
    /// Begin a new database transaction
    pub fn begin_transaction(&mut self) -> Result<DatabaseTransaction<'_>> {
        let schemas = self.table_schemas.read().unwrap().clone();
        let transaction = self.engine.begin_transaction();
        let executor = Executor::new_with_schemas(transaction, schemas);
        
        Ok(DatabaseTransaction {
            executor,
            table_schemas: Arc::clone(&self.table_schemas),
        })
    }
    
    /// Reload table schemas from disk
    /// This can be useful if the database was modified externally
    pub fn refresh_schema_cache(&mut self) -> Result<()> {
        let mut schemas = HashMap::new();
        
        // Reload schemas directly from engine
        Self::load_schemas_from_engine(&self.engine, &mut schemas)?;
        
        // Update the shared cache
        *self.table_schemas.write().unwrap() = schemas;
        
        Ok(())
    }
    
    /// Get a copy of all cached table schemas
    /// Useful for debugging or introspection
    pub fn get_table_schemas(&self) -> HashMap<String, TableSchema> {
        self.table_schemas.read().unwrap().clone()
    }
}

/// Query result containing columns and rows
#[derive(Debug, Clone, PartialEq)]
pub struct QueryResult {
    columns: Vec<String>,
    rows: Vec<Vec<SqlValue>>,
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
    
    /// Get number of rows
    pub fn len(&self) -> usize {
        self.rows.len()
    }
    
    /// Check if result is empty
    pub fn is_empty(&self) -> bool {
        self.rows.is_empty()
    }
}

/// Iterator-based query result that streams rows without loading all into memory
/// Similar to SQLite's row iterator approach
pub struct QueryIterator {
    columns: Vec<String>,
    rows: Vec<Vec<SqlValue>>, // Store materialized rows for backward compatibility
}

impl QueryIterator {
    /// Create a new QueryIterator with materialized rows
    fn new(columns: Vec<String>, rows: Vec<Vec<SqlValue>>) -> Self {
        Self { columns, rows }
    }
    
    /// Get column names
    pub fn columns(&self) -> &[String] {
        &self.columns
    }
    
    /// Get all rows (backward compatibility)
    pub fn rows(&self) -> &[Vec<SqlValue>] {
        &self.rows
    }
    
    /// Get number of rows (backward compatibility)
    pub fn len(&self) -> usize {
        self.rows.len()
    }
    
    /// Check if result is empty (backward compatibility)
    pub fn is_empty(&self) -> bool {
        self.rows.is_empty()
    }
    
    /// Collect all remaining rows into a Vec (for backward compatibility)
    pub fn collect_rows(self) -> Result<Vec<Vec<SqlValue>>> {
        Ok(self.rows)
    }
    
    /// Convert to the old QueryResult format (for backward compatibility)
    pub fn into_query_result(self) -> Result<QueryResult> {
        Ok(QueryResult { 
            columns: self.columns, 
            rows: self.rows 
        })
    }
}

impl Iterator for QueryIterator {
    type Item = Result<Vec<SqlValue>>;
    
    fn next(&mut self) -> Option<Self::Item> {
        if self.rows.is_empty() {
            None
        } else {
            Some(Ok(self.rows.remove(0)))
        }
    }
}

/// Transaction handle for batch operations
pub struct DatabaseTransaction<'a> {
    executor: Executor<'a>,
    table_schemas: Arc<RwLock<HashMap<String, TableSchema>>>,
}

impl<'a> DatabaseTransaction<'a> {
    /// Execute SQL statement within transaction
    pub fn execute(&mut self, sql: &str) -> Result<usize> {
        let (_, statement) = parse_sql(sql)
            .map_err(|e| crate::Error::Other(format!("SQL parse error: {:?}", e)))?;
        
        // Get schemas from shared cache
        let schemas = self.table_schemas.read().unwrap().clone();
        
        // Use the planner pipeline
        let planner = QueryPlanner::new(schemas);
        let plan = planner.plan(statement.clone())?;
        let result = self.executor.execute_plan(plan)?;
        
        // Update schema cache for DDL operations
        match &statement {
            crate::parser::Statement::CreateTable(create_table) => {
                let schema = crate::executor::TableSchema {
                    name: create_table.table.clone(),
                    columns: create_table.columns.iter().map(|col| crate::executor::ColumnInfo {
                        name: col.name.clone(),
                        data_type: col.data_type.clone(),
                        constraints: col.constraints.clone(),
                    }).collect(),
                };
                self.table_schemas.write().unwrap().insert(create_table.table.clone(), schema);
            }
            crate::parser::Statement::DropTable(drop_table) => {
                self.table_schemas.write().unwrap().remove(&drop_table.table);
            }
            _ => {}
        }
        
        match result {
            crate::executor::ResultSet::Insert { rows_affected } => Ok(rows_affected),
            crate::executor::ResultSet::Update { rows_affected } => Ok(rows_affected),
            crate::executor::ResultSet::Delete { rows_affected } => Ok(rows_affected),
            crate::executor::ResultSet::CreateTable { .. } => Ok(0),
            _ => Ok(0),
        }
    }

    /// Execute SQL query within transaction
    /// Returns an iterator that yields rows as they are found
    pub fn query(&mut self, sql: &str) -> Result<QueryIterator> {
        let (_, statement) = parse_sql(sql)
            .map_err(|e| crate::Error::Other(format!("SQL parse error: {:?}", e)))?;
        
        // Get schemas from shared cache
        let schemas = self.table_schemas.read().unwrap().clone();
        
        // Use the planner pipeline with streaming support
        let planner = QueryPlanner::new(schemas);
        let plan = planner.plan(statement)?;
        let streaming_result = self.executor.execute_plan_streaming(plan)?;
        
        match streaming_result {
            crate::executor::StreamingResult::Select(streaming_set) => {
                // For now, we'll collect the streaming results because we can't easily
                // return the iterator with the current lifetime constraints
                let columns = streaming_set.columns.clone();
                let rows = streaming_set.collect_rows()?;
                
                Ok(QueryIterator::new(columns, rows))
            }
            crate::executor::StreamingResult::Other(result) => {
                match result {
                    crate::executor::ResultSet::Select { columns, rows } => {
                        Ok(QueryIterator::new(columns, rows))
                    }
                    _ => Err(crate::Error::Other("Expected SELECT result".to_string())),
                }
            }
        }
    }
    
    /// Commit the transaction
    pub fn commit(mut self) -> Result<()> {
        self.executor.transaction_mut().commit()
    }
    
    /// Rollback the transaction
    pub fn rollback(mut self) -> Result<()> {
        self.executor.transaction_mut().rollback()
    }
}
