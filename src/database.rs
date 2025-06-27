//! High-level database interface
//!
//! This module provides a SQLite-like interface for TegDB, making it easy for users
//! to interact with the database without dealing with low-level engine details.

use crate::{engine::Engine, executor::TableSchema, parser::{parse_sql, SqlValue}, Result};
use crate::{planner::QueryPlanner, enhanced_plan_executor::EnhancedPlanExecutor};
use crate::storage_format::StorageFormat;
use std::{path::Path, collections::HashMap, sync::{Arc, RwLock}};

/// Configuration for database creation
#[derive(Clone, Debug)]
pub struct DatabaseConfig {
    /// Storage format to use for row data (always native binary format)
    pub storage_format: StorageFormat,
    /// Enable query planner optimizations
    pub enable_planner: bool,
    /// Enable statistics collection for better query planning
    pub enable_statistics: bool,
}

impl Default for DatabaseConfig {
    fn default() -> Self {
        Self {
            storage_format: StorageFormat::native(), // Always use native format
            enable_planner: true,
            enable_statistics: true,
        }
    }
}

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
    /// Database configuration including storage format
    config: DatabaseConfig,
}

impl Database {
    /// Create or open database with default configuration
    pub fn open<P: AsRef<Path>>(path: P) -> Result<Self> {
        Self::open_with_config(path, DatabaseConfig::default())
    }
    
    /// Create or open database with custom configuration
    pub fn open_with_config<P: AsRef<Path>>(path: P, config: DatabaseConfig) -> Result<Self> {
        let mut engine = Engine::new(path.as_ref().to_path_buf())?;
        
        // Load all table schemas at database initialization
        let mut table_schemas = HashMap::new();
        
        // Use a temporary transaction to load schemas
        {
            let temp_transaction = engine.begin_transaction();
            Self::load_schemas_from_transaction(&temp_transaction, &mut table_schemas)?;
        }
        
        Ok(Self { 
            engine, 
            table_schemas: Arc::new(RwLock::new(table_schemas)),
            config,
        })
    }
    
    /// Load schemas from a transaction into the provided HashMap
    fn load_schemas_from_transaction(
        transaction: &crate::engine::Transaction,
        schemas: &mut HashMap<String, TableSchema>
    ) -> Result<()> {
        // Scan for all schema keys
        let schema_prefix = "__schema__:".as_bytes().to_vec();
        let schema_end = "__schema__~".as_bytes().to_vec(); // '~' comes after ':'
        
        let schema_entries = transaction.scan(schema_prefix..schema_end)?;
        
        for (key, value) in schema_entries {
            // Extract table name from key
            let key_str = String::from_utf8_lossy(&key);
            if let Some(table_name) = key_str.strip_prefix("__schema__:") {
                // Deserialize schema
                if let Ok(schema) = Self::deserialize_schema(&value) {
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

        Ok(TableSchema { columns })
    }
    
    /// Execute SQL statement, return number of affected rows
    pub fn execute(&mut self, sql: &str) -> Result<usize> {
        let (_, statement) = parse_sql(sql)
            .map_err(|e| crate::Error::Other(format!("SQL parse error: {:?}", e)))?;
        
        // Clone schemas for the executor
        let schemas = self.table_schemas.read().unwrap().clone();
        
        // Use a single transaction for this operation
        let transaction = self.engine.begin_transaction();
        
        // Use the new planner pipeline with enhanced executor
        let planner = QueryPlanner::new(schemas.clone());
        let mut plan_executor = EnhancedPlanExecutor::new(transaction, schemas.clone(), self.config.storage_format.clone());
        
        // Start an implicit transaction
        plan_executor.executor_mut().begin_transaction()?;
        
        // Generate and execute the plan
        let plan = planner.plan(statement.clone())?;
        let result = plan_executor.execute_plan(plan)?;
        
        plan_executor.executor_mut().commit_transaction()?;
        
        // Update our shared schemas cache for DDL operations
        match &statement {
            crate::parser::Statement::CreateTable(create_table) => {
                let schema = crate::executor::TableSchema {
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
        plan_executor.executor_mut().transaction_mut().commit()?;
        
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
        
        // Clone schemas for the executor
        let schemas = self.table_schemas.read().unwrap().clone();
        
        let transaction = self.engine.begin_transaction();
        
        // Use the new planner pipeline with enhanced executor
        let planner = QueryPlanner::new(schemas.clone());
        let mut plan_executor = EnhancedPlanExecutor::new(transaction, schemas, self.config.storage_format.clone());
        
        // Start an implicit transaction
        plan_executor.executor_mut().begin_transaction()?;
        
        // Generate and execute the plan
        let plan = planner.plan(statement)?;
        let result = plan_executor.execute_plan(plan)?;
        
        plan_executor.executor_mut().commit_transaction()?;
        
        // Actually commit the engine transaction
        plan_executor.executor_mut().transaction_mut().commit()?;
        
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
        let schemas = self.table_schemas.read().unwrap().clone();
        Ok(Transaction::new_with_schemas(tx, schemas, self.config.storage_format.clone()))
    }
    
    /// Refresh schema cache from database storage
    /// This can be useful if the database was modified externally
    pub fn refresh_schema_cache(&mut self) -> Result<()> {
        let mut schemas = HashMap::new();
        
        // Use a temporary transaction to reload schemas
        {
            let temp_transaction = self.engine.begin_transaction();
            Self::load_schemas_from_transaction(&temp_transaction, &mut schemas)?;
        }
        
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

impl Row<'_> {
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
    plan_executor: EnhancedPlanExecutor<'a>,
    planner: QueryPlanner,
}

impl<'a> Transaction<'a> {
    fn new_with_schemas(transaction: crate::engine::Transaction<'a>, schemas: HashMap<String, TableSchema>, storage_format: StorageFormat) -> Self {
        let planner = QueryPlanner::new(schemas.clone());
        let mut plan_executor = EnhancedPlanExecutor::new(transaction, schemas, storage_format);
        // Start the transaction immediately
        let _ = plan_executor.executor_mut().begin_transaction();
        Self { plan_executor, planner }
    }
    
    /// Execute SQL in transaction
    pub fn execute(&mut self, sql: &str) -> Result<usize> {
        let (_, statement) = parse_sql(sql)
            .map_err(|e| crate::Error::Other(format!("SQL parse error: {:?}", e)))?;
        
        // Use the planner pipeline for transaction operations too
        let plan = self.planner.plan(statement)?;
        let result = self.plan_executor.execute_plan(plan)?;
        
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
        
        // Use the planner pipeline for transaction queries too
        let plan = self.planner.plan(statement)?;
        let result = self.plan_executor.execute_plan(plan)?;
        
        match result {
            crate::executor::ResultSet::Select { columns, rows } => {
                Ok(QueryResult { columns, rows })
            }
            _ => Err(crate::Error::Other("Expected SELECT result".to_string())),
        }
    }
    
    /// Commit transaction
    pub fn commit(mut self) -> Result<()> {
        self.plan_executor.executor_mut().commit_transaction()?;
        // Actually commit the underlying engine transaction
        self.plan_executor.executor_mut().transaction_mut().commit()?;
        Ok(())
    }
    
    /// Rollback transaction
    pub fn rollback(mut self) -> Result<()> {
        self.plan_executor.executor_mut().rollback_transaction()?;
        // Actually rollback the underlying engine transaction
        let _ = self.plan_executor.executor_mut().transaction_mut().rollback();
        Ok(())
    }
    
    /// Get mutable reference to the underlying transaction for low-level access
    pub fn transaction_mut(&mut self) -> &mut crate::engine::Transaction<'a> {
        self.plan_executor.executor_mut().transaction_mut()
    }
}
