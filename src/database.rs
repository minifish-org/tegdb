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

    /// Execute SQL query, return true streaming results that yield rows on-demand
    /// This is the new streaming API that doesn't materialize all rows upfront
    pub fn stream_query(&mut self, sql: &str) -> Result<StreamingQuery> {
        let (_, statement) = parse_sql(sql)
            .map_err(|e| crate::Error::Other(format!("SQL parse error: {:?}", e)))?;
        
        // Parse the statement to determine if we can do true streaming
        match &statement {
            crate::parser::Statement::Select(select) => {
                // Clone schemas for the streaming query
                let schemas = self.table_schemas.read().unwrap().clone();
                
                let table_name = &select.table;
                let selected_columns = if select.columns.is_empty() || select.columns[0] == "*" {
                    // Get all columns from schema
                    if let Some(schema) = schemas.get(table_name) {
                        schema.columns.iter().map(|col| col.name.clone()).collect()
                    } else {
                        return Err(crate::Error::Other(format!("Table '{}' not found", table_name)));
                    }
                } else {
                    select.columns.clone()
                };
                
                // Get table schema
                let schema = schemas.get(table_name)
                    .ok_or_else(|| crate::Error::Other(format!("Table '{}' not found", table_name)))?
                    .clone();
                
                // Extract condition from where clause
                let condition = select.where_clause.as_ref().map(|wc| wc.condition.clone());
                
                // Create streaming query that borrows from this database
                // Since TegDB is single-threaded, we can safely create a shared reference
                let streaming_query = StreamingQuery::new_borrowing(
                    &self.engine,
                    table_name.clone(),
                    selected_columns,
                    condition,
                    select.limit,
                    schema,
                )?;
                
                Ok(streaming_query)
            }
            _ => {
                // For non-SELECT statements, this doesn't make sense
                Err(crate::Error::Other("stream_query() should only be used for SELECT statements".to_string()))
            }
        }
    }
    
    /// Execute SQL query, return backward-compatible materialized results
    /// This method still collects all results but uses the iterator interface
//    pub fn query(&mut self, sql: &str) -> Result<QueryIterator> {
//        let (_, statement) = parse_sql(sql)
//            .map_err(|e| crate::Error::Other(format!("SQL parse error: {:?}", e)))?;
//        
//        // Clone schemas for the executor
//        let schemas = self.table_schemas.read().unwrap().clone();
//        
//        // Use a single transaction for this operation
//        let transaction = self.engine.begin_transaction();
//        
//        // Use the new planner pipeline with executor
//        let planner = QueryPlanner::new(schemas.clone());
//        let mut executor = Executor::new_with_schemas(transaction, schemas.clone());
//        
//        // Generate and execute the plan
//        let plan = planner.plan(statement)?;
//        let result = executor.execute_plan(plan)?;
//        
//        // Return iterator result
//        match result {
//            crate::executor::ResultSet::Select { columns, rows } => {
//                Ok(QueryIterator::new_materialized(columns, rows))
//            }
//            _ => Err(crate::Error::Other("Expected SELECT result".to_string())),
//        }
//    }
    
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

/// True streaming query that borrows from the database engine
/// This allows for real streaming without materializing all rows upfront
/// Since TegDB is single-threaded, we can safely borrow from the engine
pub struct StreamingQuery<'a> {
    columns: Vec<String>,
    engine: &'a crate::engine::Engine,
    table_name: String,
    selected_columns: Vec<String>,
    filter: Option<crate::parser::Condition>,
    limit: Option<u64>,
    schema: crate::executor::TableSchema,
    storage_format: crate::storage_format::StorageFormat,
    last_key: Option<Vec<u8>>, // Track last seen key for continuation
    count: u64,
    finished: bool,
}

impl<'a> StreamingQuery<'a> {
    /// Create a new streaming query that borrows from the engine
    fn new_borrowing(
        engine: &'a crate::engine::Engine,
        table_name: String,
        selected_columns: Vec<String>,
        filter: Option<crate::parser::Condition>,
        limit: Option<u64>,
        schema: crate::executor::TableSchema,
    ) -> Result<Self> {
        Ok(Self {
            columns: selected_columns.clone(),
            engine,
            table_name,
            selected_columns,
            filter,
            limit,
            schema,
            storage_format: crate::storage_format::StorageFormat::new(),
            last_key: None,
            count: 0,
            finished: false,
        })
    }
    
    /// Get column names
    pub fn columns(&self) -> &[String] {
        &self.columns
    }
    
    /// Collect all remaining rows into a Vec (for compatibility)
    pub fn collect_rows(mut self) -> Result<Vec<Vec<SqlValue>>> {
        let mut rows = Vec::new();
        while let Some(row_result) = self.next() {
            rows.push(row_result?);
        }
        Ok(rows)
    }
    
    /// Convert to the old QueryResult format (for backward compatibility)
    pub fn into_query_result(self) -> Result<QueryResult> {
        let columns = self.columns.clone();
        let rows = self.collect_rows()?;
        Ok(QueryResult { columns, rows })
    }
    
    /// Apply filter condition to row data
    fn evaluate_condition(&self, condition: &crate::parser::Condition, row_data: &std::collections::HashMap<String, SqlValue>) -> bool {
        use crate::parser::Condition;
        
        match condition {
            Condition::Comparison { left, operator, right } => {
                let row_value = row_data.get(left).unwrap_or(&SqlValue::Null);
                self.compare_values(row_value, operator, right)
            }
            Condition::And(left, right) => {
                self.evaluate_condition(left, row_data) && self.evaluate_condition(right, row_data)
            }
            Condition::Or(left, right) => {
                self.evaluate_condition(left, row_data) || self.evaluate_condition(right, row_data)
            }
        }
    }
    
    /// Compare two SqlValues using the given operator
    fn compare_values(&self, left: &SqlValue, operator: &crate::parser::ComparisonOperator, right: &SqlValue) -> bool {
        use crate::parser::ComparisonOperator::*;
        
        match operator {
            Equal => left == right,
            NotEqual => left != right,
            LessThan => match (left, right) {
                (SqlValue::Integer(a), SqlValue::Integer(b)) => a < b,
                (SqlValue::Real(a), SqlValue::Real(b)) => a < b,
                (SqlValue::Integer(a), SqlValue::Real(b)) => (*a as f64) < *b,
                (SqlValue::Real(a), SqlValue::Integer(b)) => *a < (*b as f64),
                (SqlValue::Text(a), SqlValue::Text(b)) => a < b,
                _ => false,
            },
            LessThanOrEqual => match (left, right) {
                (SqlValue::Integer(a), SqlValue::Integer(b)) => a <= b,
                (SqlValue::Real(a), SqlValue::Real(b)) => a <= b,
                (SqlValue::Integer(a), SqlValue::Real(b)) => (*a as f64) <= *b,
                (SqlValue::Real(a), SqlValue::Integer(b)) => *a <= (*b as f64),
                (SqlValue::Text(a), SqlValue::Text(b)) => a <= b,   
                _ => false,
            },
            GreaterThan => match (left, right) {
                (SqlValue::Integer(a), SqlValue::Integer(b)) => a > b,
                (SqlValue::Real(a), SqlValue::Real(b)) => a > b,
                (SqlValue::Integer(a), SqlValue::Real(b)) => (*a as f64) > *b,
                (SqlValue::Real(a), SqlValue::Integer(b)) => *a > (*b as f64),
                (SqlValue::Text(a), SqlValue::Text(b)) => a > b,
                _ => false,
            },
            GreaterThanOrEqual => match (left, right) {
                (SqlValue::Integer(a), SqlValue::Integer(b)) => a >= b,
                (SqlValue::Real(a), SqlValue::Real(b)) => a >= b,
                (SqlValue::Integer(a), SqlValue::Real(b)) => (*a as f64) >= *b,
                (SqlValue::Real(a), SqlValue::Integer(b)) => *a >= (*b as f64),
                (SqlValue::Text(a), SqlValue::Text(b)) => a >= b,
                _ => false,
            },
            Like => {
                // Simple LIKE implementation - just checking if right is substring of left
                match (left, right) {
                    (SqlValue::Text(a), SqlValue::Text(b)) => {
                        // Simple pattern matching - convert SQL LIKE to contains for now
                        let pattern = b.replace('%', "");
                        a.contains(&pattern)
                    }
                    _ => false,
                }
            }
        }
    }
}

impl<'a> Iterator for StreamingQuery<'a> {
    type Item = Result<Vec<SqlValue>>;
    
    fn next(&mut self) -> Option<Self::Item> {
        if self.finished {
            return None;
        }
        
        // Check limit
        if let Some(limit) = self.limit {
            if self.count >= limit {
                self.finished = true;
                return None;
            }
        }
        
        // Determine scan range
        let start_key = match &self.last_key {
            Some(last) => {
                // Continue from after the last key
                let mut next_key = last.clone();
                next_key.push(0); // Increment key for next scan
                next_key
            }
            None => {
                // First scan - start from table prefix
                let table_prefix = format!("{}:", self.table_name);
                table_prefix.as_bytes().to_vec()
            }
        };
        
        let end_key = format!("{}~", self.table_name).as_bytes().to_vec();
        
        // Perform scan from current position
        let scan_result = match self.engine.scan(start_key..end_key) {
            Ok(scan) => scan,
            Err(e) => return Some(Err(e)),
        };
        
        // Process rows until we find one that matches the filter
        for (key, value) in scan_result {
            match self.storage_format.deserialize_row(&value, &self.schema) {
                Ok(row_data) => {
                    // Apply filter if present
                    let matches = if let Some(ref filter) = self.filter {
                        self.evaluate_condition(filter, &row_data)
                    } else {
                        true
                    };
                    
                    if matches {
                        // Extract selected columns
                        let mut row_values = Vec::new();
                        for col_name in &self.selected_columns {
                            row_values.push(
                                row_data.get(col_name).cloned().unwrap_or(SqlValue::Null)
                            );
                        }
                        
                        self.count += 1;
                        self.last_key = Some(key);
                        return Some(Ok(row_values));
                    } else {
                        // Update last_key even for filtered out rows to continue scan
                        self.last_key = Some(key);
                    }
                }
                Err(e) => return Some(Err(e)),
            }
        }
        
        // No more rows found
        self.finished = true;
        None
    }
}

/// Iterator-based query result that streams rows without loading all into memory
/// Similar to SQLite's row iterator approach
pub enum QueryIterator<'a> {
    /// True streaming iterator that holds the transaction and executor
    Streaming {
        columns: Vec<String>,
        streaming_result: crate::executor::StreamingResultSet<'a>,
    },
    /// Materialized iterator for backward compatibility
    Materialized {
        columns: Vec<String>,
        rows: Vec<Vec<SqlValue>>,
        index: usize,
    },
}

impl<'a> QueryIterator<'a> {
    /// Create a new streaming QueryIterator
    fn new_streaming(columns: Vec<String>, streaming_result: crate::executor::StreamingResultSet<'a>) -> Self {
        Self::Streaming { columns, streaming_result }
    }
    
    /// Create a new materialized QueryIterator (for backward compatibility)
    fn new_materialized(columns: Vec<String>, rows: Vec<Vec<SqlValue>>) -> Self {
        Self::Materialized { columns, rows, index: 0 }
    }
    
    /// Get column names
    pub fn columns(&self) -> &[String] {
        match self {
            Self::Streaming { columns, .. } => columns,
            Self::Materialized { columns, .. } => columns,
        }
    }
    
    /// Get all rows (backward compatibility - forces materialization for streaming)
    pub fn rows(&self) -> Result<Vec<Vec<SqlValue>>> {
        match self {
            Self::Streaming { .. } => {
                Err(crate::Error::Other("Cannot get all rows from streaming iterator without collecting first".to_string()))
            }
            Self::Materialized { rows, .. } => Ok(rows.clone()),
        }
    }
    
    /// Get number of remaining rows (backward compatibility - only works for materialized)
    pub fn len(&self) -> Result<usize> {
        match self {
            Self::Streaming { .. } => {
                Err(crate::Error::Other("Cannot get length of streaming iterator without collecting first".to_string()))
            }
            Self::Materialized { rows, index, .. } => Ok(rows.len() - *index),
        }
    }
    
    /// Check if result is empty (only works for materialized)
    pub fn is_empty(&self) -> Result<bool> {
        match self {
            Self::Streaming { .. } => {
                Err(crate::Error::Other("Cannot check if streaming iterator is empty without collecting first".to_string()))
            }
            Self::Materialized { rows, index, .. } => Ok(*index >= rows.len()),
        }
    }
    
    /// Collect all remaining rows into a Vec
    pub fn collect_rows(self) -> Result<Vec<Vec<SqlValue>>> {
        match self {
            Self::Streaming { streaming_result, .. } => {
                streaming_result.collect_rows()
            }
            Self::Materialized { rows, index, .. } => {
                Ok(rows.into_iter().skip(index).collect())
            }
        }
    }
    
    /// Convert to the old QueryResult format (for backward compatibility)
    pub fn into_query_result(self) -> Result<QueryResult> {
        let columns = self.columns().to_vec();
        let rows = self.collect_rows()?;
        Ok(QueryResult { columns, rows })
    }
    
    /// Take only the first N rows
    pub fn take(self, n: usize) -> impl Iterator<Item = Result<Vec<SqlValue>>> + 'a {
        match self {
            Self::Streaming { streaming_result, .. } => {
                Box::new(streaming_result.take(n)) as Box<dyn Iterator<Item = Result<Vec<SqlValue>>> + 'a>
            }
            Self::Materialized { rows, index, .. } => {
                Box::new(rows.into_iter().skip(index).take(n).map(Ok)) as Box<dyn Iterator<Item = Result<Vec<SqlValue>>> + 'a>
            }
        }
    }
}

impl<'a> Iterator for QueryIterator<'a> {
    type Item = Result<Vec<SqlValue>>;
    
    fn next(&mut self) -> Option<Self::Item> {
        match self {
            Self::Streaming { streaming_result, .. } => {
                streaming_result.rows.next()
            }
            Self::Materialized { rows, index, .. } => {
                if *index < rows.len() {
                    let row = rows[*index].clone();
                    *index += 1;
                    Some(Ok(row))
                } else {
                    None
                }
            }
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
//    pub fn query(&mut self, sql: &str) -> Result<QueryIterator> {
//        let (_, statement) = parse_sql(sql)
//            .map_err(|e| crate::Error::Other(format!("SQL parse error: {:?}", e)))?;
//        
//        // Get schemas from shared cache
//        let schemas = self.table_schemas.read().unwrap().clone();
//        
//        // Use the planner pipeline with streaming support
//        let planner = QueryPlanner::new(schemas);
//        let plan = planner.plan(statement)?;
//        let streaming_result = self.executor.execute_plan_streaming(plan)?;
//        
//        match streaming_result {
//            crate::executor::StreamingResult::Select(streaming_set) => {
//                // For now, we'll collect the streaming results because we can't easily
//                // return the iterator with the current lifetime constraints
//                // TODO: Implement true streaming by restructuring the transaction lifecycle
//                let columns = streaming_set.columns.clone();
//                let rows = streaming_set.collect_rows()?;
//                
//                Ok(QueryIterator::new_materialized(columns, rows))
//            }
//            crate::executor::StreamingResult::Other(result) => {
//                match result {
//                    crate::executor::ResultSet::Select { columns, rows } => {
//                        Ok(QueryIterator::new_materialized(columns, rows))
//                    }
//                    _ => Err(crate::Error::Other("Expected SELECT result".to_string())),
//                }
//            }
//        }
//    }
    
    /// Execute SQL query within transaction using true streaming
    /// This provides the same streaming capability as Database::stream_query but within a transaction context
    pub fn streaming_query(&mut self, sql: &str) -> Result<TransactionStreamingQuery> {
        let (_, statement) = parse_sql(sql)
            .map_err(|e| crate::Error::Other(format!("SQL parse error: {:?}", e)))?;
        
        // Parse the statement to determine if we can do true streaming
        match &statement {
            crate::parser::Statement::Select(select) => {
                // Get schemas from shared cache
                let schemas = self.table_schemas.read().unwrap().clone();
                
                let table_name = &select.table;
                let selected_columns = if select.columns.is_empty() || select.columns[0] == "*" {
                    // Get all columns from schema
                    if let Some(schema) = schemas.get(table_name) {
                        schema.columns.iter().map(|col| col.name.clone()).collect()
                    } else {
                        return Err(crate::Error::Other(format!("Table '{}' not found", table_name)));
                    }
                } else {
                    select.columns.clone()
                };
                
                // Get table schema
                let schema = schemas.get(table_name)
                    .ok_or_else(|| crate::Error::Other(format!("Table '{}' not found", table_name)))?
                    .clone();
                
                // Extract condition from where clause
                let condition = select.where_clause.as_ref().map(|wc| wc.condition.clone());
                
                // Create streaming query that uses the transaction's engine
                let streaming_query = TransactionStreamingQuery::new(
                    self.executor.transaction(),
                    table_name.clone(),
                    selected_columns,
                    condition,
                    select.limit,
                    schema,
                )?;
                
                Ok(streaming_query)
            }
            _ => {
                // For non-SELECT statements, this doesn't make sense
                Err(crate::Error::Other("streaming_query() should only be used for SELECT statements".to_string()))
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

/// Transaction-based streaming query that operates within a transaction context
/// Similar to StreamingQuery but works with a transaction instead of borrowing the engine directly
pub struct TransactionStreamingQuery<'a> {
    columns: Vec<String>,
    transaction: &'a crate::engine::Transaction<'a>,
    table_name: String,
    selected_columns: Vec<String>,
    filter: Option<crate::parser::Condition>,
    limit: Option<u64>,
    schema: crate::executor::TableSchema,
    storage_format: crate::storage_format::StorageFormat,
    last_key: Option<Vec<u8>>, // Track last seen key for continuation
    count: u64,
    finished: bool,
}

impl<'a> TransactionStreamingQuery<'a> {
    /// Create a new streaming query that operates within a transaction
    fn new(
        transaction: &'a crate::engine::Transaction<'a>,
        table_name: String,
        selected_columns: Vec<String>,
        filter: Option<crate::parser::Condition>,
        limit: Option<u64>,
        schema: crate::executor::TableSchema,
    ) -> Result<Self> {
        Ok(Self {
            columns: selected_columns.clone(),
            transaction,
            table_name,
            selected_columns,
            filter,
            limit,
            schema,
            storage_format: crate::storage_format::StorageFormat::new(),
            last_key: None,
            count: 0,
            finished: false,
        })
    }
    
    /// Get column names
    pub fn columns(&self) -> &[String] {
        &self.columns
    }
    
    /// Collect all remaining rows into a Vec (for compatibility)
    pub fn collect_rows(mut self) -> Result<Vec<Vec<SqlValue>>> {
        let mut rows = Vec::new();
        while let Some(row_result) = self.next() {
            rows.push(row_result?);
        }
        Ok(rows)
    }
    
    /// Convert to the old QueryResult format (for backward compatibility)
    pub fn into_query_result(self) -> Result<QueryResult> {
        let columns = self.columns.clone();
        let rows = self.collect_rows()?;
        Ok(QueryResult { columns, rows })
    }
    
    /// Apply filter condition to row data
    fn evaluate_condition(&self, condition: &crate::parser::Condition, row_data: &std::collections::HashMap<String, SqlValue>) -> bool {
        use crate::parser::Condition;
        
        match condition {
            Condition::Comparison { left, operator, right } => {
                let row_value = row_data.get(left).unwrap_or(&SqlValue::Null);
                self.compare_values(row_value, operator, right)
            }
            Condition::And(left, right) => {
                self.evaluate_condition(left, row_data) && self.evaluate_condition(right, row_data)
            }
            Condition::Or(left, right) => {
                self.evaluate_condition(left, row_data) || self.evaluate_condition(right, row_data)
            }
        }
    }
    
    /// Compare two SqlValues using the given operator
    fn compare_values(&self, left: &SqlValue, operator: &crate::parser::ComparisonOperator, right: &SqlValue) -> bool {
        use crate::parser::ComparisonOperator::*;
        
        match operator {
            Equal => left == right,
            NotEqual => left != right,
            LessThan => match (left, right) {
                (SqlValue::Integer(a), SqlValue::Integer(b)) => a < b,
                (SqlValue::Real(a), SqlValue::Real(b)) => a < b,
                (SqlValue::Integer(a), SqlValue::Real(b)) => (*a as f64) < *b,
                (SqlValue::Real(a), SqlValue::Integer(b)) => *a < (*b as f64),
                (SqlValue::Text(a), SqlValue::Text(b)) => a < b,
                _ => false,
            },
            LessThanOrEqual => match (left, right) {
                (SqlValue::Integer(a), SqlValue::Integer(b)) => a <= b,
                (SqlValue::Real(a), SqlValue::Real(b)) => a <= b,
                (SqlValue::Integer(a), SqlValue::Real(b)) => (*a as f64) <= *b,
                (SqlValue::Real(a), SqlValue::Integer(b)) => *a <= (*b as f64),
                (SqlValue::Text(a), SqlValue::Text(b)) => a <= b,   
                _ => false,
            },
            GreaterThan => match (left, right) {
                (SqlValue::Integer(a), SqlValue::Integer(b)) => a > b,
                (SqlValue::Real(a), SqlValue::Real(b)) => a > b,
                (SqlValue::Integer(a), SqlValue::Real(b)) => (*a as f64) > *b,
                (SqlValue::Real(a), SqlValue::Integer(b)) => *a > (*b as f64),
                (SqlValue::Text(a), SqlValue::Text(b)) => a > b,
                _ => false,
            },
            GreaterThanOrEqual => match (left, right) {
                (SqlValue::Integer(a), SqlValue::Integer(b)) => a >= b,
                (SqlValue::Real(a), SqlValue::Real(b)) => a >= b,
                (SqlValue::Integer(a), SqlValue::Real(b)) => (*a as f64) >= *b,
                (SqlValue::Real(a), SqlValue::Integer(b)) => *a >= (*b as f64),
                (SqlValue::Text(a), SqlValue::Text(b)) => a >= b,
                _ => false,
            },
            Like => {
                // Simple LIKE implementation - just checking if right is substring of left
                match (left, right) {
                    (SqlValue::Text(a), SqlValue::Text(b)) => {
                        // Simple pattern matching - convert SQL LIKE to contains for now
                        let pattern = b.replace('%', "");
                        a.contains(&pattern)
                    }
                    _ => false,
                }
            }
        }
    }
}

impl<'a> Iterator for TransactionStreamingQuery<'a> {
    type Item = Result<Vec<SqlValue>>;
    
    fn next(&mut self) -> Option<Self::Item> {
        if self.finished {
            return None;
        }
        
        // Check limit
        if let Some(limit) = self.limit {
            if self.count >= limit {
                self.finished = true;
                return None;
            }
        }
        
        // Determine scan range
        let start_key = match &self.last_key {
            Some(last) => {
                // Continue from after the last key
                let mut next_key = last.clone();
                next_key.push(0); // Increment key for next scan
                next_key
            }
            None => {
                // First scan - start from table prefix
                let table_prefix = format!("{}:", self.table_name);
                table_prefix.as_bytes().to_vec()
            }
        };
        
        let end_key = format!("{}~", self.table_name).as_bytes().to_vec();
        
        // Perform scan from current position using the transaction
        let scan_result = match self.transaction.scan(start_key..end_key) {
            Ok(scan) => scan,
            Err(e) => return Some(Err(e)),
        };
        
        // Process rows until we find one that matches the filter
        for (key, value) in scan_result {
            match self.storage_format.deserialize_row(&value, &self.schema) {
                Ok(row_data) => {
                    // Apply filter if present
                    let matches = if let Some(ref filter) = self.filter {
                        self.evaluate_condition(filter, &row_data)
                    } else {
                        true
                    };
                    
                    if matches {
                        // Extract selected columns
                        let mut row_values = Vec::new();
                        for col_name in &self.selected_columns {
                            row_values.push(
                                row_data.get(col_name).cloned().unwrap_or(SqlValue::Null)
                            );
                        }
                        
                        self.count += 1;
                        self.last_key = Some(key);
                        return Some(Ok(row_values));
                    } else {
                        // Update last_key even for filtered out rows to continue scan
                        self.last_key = Some(key);
                    }
                }
                Err(e) => return Some(Err(e)),
            }
        }
        
        // No more rows found
        self.finished = true;
        None
    }
}
