//! High-level database interface
//!
//! This module provides a SQLite-like interface for TegDB, making it easy for users
//! to interact with the database without dealing with low-level engine details.

use crate::planner::QueryPlanner;
use crate::{
    engine::Engine,
    executor::{Executor, TableSchema},
    parser::{parse_sql, SqlValue},
    Result,
};
use std::{
    collections::HashMap,
    path::Path,
    sync::{Arc, RwLock},
};

/// Type alias for scan iterator result
type ScanIterator<'a> = Box<dyn Iterator<Item = (Vec<u8>, Arc<[u8]>)> + 'a>;

/// Trait for types that can perform scanning operations (Engine or Transaction)
pub trait Scannable {
    fn scan(&self, range: std::ops::Range<Vec<u8>>) -> Result<ScanIterator<'_>>;
}

impl Scannable for crate::engine::Engine {
    fn scan(
        &self,
        range: std::ops::Range<Vec<u8>>,
    ) -> Result<Box<dyn Iterator<Item = (Vec<u8>, std::sync::Arc<[u8]>)> + '_>> {
        self.scan(range)
    }
}

impl Scannable for crate::engine::Transaction<'_> {
    fn scan(
        &self,
        range: std::ops::Range<Vec<u8>>,
    ) -> Result<Box<dyn Iterator<Item = (Vec<u8>, std::sync::Arc<[u8]>)> + '_>> {
        self.scan(range)
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
        schemas: &mut HashMap<String, TableSchema>,
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
        let mut columns = Vec::new();
        let mut start = 0;

        for (i, &byte) in data.iter().enumerate() {
            if byte == b'|' {
                if i > start {
                    let column_part = &data[start..i];
                    Self::parse_column_part(column_part, &mut columns);
                }
                start = i + 1;
            }
        }

        if start < data.len() {
            let column_part = &data[start..];
            Self::parse_column_part(column_part, &mut columns);
        }

        Ok(TableSchema {
            name: "unknown".to_string(), // Will be set by caller
            columns,
        })
    }

    // Helper to parse a single column entry to avoid repetition
    fn parse_column_part(column_part: &[u8], columns: &mut Vec<crate::executor::ColumnInfo>) {
        let mut parts = column_part.splitn(3, |&b| b == b':');
        if let (Some(name_bytes), Some(type_bytes)) = (parts.next(), parts.next()) {
            let name = String::from_utf8_lossy(name_bytes).to_string();
            let type_str = String::from_utf8_lossy(type_bytes);

            let data_type = match type_str.as_ref() {
                "Integer" | "INTEGER" => crate::parser::DataType::Integer,
                "Text" | "TEXT" => crate::parser::DataType::Text,
                "Real" | "REAL" => crate::parser::DataType::Real,
                "Blob" | "BLOB" => crate::parser::DataType::Blob,
                _ => crate::parser::DataType::Text, // Default fallback
            };

            let constraints = if let Some(constraints_bytes) = parts.next() {
                constraints_bytes
                    .split(|&b| b == b',')
                    .filter_map(|c| match c {
                        b"PRIMARY_KEY" => Some(crate::parser::ColumnConstraint::PrimaryKey),
                        b"NOT_NULL" => Some(crate::parser::ColumnConstraint::NotNull),
                        b"UNIQUE" => Some(crate::parser::ColumnConstraint::Unique),
                        _ => None,
                    })
                    .collect()
            } else {
                Vec::new()
            };

            columns.push(crate::executor::ColumnInfo {
                name,
                data_type,
                constraints,
            });
        }
    }

    /// Execute SQL statement, return number of affected rows
    pub fn execute(&mut self, sql: &str) -> Result<usize> {
        let (_, statement) =
            parse_sql(sql).map_err(|e| crate::Error::Other(format!("SQL parse error: {e:?}")))?;

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
                    columns: create_table
                        .columns
                        .iter()
                        .map(|col| crate::executor::ColumnInfo {
                            name: col.name.clone(),
                            data_type: col.data_type.clone(),
                            constraints: col.constraints.clone(),
                        })
                        .collect(),
                };
                self.table_schemas
                    .write()
                    .unwrap()
                    .insert(create_table.table.clone(), schema);
            }
            crate::parser::Statement::DropTable(drop_table) => {
                // Remove table schema from cache when table is dropped
                self.table_schemas
                    .write()
                    .unwrap()
                    .remove(&drop_table.table);
            }
            _ => {} // No schema changes for other statements
        }

        // Actually commit the engine transaction
        executor.transaction_mut().commit()?;

        match result {
            crate::executor::ResultSet::Insert { rows_affected } => Ok(rows_affected),
            crate::executor::ResultSet::Update { rows_affected } => Ok(rows_affected),
            crate::executor::ResultSet::Delete { rows_affected } => Ok(rows_affected),
            crate::executor::ResultSet::CreateTable => Ok(0),
            _ => Ok(0),
        }
    }

    /// Execute SQL query, return true streaming results that yield rows on-demand
    /// This is the new streaming API that doesn't materialize all rows upfront
    /// Following the parse -> plan -> execute_plan pipeline
    pub fn query(&mut self, sql: &str) -> Result<StreamingQuery> {
        let (_, statement) =
            parse_sql(sql).map_err(|e| crate::Error::Other(format!("SQL parse error: {e:?}")))?;

        // Only SELECT statements make sense for streaming
        match &statement {
            crate::parser::Statement::Select(_) => {
                // Clone schemas for the planner
                let schemas = self.table_schemas.read().unwrap().clone();

                // Use the planner to generate an optimized execution plan
                let planner = QueryPlanner::new(schemas.clone());
                let plan = planner.plan(statement)?;

                // Create streaming query that executes the plan against the engine
                let streaming_query = BaseStreamingQuery::from_plan(&self.engine, plan, schemas)?;

                Ok(streaming_query)
            }
            _ => {
                // For non-SELECT statements, this doesn't make sense
                Err(crate::Error::Other(
                    "query() should only be used for SELECT statements".to_string(),
                ))
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

/// Generic streaming query that works with any scannable backend
/// This single implementation replaces both StreamingQuery and TransactionStreamingQuery
pub struct BaseStreamingQuery<'a, S: Scannable> {
    columns: Vec<String>,
    scanner: &'a S,
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

impl<'a, S: Scannable> BaseStreamingQuery<'a, S> {
    /// Create a streaming query from an execution plan
    /// This follows the proper parse -> plan -> execute_plan pipeline
    fn from_plan(
        scanner: &'a S,
        plan: crate::planner::ExecutionPlan,
        schemas: HashMap<String, TableSchema>,
    ) -> Result<Self> {
        use crate::planner::ExecutionPlan;

        match plan {
            ExecutionPlan::PrimaryKeyLookup {
                table,
                pk_values,
                selected_columns,
                additional_filter,
            } => {
                let schema = schemas
                    .get(&table)
                    .ok_or_else(|| crate::Error::Other(format!("Table '{table}' not found")))?
                    .clone();

                // For primary key lookups, we need to create a special filter condition
                // that combines the PK equality conditions with any additional filter
                let pk_filter = Self::create_pk_filter_condition(&pk_values, additional_filter)?;

                Ok(Self {
                    columns: selected_columns.clone(),
                    scanner,
                    table_name: table,
                    selected_columns,
                    filter: Some(pk_filter),
                    limit: Some(1), // PK lookups should return at most 1 row
                    schema,
                    storage_format: crate::storage_format::StorageFormat::new(),
                    last_key: None,
                    count: 0,
                    finished: false,
                })
            }
            ExecutionPlan::TableScan {
                table,
                selected_columns,
                filter,
                limit,
                ..
            } => {
                let schema = schemas
                    .get(&table)
                    .ok_or_else(|| crate::Error::Other(format!("Table '{table}' not found")))?
                    .clone();

                Ok(Self {
                    columns: selected_columns.clone(),
                    scanner,
                    table_name: table,
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
            _ => Err(crate::Error::Other(
                "Streaming query only supports SELECT execution plans".to_string(),
            )),
        }
    }

    /// Create a filter condition from primary key values
    fn create_pk_filter_condition(
        pk_values: &HashMap<String, SqlValue>,
        additional_filter: Option<crate::parser::Condition>,
    ) -> Result<crate::parser::Condition> {
        use crate::parser::{ComparisonOperator, Condition};

        if pk_values.is_empty() {
            return Err(crate::Error::Other(
                "Primary key lookup requires at least one key value".to_string(),
            ));
        }

        // Convert PK values to equality conditions
        let pk_conditions: Vec<Condition> = pk_values
            .iter()
            .map(|(column, value)| Condition::Comparison {
                left: column.clone(),
                operator: ComparisonOperator::Equal,
                right: value.clone(),
            })
            .collect();

        // Combine all PK conditions with AND
        let combined_pk_condition = if pk_conditions.len() == 1 {
            pk_conditions.into_iter().next().unwrap()
        } else {
            pk_conditions
                .into_iter()
                .reduce(|acc, condition| Condition::And(Box::new(acc), Box::new(condition)))
                .unwrap()
        };

        // If there's an additional filter, combine it with the PK condition
        if let Some(additional) = additional_filter {
            Ok(Condition::And(
                Box::new(combined_pk_condition),
                Box::new(additional),
            ))
        } else {
            Ok(combined_pk_condition)
        }
    }

    /// Get column names
    pub fn columns(&self) -> &[String] {
        &self.columns
    }

    /// Collect all remaining rows into a Vec (for compatibility)
    pub fn collect_rows(self) -> Result<Vec<Vec<SqlValue>>> {
        let mut rows = Vec::new();
        for row_result in self {
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
    fn evaluate_condition(
        condition: &crate::parser::Condition,
        row_data: &std::collections::HashMap<String, SqlValue>,
    ) -> bool {
        use crate::parser::Condition;

        match condition {
            Condition::Comparison {
                left,
                operator,
                right,
            } => {
                let row_value = row_data.get(left).unwrap_or(&SqlValue::Null);
                Self::compare_values(row_value, operator, right)
            }
            Condition::And(left, right) => {
                Self::evaluate_condition(left, row_data)
                    && Self::evaluate_condition(right, row_data)
            }
            Condition::Or(left, right) => {
                Self::evaluate_condition(left, row_data)
                    || Self::evaluate_condition(right, row_data)
            }
        }
    }

    /// Compare two SqlValues using the given operator
    fn compare_values(
        left: &SqlValue,
        operator: &crate::parser::ComparisonOperator,
        right: &SqlValue,
    ) -> bool {
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

impl<S: Scannable> Iterator for BaseStreamingQuery<'_, S> {
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
        let scan_result = match self.scanner.scan(start_key..end_key) {
            Ok(scan) => scan,
            Err(e) => return Some(Err(e)),
        };

        // Process rows until we find one that matches the filter
        for (key, value) in scan_result {
            match self.storage_format.deserialize_row(&value, &self.schema) {
                Ok(row_data) => {
                    // Apply filter if present
                    let matches = if let Some(ref filter) = self.filter {
                        Self::evaluate_condition(filter, &row_data)
                    } else {
                        true
                    };

                    if matches {
                        // Extract selected columns
                        let mut row_values = Vec::new();
                        for col_name in &self.selected_columns {
                            row_values
                                .push(row_data.get(col_name).cloned().unwrap_or(SqlValue::Null));
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

/// Type aliases to maintain API compatibility
/// These replace the original struct definitions with zero-cost abstractions
pub type StreamingQuery<'a> = BaseStreamingQuery<'a, crate::engine::Engine>;
pub type TransactionStreamingQuery<'a> = BaseStreamingQuery<'a, crate::engine::Transaction<'a>>;

/// Transaction handle for batch operations
pub struct DatabaseTransaction<'a> {
    executor: Executor<'a>,
    table_schemas: Arc<RwLock<HashMap<String, TableSchema>>>,
}

impl DatabaseTransaction<'_> {
    /// Execute SQL statement within transaction
    pub fn execute(&mut self, sql: &str) -> Result<usize> {
        let (_, statement) =
            parse_sql(sql).map_err(|e| crate::Error::Other(format!("SQL parse error: {e:?}")))?;

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
                    columns: create_table
                        .columns
                        .iter()
                        .map(|col| crate::executor::ColumnInfo {
                            name: col.name.clone(),
                            data_type: col.data_type.clone(),
                            constraints: col.constraints.clone(),
                        })
                        .collect(),
                };
                self.table_schemas
                    .write()
                    .unwrap()
                    .insert(create_table.table.clone(), schema);
            }
            crate::parser::Statement::DropTable(drop_table) => {
                self.table_schemas
                    .write()
                    .unwrap()
                    .remove(&drop_table.table);
            }
            _ => {}
        }

        match result {
            crate::executor::ResultSet::Insert { rows_affected } => Ok(rows_affected),
            crate::executor::ResultSet::Update { rows_affected } => Ok(rows_affected),
            crate::executor::ResultSet::Delete { rows_affected } => Ok(rows_affected),
            crate::executor::ResultSet::CreateTable => Ok(0),
            _ => Ok(0),
        }
    }

    /// Execute SQL query within transaction using true streaming
    /// This provides the same streaming capability as Database::query but within a transaction context
    /// Following the parse -> plan -> execute_plan pipeline
    pub fn streaming_query(&mut self, sql: &str) -> Result<TransactionStreamingQuery> {
        let (_, statement) =
            parse_sql(sql).map_err(|e| crate::Error::Other(format!("SQL parse error: {e:?}")))?;

        // Only SELECT statements make sense for streaming
        match &statement {
            crate::parser::Statement::Select(_) => {
                // Get schemas from shared cache
                let schemas = self.table_schemas.read().unwrap().clone();

                // Use the planner to generate an optimized execution plan
                let planner = QueryPlanner::new(schemas.clone());
                let plan = planner.plan(statement)?;

                // Create streaming query that executes the plan against the transaction
                let streaming_query = BaseStreamingQuery::from_plan(
                    self.executor.transaction(),
                    plan,
                    schemas,
                )?;

                Ok(streaming_query)
            }
            _ => {
                // For non-SELECT statements, this doesn't make sense
                Err(crate::Error::Other(
                    "streaming_query() should only be used for SELECT statements".to_string(),
                ))
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
