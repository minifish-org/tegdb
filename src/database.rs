//! High-level database interface
//!
//! This module provides a SQLite-like interface for TegDB, making it easy for users
//! to interact with the database without dealing with low-level engine details.

use crate::planner::QueryPlanner;
use crate::sql_utils;
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

    /// Helper function to create TableSchema from CreateTableStatement
    /// Centralizes schema creation logic to avoid duplication
    fn create_table_schema(create_table: &crate::parser::CreateTableStatement) -> TableSchema {
        TableSchema {
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
        }
    }

    /// Helper function to update schema cache for DDL operations
    /// Centralizes schema cache update logic to avoid duplication
    fn update_schema_cache_for_ddl(
        table_schemas: &Arc<RwLock<HashMap<String, TableSchema>>>,
        statement: &crate::parser::Statement,
    ) {
        match statement {
            crate::parser::Statement::CreateTable(create_table) => {
                let schema = Self::create_table_schema(create_table);
                table_schemas
                    .write()
                    .unwrap()
                    .insert(create_table.table.clone(), schema);
            }
            crate::parser::Statement::DropTable(drop_table) => {
                table_schemas
                    .write()
                    .unwrap()
                    .remove(&drop_table.table);
            }
            _ => {} // No schema changes for other statements
        }
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
                // Deserialize schema using centralized utility
                if let Ok(mut schema) = sql_utils::deserialize_schema_from_bytes(&value) {
                    schema.name = table_name.to_string(); // Set the actual table name
                    schemas.insert(table_name.to_string(), schema);
                }
            }
        }

        Ok(())
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

        // Process the result immediately to avoid lifetime conflicts
        let final_result = match result {
            crate::executor::ResultSet::Insert { rows_affected } => rows_affected,
            crate::executor::ResultSet::Update { rows_affected } => rows_affected,
            crate::executor::ResultSet::Delete { rows_affected } => rows_affected,
            crate::executor::ResultSet::CreateTable => 0,
            crate::executor::ResultSet::DropTable => 0,
            crate::executor::ResultSet::Begin => 0,
            crate::executor::ResultSet::Commit => 0,
            crate::executor::ResultSet::Rollback => 0,
            crate::executor::ResultSet::Select { .. } => {
                return Err(crate::Error::Other(
                    "execute() should not be used for SELECT statements. Use query() instead."
                        .to_string(),
                ))
            }
        };
        // Drop the result to release the borrow
        drop(result);

        // Update our shared schemas cache for DDL operations using centralized helper
        Self::update_schema_cache_for_ddl(&self.table_schemas, &statement);

        // Actually commit the engine transaction
        executor.transaction_mut().commit()?;

        Ok(final_result)
    }

    /// Execute SQL query, return all results materialized in memory
    /// This follows the parse -> plan -> execute_plan pipeline but returns simple QueryResult
    pub fn query(&mut self, sql: &str) -> Result<QueryResult> {
        let (_, statement) =
            parse_sql(sql).map_err(|e| crate::Error::Other(format!("SQL parse error: {e:?}")))?;

        // Only SELECT statements make sense for queries
        match &statement {
            crate::parser::Statement::Select(_) => {
                // Clone schemas for the executor
                let schemas = self.table_schemas.read().unwrap().clone();

                // Use a single transaction for this operation
                let transaction = self.engine.begin_transaction();

                // Use the planner pipeline with executor
                let planner = QueryPlanner::new(schemas.clone());
                let mut executor = Executor::new_with_schemas(transaction, schemas);

                // Generate and execute the plan
                let plan = planner.plan(statement)?;

                // Execute and immediately collect results to avoid lifetime issues
                let final_result = {
                    let result = executor.execute_plan(plan)?;
                    match result {
                        crate::executor::ResultSet::Select { columns, rows } => {
                            // Collect all rows from the iterator immediately
                            let collected_rows: Result<Vec<Vec<SqlValue>>> = rows.collect();
                            collected_rows.map(|final_rows| QueryResult {
                                columns,
                                rows: final_rows,
                            })
                        }
                        _ => Err(crate::Error::Other(
                            "Expected SELECT result but got something else".to_string(),
                        )),
                    }
                };

                // Now commit the transaction
                executor.transaction_mut().commit()?;

                final_result
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
    /// Collect rows into a Vec (for compatibility)
    pub fn collect_rows(self) -> Result<Vec<Vec<SqlValue>>> {
        Ok(self.rows)
    }
}

// Allow iterating over QueryResult as a stream of Result<Vec<SqlValue>>
impl IntoIterator for QueryResult {
    type Item = Result<Vec<SqlValue>>;
    type IntoIter = std::vec::IntoIter<Self::Item>;

    fn into_iter(self) -> Self::IntoIter {
        self.rows
            .into_iter()
            .map(Ok)
            .collect::<Vec<_>>()
            .into_iter()
    }
}

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

        // Update schema cache for DDL operations using centralized helper
        Database::update_schema_cache_for_ddl(&self.table_schemas, &statement);

        match result {
            crate::executor::ResultSet::Insert { rows_affected } => Ok(rows_affected),
            crate::executor::ResultSet::Update { rows_affected } => Ok(rows_affected),
            crate::executor::ResultSet::Delete { rows_affected } => Ok(rows_affected),
            crate::executor::ResultSet::CreateTable => Ok(0),
            _ => Ok(0),
        }
    }

    /// Execute SQL query within transaction, return all results materialized in memory
    /// Following the parse -> plan -> execute_plan pipeline
    pub fn query(&mut self, sql: &str) -> Result<QueryResult> {
        let (_, statement) =
            parse_sql(sql).map_err(|e| crate::Error::Other(format!("SQL parse error: {e:?}")))?;

        // Only SELECT statements make sense for queries
        match &statement {
            crate::parser::Statement::Select(_) => {
                // Get schemas from shared cache
                let schemas = self.table_schemas.read().unwrap().clone();

                // Use the planner to generate an optimized execution plan
                let planner = QueryPlanner::new(schemas);
                let plan = planner.plan(statement)?;

                // Execute and immediately collect results
                let result = self.executor.execute_plan(plan)?;
                match result {
                    crate::executor::ResultSet::Select { columns, rows } => {
                        // Collect all rows from the iterator
                        let collected_rows: Result<Vec<Vec<SqlValue>>> = rows.collect();
                        let final_rows = collected_rows?;
                        Ok(QueryResult {
                            columns,
                            rows: final_rows,
                        })
                    }
                    _ => Err(crate::Error::Other(
                        "Expected SELECT result but got something else".to_string(),
                    )),
                }
            }
            _ => {
                // For non-SELECT statements, this doesn't make sense
                Err(crate::Error::Other(
                    "query() should only be used for SELECT statements".to_string(),
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
