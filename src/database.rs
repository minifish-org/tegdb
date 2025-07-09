//! High-level database interface
//!
//! This module provides a SQLite-like interface for TegDB, making it easy for users
//! to interact with the database without dealing with low-level engine details.

use crate::planner::QueryPlanner;
use crate::{
    catalog::Catalog,
    parser::{parse_sql, SqlValue},
    query::{QueryProcessor, TableSchema},
    storage_engine::StorageEngine,
    Result,
};
use crate::protocol_utils::parse_storage_identifier;
use std::{
    collections::HashMap,
    path::Path,
    sync::{Arc, RwLock},
};

/// Database connection, similar to sqlite::Connection
///
/// This struct maintains a schema catalog at the database level to avoid
/// repeated schema loading from disk for every query processor creation.
/// Schemas are loaded once when the database is opened and kept in sync
/// with DDL operations (CREATE TABLE, DROP TABLE).
pub struct Database {
    storage: StorageEngine,
    /// Schema catalog for managing table metadata
    catalog: Arc<RwLock<Catalog>>,
}

impl Database {
    /// Create or open database
    /// 
    /// On native platforms: Only accepts absolute paths with the file:// protocol.
    /// On WASM platforms: Supports browser://, localStorage://, and indexeddb:// protocols.
    /// 
    /// Examples:
    /// - ✅ file:///absolute/path/to/db (native only)
    /// - ✅ browser://my-app-db (WASM only)
    /// - ✅ localStorage://user-data (WASM only)
    /// - ✅ indexeddb://app-cache (WASM only)
    /// - ❌ relative/path (no protocol)
    /// - ❌ file://relative/path (relative path with protocol)
    pub fn open<P: AsRef<str>>(path: P) -> Result<Self> {
        let path_str = path.as_ref();
        let (protocol, path_part) = parse_storage_identifier(path_str);
        
        #[cfg(not(target_arch = "wasm32"))]
        {
            // On native platforms, only support file protocol
            if protocol != "file" {
                return Err(crate::Error::Other(format!(
                    "Unsupported protocol: {}. Only 'file://' protocol is supported on native platforms.",
                    protocol
                )));
            }
            
            // Check if path is absolute
            let path_buf = Path::new(path_part);
            if !path_buf.is_absolute() {
                return Err(crate::Error::Other(format!(
                    "Path must be absolute. Got: '{}'. Use absolute path like 'file:///absolute/path/to/db'",
                    path_str
                )));
            }
            
            let storage = StorageEngine::new(path_buf.to_path_buf())?;
            
            // Load all table schemas into the catalog at database initialization
            let catalog = Catalog::load_from_storage(&storage)?;

            Ok(Self {
                storage,
                catalog: Arc::new(RwLock::new(catalog)),
            })
        }
        
        #[cfg(target_arch = "wasm32")]
        {
            // On WASM platforms, support browser protocols
            match protocol {
                "browser" | "localstorage" | "indexeddb" => {
                    // For browser backends, we use the full identifier string
                    let storage = StorageEngine::new_with_identifier(path_str.to_string())?;
                    
                    // Load all table schemas into the catalog at database initialization
                    let catalog = Catalog::load_from_storage(&storage)?;

                    Ok(Self {
                        storage,
                        catalog: Arc::new(RwLock::new(catalog)),
                    })
                }
                "file" => {
                    // File protocol is not supported on WASM
                    return Err(crate::Error::Other(format!(
                        "File protocol is not supported on WASM. Use 'browser://', 'localstorage://', or 'indexeddb://' protocols instead."
                    )));
                }
                _ => {
                    return Err(crate::Error::Other(format!(
                        "Unsupported protocol: {}. On WASM, only 'browser://', 'localstorage://', and 'indexeddb://' protocols are supported.",
                        protocol
                    )));
                }
            }
        }
    }

    /// Helper function to create TableSchema from CreateTableStatement
    /// Centralizes schema creation logic to avoid duplication
    fn create_table_schema(create_table: &crate::parser::CreateTableStatement) -> TableSchema {
        Catalog::create_table_schema(create_table)
    }

    /// Helper function to update schema catalog for DDL operations
    /// Centralizes schema catalog update logic to avoid duplication
    fn update_schema_catalog_for_ddl(
        catalog: &Arc<RwLock<Catalog>>,
        statement: &crate::parser::Statement,
    ) {
        match statement {
            crate::parser::Statement::CreateTable(create_table) => {
                let schema = Self::create_table_schema(create_table);
                catalog.write().unwrap().add_table_schema(schema);
            }
            crate::parser::Statement::DropTable(drop_table) => {
                catalog
                    .write()
                    .unwrap()
                    .remove_table_schema(&drop_table.table);
            }
            _ => {} // No schema changes for other statements
        }
    }

    /// Centralized query execution helper to eliminate duplication
    /// Executes SELECT statements and returns QueryResult
    fn execute_query_with_processor(
        mut processor: QueryProcessor<'_>,
        sql: &str,
        schemas: HashMap<String, TableSchema>,
    ) -> Result<QueryResult> {
        Self::execute_query_core(&mut processor, sql, schemas)
    }

    /// Centralized query execution helper for mutable reference
    /// Executes SELECT statements and returns QueryResult
    fn execute_query_with_processor_ref(
        processor: &mut QueryProcessor<'_>,
        sql: &str,
        schemas: HashMap<String, TableSchema>,
    ) -> Result<QueryResult> {
        Self::execute_query_core(processor, sql, schemas)
    }

    /// Core query execution logic - the actual implementation
    /// Executes SELECT statements and returns QueryResult
    fn execute_query_core(
        processor: &mut QueryProcessor<'_>,
        sql: &str,
        schemas: HashMap<String, TableSchema>,
    ) -> Result<QueryResult> {
        let (_, statement) =
            parse_sql(sql).map_err(|e| crate::Error::Other(format!("SQL parse error: {e:?}")))?;

        // Only SELECT statements make sense for queries
        match &statement {
            crate::parser::Statement::Select(_) => {
                // Use the planner to generate an optimized execution plan
                let planner = QueryPlanner::new(schemas);
                let plan = planner.plan(statement)?;

                // Execute and immediately collect results
                let result = processor.execute_plan(plan)?;
                match result {
                    crate::query::ResultSet::Select { columns, rows } => {
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

    /// Execute SQL statement, return number of affected rows
    pub fn execute(&mut self, sql: &str) -> Result<usize> {
        let (_, statement) =
            parse_sql(sql).map_err(|e| crate::Error::Other(format!("SQL parse error: {e:?}")))?;

        // Use a single transaction for this operation
        let transaction = self.storage.begin_transaction();

        // Use the new planner pipeline with executor
        let planner = QueryPlanner::new(self.catalog.read().unwrap().get_all_schemas().clone());
        let mut processor = QueryProcessor::new_with_schemas(
            transaction,
            self.catalog.read().unwrap().get_all_schemas().clone(),
        );

        // Generate and execute the plan (no need to begin transaction as it's already started)
        let plan = planner.plan(statement.clone())?;
        let result = processor.execute_plan(plan)?;

        // Process the result immediately to avoid lifetime conflicts
        let final_result = match result {
            crate::query::ResultSet::Insert { rows_affected } => rows_affected,
            crate::query::ResultSet::Update { rows_affected } => rows_affected,
            crate::query::ResultSet::Delete { rows_affected } => rows_affected,
            crate::query::ResultSet::CreateTable => 0,
            crate::query::ResultSet::DropTable => 0,
            crate::query::ResultSet::Begin => 0,
            crate::query::ResultSet::Commit => 0,
            crate::query::ResultSet::Rollback => 0,
            crate::query::ResultSet::Select { .. } => {
                return Err(crate::Error::Other(
                    "execute() should not be used for SELECT statements. Use query() instead."
                        .to_string(),
                ))
            }
        };
        // Drop the result to release the borrow
        drop(result);

        // Update our shared schemas cache for DDL operations using centralized helper
        Self::update_schema_catalog_for_ddl(&self.catalog, &statement);

        // Actually commit the engine transaction
        processor.transaction_mut().commit()?;

        Ok(final_result)
    }

    /// Execute SQL query, return all results materialized in memory
    /// This follows the parse -> plan -> execute_plan pipeline but returns simple QueryResult
    pub fn query(&mut self, sql: &str) -> Result<QueryResult> {
        // Clone schemas for the executor
        let schemas = self.catalog.read().unwrap().get_all_schemas().clone();

        // Use a single transaction for this operation
        let transaction = self.storage.begin_transaction();

        // Create executor with schemas
        let processor = QueryProcessor::new_with_schemas(
            transaction,
            self.catalog.read().unwrap().get_all_schemas().clone(),
        );

        // Use centralized query execution helper
        let result = Self::execute_query_with_processor(processor, sql, schemas)?;

        Ok(result)
    }

    /// Begin a new database transaction
    pub fn begin_transaction(&mut self) -> Result<DatabaseTransaction<'_>> {
        let schemas = self.catalog.read().unwrap().get_all_schemas().clone();
        let transaction = self.storage.begin_transaction();
        let processor = QueryProcessor::new_with_schemas(transaction, schemas);

        Ok(DatabaseTransaction {
            processor,
            catalog: Arc::clone(&self.catalog),
        })
    }

    /// Reload table schemas from storage
    /// This can be useful if the database was modified externally
    pub fn refresh_schema_cache(&mut self) -> Result<()> {
        self.catalog
            .write()
            .unwrap()
            .reload_from_storage(&self.storage)?;
        Ok(())
    }

    /// Get a copy of all cached table schemas
    /// Useful for debugging or introspection
    pub fn get_table_schemas(&self) -> HashMap<String, TableSchema> {
        self.catalog.read().unwrap().get_all_schemas().clone()
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
    processor: QueryProcessor<'a>,
    catalog: Arc<RwLock<Catalog>>,
}

impl DatabaseTransaction<'_> {
    /// Execute SQL statement within transaction
    pub fn execute(&mut self, sql: &str) -> Result<usize> {
        let (_, statement) =
            parse_sql(sql).map_err(|e| crate::Error::Other(format!("SQL parse error: {e:?}")))?;

        // Get schemas from shared catalog
        let schemas = self.catalog.read().unwrap().get_all_schemas().clone();

        // Use the planner pipeline
        let planner = QueryPlanner::new(schemas);
        let plan = planner.plan(statement.clone())?;
        let result = self.processor.execute_plan(plan)?;

        // Update schema cache for DDL operations using centralized helper
        Database::update_schema_catalog_for_ddl(&self.catalog, &statement);

        match result {
            crate::query::ResultSet::Insert { rows_affected } => Ok(rows_affected),
            crate::query::ResultSet::Update { rows_affected } => Ok(rows_affected),
            crate::query::ResultSet::Delete { rows_affected } => Ok(rows_affected),
            crate::query::ResultSet::CreateTable => Ok(0),
            _ => Ok(0),
        }
    }

    /// Execute SQL query within transaction, return all results materialized in memory
    /// Following the parse -> plan -> execute_plan pipeline
    pub fn query(&mut self, sql: &str) -> Result<QueryResult> {
        // Get schemas from shared cache
        let schemas = self.catalog.read().unwrap().get_all_schemas().clone();

        // Use centralized query execution helper
        // Note: We need to be careful about borrowing here since we can't move self.executor
        // Instead, we'll use a more direct approach that's still centralized
        Database::execute_query_with_processor_ref(&mut self.processor, sql, schemas)
    }

    /// Commit the transaction
    pub fn commit(mut self) -> Result<()> {
        self.processor.transaction_mut().commit()
    }

    /// Rollback the transaction
    pub fn rollback(mut self) -> Result<()> {
        self.processor.transaction_mut().rollback()
    }
}
