//! High-level database interface
//!
//! This module provides a SQLite-like interface for TegDB, making it easy for users
//! to interact with the database without dealing with low-level engine details.

use crate::catalog::Catalog;
use crate::executor::{QueryProcessor, TableSchema};
use crate::parser::parse_sql;
use crate::planner::QueryPlanner;
use crate::storage_engine::StorageEngine;
use crate::Result;
use std::collections::HashMap;
use std::rc::Rc;

/// Database connection, similar to sqlite::Connection
///
/// This struct maintains a schema catalog at the database level to avoid
/// repeated schema loading from disk for every query processor creation.
/// Schemas are loaded once when the database is opened and kept in sync
/// with DDL operations (CREATE TABLE, DROP TABLE).
/// Optimized for single-threaded usage without locks.
pub struct Database {
    storage: StorageEngine,
    /// Schema catalog for managing table metadata (no locks needed for single-threaded)
    catalog: Catalog,
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
        let (protocol, path_part) = crate::protocol_utils::parse_storage_identifier(path_str);

        #[cfg(not(target_arch = "wasm32"))]
        {
            // On native platforms, only support file protocol
            if protocol != "file" {
                return Err(crate::Error::Other(format!(
                    "Unsupported protocol: {protocol}. Only 'file://' protocol is supported on native platforms."
                )));
            }

            // Check if path is absolute
            let path_buf = std::path::Path::new(path_part);
            if !path_buf.is_absolute() {
                return Err(crate::Error::Other(format!(
                    "Path must be absolute. Got: '{path_str}'. Use absolute path like 'file:///absolute/path/to/db'"
                )));
            }

            let storage = StorageEngine::new(path_buf.to_path_buf())?;

            // Load all table schemas into the catalog at database initialization
            let catalog = Catalog::load_from_storage(&storage)?;

            Ok(Self { storage, catalog })
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

                    Ok(Self { storage, catalog })
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

    /// Helper function to get schemas in Rc format (no conversion needed)
    fn get_schemas_rc(schemas: &HashMap<String, Rc<TableSchema>>) -> HashMap<String, Rc<TableSchema>> {
        schemas
            .iter()
            .map(|(k, v)| (k.clone(), Rc::clone(v)))
            .collect()
    }

    /// Helper function to update schema catalog for DDL operations
    /// Centralizes schema catalog update logic to avoid duplication
    fn update_schema_catalog_for_ddl(catalog: &mut Catalog, statement: &crate::parser::Statement) {
        match statement {
            crate::parser::Statement::CreateTable(create_table) => {
                let schema = Self::create_table_schema(create_table);
                catalog.add_table_schema(schema);
            }
            crate::parser::Statement::DropTable(drop_table) => {
                catalog.remove_table_schema(&drop_table.table);
            }
            _ => {} // No schema changes for other statements
        }
    }

    /// Centralized query execution helper to eliminate duplication
    /// Executes SELECT statements and returns QueryResult
    fn execute_query_with_processor(
        mut processor: QueryProcessor<'_>,
        sql: &str,
        schemas: &HashMap<String, Rc<TableSchema>>,
    ) -> Result<QueryResult> {
        // Get schemas in Rc format for the planner
        let rc_schemas = Self::get_schemas_rc(schemas);
        Self::execute_query_core(&mut processor, sql, &rc_schemas)
    }

    /// Centralized query execution helper for mutable reference
    /// Executes SELECT statements and returns QueryResult
    fn execute_query_with_processor_ref(
        processor: &mut QueryProcessor<'_>,
        sql: &str,
        schemas: &HashMap<String, Rc<TableSchema>>,
    ) -> Result<QueryResult> {
        // Get schemas in Rc format for the planner
        let rc_schemas = Self::get_schemas_rc(schemas);
        Self::execute_query_core(processor, sql, &rc_schemas)
    }

    /// Core query execution logic - the actual implementation
    /// Executes SELECT statements and returns QueryResult
    fn execute_query_core(
        processor: &mut QueryProcessor<'_>,
        sql: &str,
        schemas: &HashMap<String, Rc<TableSchema>>,
    ) -> Result<QueryResult> {
        let (_, statement) =
            parse_sql(sql).map_err(|e| crate::Error::Other(format!("SQL parse error: {e:?}")))?;

        // Only SELECT statements make sense for queries
        match &statement {
            crate::parser::Statement::Select(_) => {
                // Use the planner to generate an optimized execution plan
                let planner = QueryPlanner::new(schemas.clone());
                let plan = planner.plan(statement)?;

                // Execute and immediately collect results
                let result = processor.execute_plan(plan)?;
                match result {
                    crate::executor::ResultSet::Select { columns, rows } => {
                        // Collect all rows from the iterator
                        let collected_rows: Result<Vec<Vec<crate::parser::SqlValue>>> = rows.collect();
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

        // Get schemas in Rc format for shared ownership (no cloning needed)
        let schemas = Self::get_schemas_rc(self.catalog.get_all_schemas());

        // Use the new planner pipeline with executor
        let planner = QueryPlanner::new(schemas.clone());
        let mut processor = QueryProcessor::new_with_rc_schemas(transaction, schemas);

        // Generate and execute the plan (no need to begin transaction as it's already started)
        let plan = planner.plan(statement.clone())?;
        let result = processor.execute_plan(plan)?;

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
        Self::update_schema_catalog_for_ddl(&mut self.catalog, &statement);

        // Actually commit the engine transaction
        processor.transaction_mut().commit()?;

        Ok(final_result)
    }

    /// Execute SQL query, return all results materialized in memory
    /// This follows the parse -> plan -> execute_plan pipeline but returns simple QueryResult
    pub fn query(&mut self, sql: &str) -> Result<QueryResult> {
        // Get schemas in Rc format for shared ownership (no cloning needed)
        let schemas = Self::get_schemas_rc(self.catalog.get_all_schemas());

        // Use a single transaction for this operation
        let transaction = self.storage.begin_transaction();

        // Create executor with schemas
        let processor = QueryProcessor::new_with_rc_schemas(transaction, schemas.clone());

        // Use centralized query execution helper
        let result = Self::execute_query_with_processor(processor, sql, self.catalog.get_all_schemas())?;

        Ok(result)
    }

    /// Begin a new database transaction
    pub fn begin_transaction(&mut self) -> Result<DatabaseTransaction<'_>> {
        let schemas = Self::get_schemas_rc(self.catalog.get_all_schemas());
        let transaction = self.storage.begin_transaction();
        let processor = QueryProcessor::new_with_rc_schemas(transaction, schemas);

        Ok(DatabaseTransaction {
            processor,
            catalog: &mut self.catalog,
        })
    }



    /// Get a reference to all cached table schemas (no cloning)
    /// Use this when you only need to read schema information
    pub fn get_table_schemas_ref(&self) -> &HashMap<String, Rc<TableSchema>> {
        self.catalog.get_all_schemas()
    }

    /// Get a copy of all cached table schemas
    /// Useful for debugging or introspection
    /// Note: This clones the entire schema HashMap - use sparingly
    pub fn get_table_schemas(&self) -> HashMap<String, TableSchema> {
        self.catalog.get_all_schemas().iter().map(|(k, v)| (k.clone(), (**v).clone())).collect()
    }
}

/// Query result containing columns and rows
#[derive(Debug, Clone, PartialEq)]
pub struct QueryResult {
    columns: Vec<String>,
    rows: Vec<Vec<crate::parser::SqlValue>>,
}

impl QueryResult {
    /// Get column names
    pub fn columns(&self) -> &[String] {
        &self.columns
    }

    /// Get all rows
    pub fn rows(&self) -> &[Vec<crate::parser::SqlValue>] {
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
    pub fn collect_rows(self) -> Result<Vec<Vec<crate::parser::SqlValue>>> {
        Ok(self.rows)
    }
}

// Allow iterating over QueryResult as a stream of Result<Vec<SqlValue>>
impl IntoIterator for QueryResult {
    type Item = Result<Vec<crate::parser::SqlValue>>;
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
    catalog: &'a mut Catalog,
}

impl DatabaseTransaction<'_> {
    /// Execute SQL statement within transaction
    pub fn execute(&mut self, sql: &str) -> Result<usize> {
        let (_, statement) =
            parse_sql(sql).map_err(|e| crate::Error::Other(format!("SQL parse error: {e:?}")))?;

        // Get schemas from shared catalog and convert to Rc
        let schemas = Database::get_schemas_rc(self.catalog.get_all_schemas());

        // Use the planner pipeline
        let planner = QueryPlanner::new(schemas);
        let plan = planner.plan(statement.clone())?;
        let result = self.processor.execute_plan(plan)?;

        // Update schema cache for DDL operations using centralized helper
        Database::update_schema_catalog_for_ddl(self.catalog, &statement);

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
        // Get schemas from shared cache (reuse existing schemas in processor)
        let schemas = self.catalog.get_all_schemas().clone();

        // Use centralized query execution helper
        // Note: We need to be careful about borrowing here since we can't move self.executor
        // Instead, we'll use a more direct approach that's still centralized
        Database::execute_query_with_processor_ref(&mut self.processor, sql, &schemas)
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
