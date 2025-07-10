//! Schema catalog management for TegDB
//!
//! This module provides the schema catalog that manages table metadata,
//! similar to the system catalog in traditional RDBMS systems.

use crate::query::{ColumnInfo, TableSchema};
use crate::sql_utils;
use crate::storage_engine::StorageEngine;
use crate::Result;
use std::collections::HashMap;

/// Schema catalog manager for TegDB
///
/// The catalog maintains metadata about tables, columns, indexes, and other
/// database objects, similar to the system catalog in traditional RDBMS.
/// Optimized for single-threaded usage without locks.
pub struct Catalog {
    schemas: HashMap<String, TableSchema>,
    // Cache for column name lookups to avoid repeated HashMap access
    column_cache: HashMap<String, Vec<String>>,
}

impl Catalog {
    /// Create a new empty catalog
    pub fn new() -> Self {
        Self {
            schemas: HashMap::new(),
            column_cache: HashMap::new(),
        }
    }

    /// Create a catalog and load all schemas from storage
    pub fn load_from_storage(storage: &StorageEngine) -> Result<Self> {
        let mut catalog = Self::new();
        catalog.reload_from_storage(storage)?;
        Ok(catalog)
    }

    /// Reload all schemas from storage
    pub fn reload_from_storage(&mut self, storage: &StorageEngine) -> Result<()> {
        self.schemas.clear();
        self.column_cache.clear();
        Self::load_schemas_from_storage(storage, &mut self.schemas)?;

        // Pre-build column cache for faster lookups
        for (table_name, schema) in &self.schemas {
            let column_names: Vec<String> = schema.columns.iter().map(|c| c.name.clone()).collect();
            self.column_cache.insert(table_name.clone(), column_names);
        }
        Ok(())
    }

    /// Get a reference to a table schema by name
    pub fn get_table_schema(&self, table_name: &str) -> Option<&TableSchema> {
        self.schemas.get(table_name)
    }

    /// Get all table schemas (returns reference to avoid cloning)
    pub fn get_all_schemas(&self) -> &HashMap<String, TableSchema> {
        &self.schemas
    }

    /// Get column names for a table (cached for performance)
    pub fn get_column_names(&self, table_name: &str) -> Option<&[String]> {
        self.column_cache.get(table_name).map(|v| v.as_slice())
    }

    /// Add or update a table schema in the catalog
    pub fn add_table_schema(&mut self, schema: TableSchema) {
        let column_names: Vec<String> = schema.columns.iter().map(|c| c.name.clone()).collect();
        self.column_cache.insert(schema.name.clone(), column_names);
        self.schemas.insert(schema.name.clone(), schema);
    }

    /// Remove a table schema from the catalog
    pub fn remove_table_schema(&mut self, table_name: &str) -> Option<TableSchema> {
        self.column_cache.remove(table_name);
        self.schemas.remove(table_name)
    }

    /// Check if a table exists in the catalog
    pub fn table_exists(&self, table_name: &str) -> bool {
        self.schemas.contains_key(table_name)
    }

    /// Get the number of tables in the catalog
    pub fn table_count(&self) -> usize {
        self.schemas.len()
    }

    /// Create a table schema from CREATE TABLE statement
    pub fn create_table_schema(create_table: &crate::parser::CreateTableStatement) -> TableSchema {
        TableSchema {
            name: create_table.table.clone(),
            columns: create_table
                .columns
                .iter()
                .map(|col| ColumnInfo {
                    name: col.name.clone(),
                    data_type: col.data_type.clone(),
                    constraints: col.constraints.clone(),
                })
                .collect(),
        }
    }

    /// Load schemas from storage into the provided HashMap
    /// This is a utility function that can be used by other parts of the system
    pub fn load_schemas_from_storage(
        storage: &StorageEngine,
        schemas: &mut HashMap<String, TableSchema>,
    ) -> Result<()> {
        // Scan for all schema keys
        let schema_prefix = "__schema__:".as_bytes().to_vec();
        let schema_end = "__schema__~".as_bytes().to_vec(); // '~' comes after ':'

        let schema_entries = storage.scan(schema_prefix..schema_end)?;

        for (key, value_arc) in schema_entries {
            // Extract table name from key
            let key_str = String::from_utf8_lossy(&key);
            if let Some(table_name) = key_str.strip_prefix("__schema__:") {
                // Deserialize schema using centralized utility
                if let Ok(mut schema) = sql_utils::deserialize_schema_from_bytes(&value_arc) {
                    schema.name = table_name.to_string(); // Set the actual table name
                    schemas.insert(table_name.to_string(), schema);
                }
            }
        }

        Ok(())
    }
}

impl Default for Catalog {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::parser::{ColumnConstraint, DataType};
    use crate::query::{ColumnInfo, TableSchema};

    #[test]
    fn test_catalog_basic_operations() {
        let mut catalog = Catalog::new();
        assert_eq!(catalog.table_count(), 0);
        assert!(!catalog.table_exists("users"));

        // Create a test schema
        let schema = TableSchema {
            name: "users".to_string(),
            columns: vec![
                ColumnInfo {
                    name: "id".to_string(),
                    data_type: DataType::Integer,
                    constraints: vec![ColumnConstraint::PrimaryKey],
                },
                ColumnInfo {
                    name: "name".to_string(),
                    data_type: DataType::Text,
                    constraints: vec![],
                },
            ],
        };

        catalog.add_table_schema(schema);
        assert_eq!(catalog.table_count(), 1);
        assert!(catalog.table_exists("users"));

        let retrieved = catalog.get_table_schema("users").unwrap();
        assert_eq!(retrieved.name, "users");
        assert_eq!(retrieved.columns.len(), 2);

        // Remove schema
        let removed = catalog.remove_table_schema("users");
        assert!(removed.is_some());
        assert_eq!(catalog.table_count(), 0);
        assert!(!catalog.table_exists("users"));
    }
}
