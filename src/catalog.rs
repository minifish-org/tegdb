//! Schema catalog management for TegDB
//!
//! This module provides the schema catalog that manages table metadata,
//! similar to the system catalog in traditional RDBMS systems.

use crate::executor::{ColumnInfo, TableSchema};
use crate::sql_utils;
use crate::storage_engine::StorageEngine;
use crate::Result;
use std::collections::HashMap;
use std::rc::Rc;

/// Storage key prefix for schema entries
pub const SCHEMA_KEY_PREFIX: &str = "S:";
/// Storage key end marker for schema entries (comes after ':' in lexicographic order)
pub const SCHEMA_KEY_END: &str = "S~";
/// Default table name for unknown schemas during deserialization
pub const UNKNOWN_TABLE_NAME: &str = "unknown";

/// Schema catalog manager for TegDB
///
/// The catalog maintains metadata about tables, columns, indexes, and other
/// database objects, similar to the system catalog in traditional RDBMS.
/// Optimized for single-threaded usage without locks.
pub struct Catalog {
    schemas: HashMap<String, Rc<TableSchema>>,
}

impl Catalog {
    /// Create a new empty catalog
    pub fn new() -> Self {
        Self {
            schemas: HashMap::new(),
        }
    }

    /// Create a catalog and load all schemas from storage
    pub fn load_from_storage(storage: &StorageEngine) -> Result<Self> {
        let mut catalog = Self::new();
        Self::load_schemas_from_storage(storage, &mut catalog.schemas)?;
        Ok(catalog)
    }

    /// Get a reference to a table schema by name
    pub fn get_table_schema(&self, table_name: &str) -> Option<&Rc<TableSchema>> {
        self.schemas.get(table_name)
    }

    /// Get all table schemas (returns reference to avoid cloning)
    pub fn get_all_schemas(&self) -> &HashMap<String, Rc<TableSchema>> {
        &self.schemas
    }

    /// Add or update a table schema in the catalog
    pub fn add_table_schema(&mut self, schema: TableSchema) {
        self.schemas.insert(schema.name.clone(), Rc::new(schema));
    }

    /// Remove a table schema from the catalog
    pub fn remove_table_schema(&mut self, table_name: &str) -> Option<Rc<TableSchema>> {
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
        schemas: &mut HashMap<String, Rc<TableSchema>>,
    ) -> Result<()> {
        // Scan for all schema keys
        let schema_prefix = SCHEMA_KEY_PREFIX.as_bytes().to_vec();
        let schema_end = SCHEMA_KEY_END.as_bytes().to_vec(); // '~' comes after ':'

        let schema_entries = storage.scan(schema_prefix..schema_end)?;

        for (key, value_rc) in schema_entries {
            // Extract table name from key
            let key_str = String::from_utf8_lossy(&key);
            if let Some(table_name) = key_str.strip_prefix(SCHEMA_KEY_PREFIX) {
                // Deserialize schema using centralized utility
                if let Ok(mut schema) = sql_utils::deserialize_schema_from_bytes(&value_rc) {
                    schema.name = table_name.to_string(); // Set the actual table name
                    schemas.insert(table_name.to_string(), Rc::new(schema));
                }
            }
        }

        Ok(())
    }

    /// Serialize a table schema to bytes for storage
    /// This provides a centralized schema serialization format
    pub fn serialize_schema_to_bytes(schema: &TableSchema) -> Vec<u8> {
        let mut schema_data = Vec::new();

        for (i, col) in schema.columns.iter().enumerate() {
            if i > 0 {
                schema_data.push(b'|');
            }
            schema_data.extend_from_slice(col.name.as_bytes());
            schema_data.push(b':');
            let type_str = format!("{:?}", col.data_type);
            schema_data.extend_from_slice(type_str.as_bytes());

            if !col.constraints.is_empty() {
                schema_data.push(b':');
                for (j, constraint) in col.constraints.iter().enumerate() {
                    if j > 0 {
                        schema_data.push(b',');
                    }
                    let constraint_str = match constraint {
                        crate::parser::ColumnConstraint::PrimaryKey => "PRIMARY_KEY",
                        crate::parser::ColumnConstraint::NotNull => "NOT_NULL",
                        crate::parser::ColumnConstraint::Unique => "UNIQUE",
                    };
                    schema_data.extend_from_slice(constraint_str.as_bytes());
                }
            }
        }

        schema_data
    }

    /// Get schema storage key for a table
    pub fn get_schema_storage_key(table_name: &str) -> String {
        format!("{SCHEMA_KEY_PREFIX}{table_name}")
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
    use crate::executor::{ColumnInfo, TableSchema};
    use crate::parser::{ColumnConstraint, DataType};

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
                    data_type: DataType::Text(None),
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

        // Test schema serialization
        let serialized = Catalog::serialize_schema_to_bytes(retrieved);
        assert!(!serialized.is_empty());

        // Test storage key generation
        let storage_key = Catalog::get_schema_storage_key("users");
        assert_eq!(storage_key, "S:users");

        // Remove schema
        let removed = catalog.remove_table_schema("users");
        assert!(removed.is_some());
        assert_eq!(catalog.table_count(), 0);
        assert!(!catalog.table_exists("users"));
    }
}
