//! Schema catalog management for TegDB
//!
//! This module provides the schema catalog that manages table metadata,
//! similar to the system catalog in traditional RDBMS systems.

use crate::query_processor::{ColumnInfo, TableSchema};
use crate::sql_utils;
use crate::storage_engine::StorageEngine;
use crate::Result;
use std::collections::HashMap;
use std::rc::Rc;

/// Storage key prefix for schema entries
pub const SCHEMA_KEY_PREFIX: &str = "S:";
/// Storage key end marker for schema entries (comes after ':' in lexicographic order)
pub const SCHEMA_KEY_END: &str = "S~";
/// Storage key prefix for index entries
pub const INDEX_KEY_PREFIX: &str = "I:";
/// Storage key end marker for index entries (comes after ':' in lexicographic order)
pub const INDEX_KEY_END: &str = "I~";
/// Default table name for unknown schemas during deserialization
pub const UNKNOWN_TABLE_NAME: &str = "unknown";

/// Index information for table schema
#[derive(Debug, Clone)]
pub struct IndexInfo {
    pub name: String,
    pub table_name: String,
    pub column_name: String,
    pub unique: bool,
}

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
        
        // Load indexes and add them to the appropriate tables
        let indexes = Self::load_indexes_from_storage(storage)?;
        for index in indexes {
            let _ = catalog.add_index(index);
        }
        
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
    pub fn add_table_schema(&mut self, mut schema: TableSchema) {
        // Compute storage metadata automatically when adding to catalog
        let _ = Self::compute_table_metadata(&mut schema);
        self.schemas.insert(schema.name.clone(), Rc::new(schema));
    }

    /// Remove a table schema from the catalog
    pub fn remove_table_schema(&mut self, table_name: &str) -> Option<Rc<TableSchema>> {
        self.schemas.remove(table_name)
    }

    /// Add an index to a table
    pub fn add_index(&mut self, index: IndexInfo) -> Result<()> {
        let table_name = index.table_name.clone();
        if let Some(schema_rc) = self.schemas.get(&table_name) {
            let mut schema = schema_rc.as_ref().clone();
            schema.indexes.push(index);
            self.schemas.insert(table_name, Rc::new(schema));
            Ok(())
        } else {
            Err(crate::Error::Other(format!(
                "Table '{}' not found",
                table_name
            )))
        }
    }

    /// Remove an index from a table
    pub fn remove_index(&mut self, index_name: &str) -> Result<()> {
        let table_names: Vec<String> = self.schemas.keys().cloned().collect();
        for table_name in table_names {
            if let Some(schema_rc) = self.schemas.get(&table_name) {
                let mut schema = schema_rc.as_ref().clone();
                if let Some(index_pos) = schema.indexes.iter().position(|idx| idx.name == index_name) {
                    schema.indexes.remove(index_pos);
                    self.schemas.insert(table_name, Rc::new(schema));
                    return Ok(());
                }
            }
        }
        Err(crate::Error::Other(format!("Index '{}' not found", index_name)))
    }

    /// Get an index by name
    pub fn get_index(&self, index_name: &str) -> Option<&IndexInfo> {
        for schema in self.schemas.values() {
            if let Some(index) = schema.indexes.iter().find(|idx| idx.name == index_name) {
                return Some(index);
            }
        }
        None
    }

    /// Get all indexes for a table
    pub fn get_indexes_for_table(&self, table_name: &str) -> Vec<&IndexInfo> {
        if let Some(schema) = self.schemas.get(table_name) {
            schema.indexes.iter().collect()
        } else {
            Vec::new()
        }
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
        let mut schema = TableSchema {
            name: create_table.table.clone(),
            columns: create_table
                .columns
                .iter()
                .map(|col| ColumnInfo {
                    name: col.name.clone(),
                    data_type: col.data_type.clone(),
                    constraints: col.constraints.clone(),
                    storage_offset: 0,
                    storage_size: 0,
                    storage_type_code: 0,
                })
                .collect(),
            indexes: vec![], // Initialize indexes as empty
        };
        let _ = Self::compute_table_metadata(&mut schema);
        schema
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
                                                          // Compute storage metadata automatically when loading from storage
                    let _ = Self::compute_table_metadata(&mut schema);
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

    /// Get index storage key for an index
    pub fn get_index_storage_key(index_name: &str) -> String {
        format!("{INDEX_KEY_PREFIX}{index_name}")
    }

    /// Serialize an index to bytes for storage
    pub fn serialize_index_to_bytes(index: &IndexInfo) -> Vec<u8> {
        let mut index_data = Vec::new();
        index_data.extend_from_slice(index.table_name.as_bytes());
        index_data.push(b'|');
        index_data.extend_from_slice(index.column_name.as_bytes());
        index_data.push(b'|');
        if index.unique {
            index_data.extend_from_slice(b"UNIQUE");
        } else {
            index_data.extend_from_slice(b"NON_UNIQUE");
        }
        index_data
    }

    /// Deserialize an index from bytes
    pub fn deserialize_index_from_bytes(index_name: &str, data: &[u8]) -> Option<IndexInfo> {
        let data_str = String::from_utf8_lossy(data);
        let parts: Vec<&str> = data_str.split('|').collect();
        if parts.len() == 3 {
            let table_name = parts[0].to_string();
            let column_name = parts[1].to_string();
            let unique = parts[2] == "UNIQUE";
            Some(IndexInfo {
                name: index_name.to_string(),
                table_name,
                column_name,
                unique,
            })
        } else {
            None
        }
    }

    /// Load indexes from storage into the catalog
    pub fn load_indexes_from_storage(storage: &StorageEngine) -> Result<Vec<IndexInfo>> {
        let mut indexes = Vec::new();
        let index_prefix = INDEX_KEY_PREFIX.as_bytes().to_vec();
        let index_end = INDEX_KEY_END.as_bytes().to_vec();

        let index_entries = storage.scan(index_prefix..index_end)?;

        for (key, value_rc) in index_entries {
            let key_str = String::from_utf8_lossy(&key);
            if let Some(index_name) = key_str.strip_prefix(INDEX_KEY_PREFIX) {
                if let Some(index) = Self::deserialize_index_from_bytes(index_name, &value_rc) {
                    indexes.push(index);
                }
            }
        }

        Ok(indexes)
    }

    /// Compute table metadata and embed it in columns
    pub fn compute_table_metadata(schema: &mut TableSchema) -> crate::Result<()> {
        let mut current_offset = 0;
        for column in schema.columns.iter_mut() {
            let (size, type_code) = Self::get_column_size_and_type(&column.data_type)?;
            column.storage_offset = current_offset;
            column.storage_size = size;
            column.storage_type_code = type_code;
            current_offset += size;
        }
        Ok(())
    }

    pub fn get_column_size_and_type(
        data_type: &crate::parser::DataType,
    ) -> crate::Result<(usize, u8)> {
        use crate::storage_format::TypeCode;
        match data_type {
            crate::parser::DataType::Integer => Ok((8, TypeCode::Integer as u8)),
            crate::parser::DataType::Real => Ok((8, TypeCode::Real as u8)),
            crate::parser::DataType::Text(Some(len)) => Ok((*len, TypeCode::TextFixed as u8)),
            crate::parser::DataType::Text(None) => Err(crate::Error::Other(
                "Variable-length TEXT not supported in fixed-length format".to_string(),
            )),
            crate::parser::DataType::Vector(Some(dimension)) => {
                let size = dimension * 8; // Each f64 is 8 bytes
                Ok((size, TypeCode::Vector as u8))
            }
            crate::parser::DataType::Vector(None) => Err(crate::Error::Other(
                "Variable-length VECTOR not supported in fixed-length format".to_string(),
            )),
        }
    }
}

impl Default for Catalog {
    fn default() -> Self {
        Self::new()
    }
}

/// Helper to serialize SqlValue for index key
fn sql_value_to_index_string(val: &crate::parser::SqlValue) -> String {
    match val {
        crate::parser::SqlValue::Integer(i) => i.to_string(),
        crate::parser::SqlValue::Real(f) => f.to_string(),
        crate::parser::SqlValue::Text(s) => s.clone(),
        crate::parser::SqlValue::Vector(v) => v.iter().map(|f| f.to_string()).collect::<Vec<_>>().join(","),
        crate::parser::SqlValue::Null => "NULL".to_string(),
        crate::parser::SqlValue::Parameter(i) => format!("?{}", i),
    }
}

/// Encode an index entry key
/// Format: I:{table}:{index}:{column_value}:{pk}
pub fn encode_index_key(table: &str, index: &str, column_value: &crate::parser::SqlValue, pk: &crate::parser::SqlValue) -> Vec<u8> {
    let mut key = Vec::new();
    key.extend_from_slice(INDEX_KEY_PREFIX.as_bytes());
    key.extend_from_slice(table.as_bytes());
    key.push(b':');
    key.extend_from_slice(index.as_bytes());
    key.push(b':');
    key.extend_from_slice(sql_value_to_index_string(column_value).as_bytes());
    key.push(b':');
    key.extend_from_slice(sql_value_to_index_string(pk).as_bytes());
    key
}

/// Decode an index entry key
/// Returns (table, index, column_value, pk) if successful
pub fn decode_index_key(key: &[u8]) -> Option<(String, String, String, String)> {
    let s = String::from_utf8_lossy(key);
    if !s.starts_with(INDEX_KEY_PREFIX) {
        return None;
    }
    let s = &s[INDEX_KEY_PREFIX.len()..];
    let parts: Vec<&str> = s.splitn(4, ':').collect();
    if parts.len() == 4 {
        Some((parts[0].to_string(), parts[1].to_string(), parts[2].to_string(), parts[3].to_string()))
    } else {
        None
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::parser::{ColumnConstraint, DataType, SqlValue};
    use crate::query_processor::{ColumnInfo, TableSchema};

    #[test]
    fn test_catalog_basic_operations() {
        let mut catalog = Catalog::new();
        assert_eq!(catalog.table_count(), 0);
        assert!(!catalog.table_exists("users"));

        // Create a test schema
        let mut users_schema = TableSchema {
            name: "users".to_string(),
            columns: vec![
                ColumnInfo {
                    name: "id".to_string(),
                    data_type: DataType::Integer,
                    constraints: vec![ColumnConstraint::PrimaryKey],
                    storage_offset: 0,
                    storage_size: 0,
                    storage_type_code: 0,
                },
                ColumnInfo {
                    name: "name".to_string(),
                    data_type: DataType::Text(None),
                    constraints: vec![],
                    storage_offset: 0,
                    storage_size: 0,
                    storage_type_code: 0,
                },
            ],
            indexes: vec![], // No indexes in CREATE TABLE
        };
        let _ = Catalog::compute_table_metadata(&mut users_schema);

        catalog.add_table_schema(users_schema);
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

    #[test]
    fn test_index_key_codec_roundtrip() {
        let table = "users";
        let index = "idx_name";
        let col_val = SqlValue::Text("alice".to_string());
        let pk = SqlValue::Integer(42);
        let key = encode_index_key(table, index, &col_val, &pk);
        let decoded = decode_index_key(&key).unwrap();
        assert_eq!(decoded.0, table);
        assert_eq!(decoded.1, index);
        assert_eq!(decoded.2, sql_value_to_index_string(&col_val));
        assert_eq!(decoded.3, sql_value_to_index_string(&pk));
    }
}
