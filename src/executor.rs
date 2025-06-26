//! SQL executor that bridges parsed SQL statements with TegDB engine operations
//! 
//! This module provides a SQL executor that can take parsed SQL statements
//! and execute them against a TegDB engine instance using transactions for ACID compliance.

use crate::parser::{
    SelectStatement, InsertStatement, UpdateStatement, 
    DeleteStatement, CreateTableStatement, DropTableStatement, SqlValue, Condition, 
    ComparisonOperator
};
use crate::Result;
use std::collections::HashMap;

/// A SQL executor that can execute parsed SQL statements against a TegDB engine
/// 
/// This executor receives pre-cached table schemas from the Database level
/// to avoid repeated schema loading from disk. The schemas are kept in sync
/// at the database level when DDL operations are performed.
pub struct Executor<'a> {
    transaction: crate::engine::Transaction<'a>,
    /// Table schemas provided by the database-level cache
    table_schemas: HashMap<String, TableSchema>,
    /// Track if we're in an explicit transaction
    in_transaction: bool,
}

/// Simple table schema representation
#[derive(Debug, Clone)]
pub struct TableSchema {
    pub columns: Vec<ColumnInfo>,
}

#[derive(Debug, Clone)]
pub struct ColumnInfo {
    pub name: String,
    pub data_type: crate::parser::DataType,
    pub constraints: Vec<crate::parser::ColumnConstraint>,
}

/// Result of executing a SQL statement
#[derive(Debug, Clone)]
pub enum ResultSet {
    /// Result of a SELECT query
    Select { 
        columns: Vec<String>, 
        rows: Vec<Vec<SqlValue>> 
    },
    /// Result of an INSERT operation
    Insert { 
        rows_affected: usize 
    },
    /// Result of an UPDATE operation
    Update { 
        rows_affected: usize 
    },
    /// Result of a DELETE operation
    Delete { 
        rows_affected: usize 
    },
    /// Result of a CREATE TABLE operation
    CreateTable { 
        table_name: String 
    },
    /// Result of a DROP TABLE operation
    DropTable { 
        table_name: String,
        existed: bool, // Indicates if the table existed before dropping
    },
    /// Result of a BEGIN operation
    Begin,
    /// Result of a COMMIT operation
    Commit,
    /// Result of a ROLLBACK operation
    Rollback,
}

impl<'a> Executor<'a> {
    /// Create a new SQL executor with pre-loaded schemas
    /// Schemas should be provided from a database-level cache for efficiency
    pub fn new_with_schemas(
        transaction: crate::engine::Transaction<'a>,
        table_schemas: HashMap<String, TableSchema>
    ) -> Self {
        Self {
            transaction,
            table_schemas,
            in_transaction: false,
        }
    }

    /// Create a new SQL executor and load schemas from the transaction
    /// Note: This method loads schemas from disk on every call.
    /// For better performance, use Database::open() which caches schemas.
    pub fn new(transaction: crate::engine::Transaction<'a>) -> Self {
        let table_schemas = Self::load_schemas_from_transaction(&transaction);
        Self {
            transaction,
            table_schemas,
            in_transaction: false,
        }
    }

    /// Start a transaction (replaces execute with Statement::Begin)
    pub fn begin_transaction(&mut self) -> Result<ResultSet> {
        if self.in_transaction {
            return Err(crate::Error::Other("Already in a transaction".to_string()));
        }
        
        self.in_transaction = true;
        
        Ok(ResultSet::Begin)
    }

    /// Commit the current transaction (replaces execute with Statement::Commit)
    pub fn commit_transaction(&mut self) -> Result<ResultSet> {
        if !self.in_transaction {
            return Err(crate::Error::Other("No active transaction to commit".to_string()));
        }
        
        // Note: The actual commit will happen when the transaction is dropped/committed externally
        self.in_transaction = false;
        
        Ok(ResultSet::Commit)
    }

    /// Rollback the current transaction (replaces execute with Statement::Rollback)
    pub fn rollback_transaction(&mut self) -> Result<ResultSet> {
        if !self.in_transaction {
            return Err(crate::Error::Other("No active transaction to rollback".to_string()));
        }
        
        // Note: The actual rollback will happen when the transaction is dropped/rolled back externally
        self.in_transaction = false;
        
        Ok(ResultSet::Rollback)
    }

    /// Execute a SELECT statement with query optimization
    pub fn execute_select(&mut self, select: SelectStatement) -> Result<ResultSet> {
        // Try to optimize the query using the planner
        if let Some(ref where_clause) = select.where_clause {
            if let Some(optimized_result) = self.try_optimize_select(&select, &where_clause.condition)? {
                return Ok(optimized_result);
            }
        }

        // Fall back to full table scan if optimization isn't possible
        self.execute_select_scan(&select)
    }

    /// Try to optimize a SELECT query using direct primary key lookup
    fn try_optimize_select(&mut self, select: &SelectStatement, condition: &Condition) -> Result<Option<ResultSet>> {
        // Try to extract primary key equality conditions
        if let Some(pk_values) = self.extract_pk_equality_conditions(&select.table, condition)? {
            // We can use direct PK lookup!
            if let Some(row_data) = self.get_row_by_primary_key(&select.table, &pk_values)? {
                // We found exactly one row, now apply any additional filtering and column selection
                let matching_rows = if let Some(ref where_clause) = select.where_clause {
                    if self.evaluate_condition(&where_clause.condition, &row_data) {
                        vec![row_data]
                    } else {
                        vec![]
                    }
                } else {
                    vec![row_data]
                };

                // Apply column selection
                let result_columns = if select.columns.len() == 1 && select.columns[0] == "*" {
                    if let Some(first_row) = matching_rows.first() {
                        let mut cols: Vec<_> = first_row.keys().cloned().collect();
                        cols.sort(); // Ensure consistent column ordering
                        cols
                    } else {
                        vec![]
                    }
                } else {
                    select.columns.clone()
                };

                // Extract selected columns from matching rows
                let mut result_rows = Vec::with_capacity(matching_rows.len());
                for row in matching_rows {
                    let mut result_row = Vec::with_capacity(result_columns.len());
                    for col in &result_columns {
                        result_row.push(row.get(col).cloned().unwrap_or(SqlValue::Null));
                    }
                    result_rows.push(result_row);
                }

                return Ok(Some(ResultSet::Select {
                    columns: result_columns,
                    rows: result_rows,
                }));
            } else {
                // PK not found, return empty result
                let result_columns = if select.columns.len() == 1 && select.columns[0] == "*" {
                    // Get column names from schema for empty result
                    if let Some(schema) = self.table_schemas.get(&select.table) {
                        let mut cols: Vec<_> = schema.columns.iter().map(|c| c.name.clone()).collect();
                        cols.sort();
                        cols
                    } else {
                        vec![]
                    }
                } else {
                    select.columns.clone()
                };

                return Ok(Some(ResultSet::Select {
                    columns: result_columns,
                    rows: vec![],
                }));
            }
        }

        Ok(None) // Cannot optimize, fall back to scan
    }

    /// Extract primary key equality conditions from a WHERE clause for optimization
    fn extract_pk_equality_conditions(&self, table_name: &str, condition: &Condition) -> Result<Option<HashMap<String, SqlValue>>> {
        let pk_columns = self.get_primary_key_columns(table_name)?;
        let mut pk_values = HashMap::new();

        // Try to extract all primary key values from equality conditions
        self.collect_pk_equality_values(condition, &pk_columns, &mut pk_values);

        // Check if we have values for ALL primary key columns
        if pk_values.len() == pk_columns.len() {
            Ok(Some(pk_values))
        } else {
            Ok(None)
        }
    }

    /// Recursively collect primary key equality values from conditions
    fn collect_pk_equality_values(&self, condition: &Condition, pk_columns: &[String], pk_values: &mut HashMap<String, SqlValue>) {
        match condition {
            Condition::Comparison { left, operator, right } => {
                if let ComparisonOperator::Equal = operator {
                    if pk_columns.contains(left) && !pk_values.contains_key(left) {
                        pk_values.insert(left.clone(), right.clone());
                    }
                }
            }
            Condition::And(left_cond, right_cond) => {
                // For AND conditions, collect from both sides
                self.collect_pk_equality_values(left_cond, pk_columns, pk_values);
                self.collect_pk_equality_values(right_cond, pk_columns, pk_values);
            }
            Condition::Or(_, _) => {
                // For OR conditions, we cannot optimize with PK lookup since we'd need multiple lookups
                // Leave pk_values unchanged (empty or partial)
            }
        }
    }

    /// Execute a SELECT statement using full table scan (fallback method)
    fn execute_select_scan(&mut self, select: &SelectStatement) -> Result<ResultSet> {
        let table_key_prefix = format!("{}:", select.table);
        let mut matching_rows: Vec<HashMap<String, SqlValue>> = Vec::new();
        
        let start_key = table_key_prefix.as_bytes().to_vec();
        let end_key = format!("{}~", select.table).as_bytes().to_vec();
        
        let scan_results = self.transaction.scan(start_key..end_key)?;
        
        // Process with early termination for LIMIT to save memory
        let mut processed_count = 0;
        let limit = select.limit.unwrap_or(u64::MAX) as usize;
        
        for (key, value) in scan_results {
            if processed_count >= limit {
                break; // Early termination for LIMIT
            }
            
            if let Ok(row_data) = self.deserialize_row(&select.table, &key, &value) {
                // Apply WHERE clause if present (predicate pushdown)
                let matches = if let Some(ref where_clause) = select.where_clause {
                    self.evaluate_condition(&where_clause.condition, &row_data)
                } else {
                    true
                };
                
                if matches {
                    matching_rows.push(row_data);
                    processed_count += 1;
                }
            }
        }

        // Apply column selection
        let result_columns = if select.columns.len() == 1 && select.columns[0] == "*" {
            if let Some(first_row) = matching_rows.first() {
                let mut cols: Vec<_> = first_row.keys().cloned().collect();
                cols.sort(); // Ensure consistent column ordering
                cols
            } else {
                vec![]
            }
        } else {
            select.columns.clone()
        };

        // Extract selected columns from matching rows with memory efficiency
        let mut result_rows = Vec::with_capacity(matching_rows.len());
        for row in matching_rows {
            let mut result_row = Vec::with_capacity(result_columns.len());
            for col in &result_columns {
                result_row.push(row.get(col).cloned().unwrap_or(SqlValue::Null));
            }
            result_rows.push(result_row);
        }

        Ok(ResultSet::Select {
            columns: result_columns,
            rows: result_rows,
        })
    }

    /// Execute an INSERT statement within a transaction
    pub fn execute_insert(&mut self, insert: InsertStatement) -> Result<ResultSet> {
        let mut rows_affected = 0;

        // Prepare and apply each row operation directly to the transaction
        for values in insert.values.iter() {
            // Create row data map
            let mut row_data = HashMap::new();
            for (col_idx, value) in values.iter().enumerate() {
                if let Some(column_name) = insert.columns.get(col_idx) {
                    row_data.insert(column_name.clone(), value.clone());
                }
            }

            // Validate row data against schema
            self.validate_row_data(&insert.table, &row_data)?;

            // Generate row key based on primary key values (IOT approach)
            let row_key = self.generate_row_key(&insert.table, &row_data)?;
            
            // Check for primary key constraint violation
            if self.primary_key_exists(&insert.table, &row_data)? {
                return Err(crate::Error::Other(format!(
                    "Primary key constraint violation: duplicate key in table '{}'", 
                    insert.table
                )));
            }

            // Check UNIQUE constraints
            self.validate_unique_constraints(&insert.table, &row_data, None)?;

            // Serialize the row and store with primary key as the actual storage key
            let serialized_row = self.serialize_row(&insert.table, &row_data)?;
            self.transaction.set(row_key.as_bytes(), serialized_row)?;
            rows_affected += 1;
        }

        Ok(ResultSet::Insert { rows_affected })
    }

    /// Execute an UPDATE statement within a transaction
    pub fn execute_update(&mut self, update: UpdateStatement) -> Result<ResultSet> {
        let mut rows_affected = 0;
        let table_key_prefix = format!("{}:", update.table);
        
        // Get current state using transaction's scan method (includes pending operations)
        let start_key = table_key_prefix.as_bytes().to_vec();
        let end_key = format!("{}~", update.table).as_bytes().to_vec();
        
        let current_data = self.transaction.scan(start_key..end_key)?;
        let current_data: Vec<_> = current_data.collect(); // Collect to avoid borrow conflicts
        
        // Process each row
        for (key, value) in current_data {
            if let Ok(row_data) = self.deserialize_row(&update.table, &key, &value) {
                // Check if row matches WHERE clause
                let matches = if let Some(ref where_clause) = update.where_clause {
                    self.evaluate_condition(&where_clause.condition, &row_data)
                } else {
                    true
                };
                
                if matches {
                    // Apply updates to a copy first for validation
                    let mut updated_row = row_data.clone();
                    for assignment in &update.assignments {
                        updated_row.insert(assignment.column.clone(), assignment.value.clone());
                    }
                    
                    // Validate updated row data
                    self.validate_row_data(&update.table, &updated_row)?;
                    
                    // Check UNIQUE constraints (excluding current row)
                    self.validate_unique_constraints(&update.table, &updated_row, Some(&key))?;
                    
                    // Serialize updated row and apply directly to transaction
                    let serialized_row = self.serialize_row(&update.table, &updated_row)?;
                    self.transaction.set(&key, serialized_row)?;
                    rows_affected += 1;
                }
            }
        }

        Ok(ResultSet::Update { rows_affected })
    }

    /// Execute a DELETE statement within a transaction
    pub fn execute_delete(&mut self, delete: DeleteStatement) -> Result<ResultSet> {
        let mut rows_affected = 0;
        let table_key_prefix = format!("{}:", delete.table);
        
        // Get current state using transaction's scan method (includes pending operations)
        let start_key = table_key_prefix.as_bytes().to_vec();
        let end_key = format!("{}~", delete.table).as_bytes().to_vec();
        
        let current_data = self.transaction.scan(start_key..end_key)?;
        let current_data: Vec<_> = current_data.collect(); // Collect to avoid borrow conflicts
        
        // Find rows to delete
        for (key, value) in current_data {
            if let Ok(row_data) = self.deserialize_row(&delete.table, &key, &value) {
                // Check if row matches WHERE clause
                let should_delete = if let Some(ref where_clause) = delete.where_clause {
                    self.evaluate_condition(&where_clause.condition, &row_data)
                } else {
                    true // DELETE without WHERE deletes all rows
                };
                
                if should_delete {
                    // Apply deletion directly to transaction
                    self.transaction.delete(&key)?;
                    rows_affected += 1;
                }
            }
        }

        Ok(ResultSet::Delete { rows_affected })
    }

    /// Execute a CREATE TABLE statement within a transaction
    pub fn execute_create_table(&mut self, create: CreateTableStatement) -> Result<ResultSet> {
        // Validate that the table has at least one primary key column
        let has_primary_key = create.columns
            .iter()
            .any(|col| col.constraints.contains(&crate::parser::ColumnConstraint::PrimaryKey));
        
        if !has_primary_key {
            return Err(crate::Error::Other(format!(
                "Table '{}' must have at least one PRIMARY KEY column for IOT implementation", 
                create.table
            )));
        }

        // Store table schema metadata
        let schema = TableSchema {
            columns: create.columns.iter().map(|col| ColumnInfo {
                name: col.name.clone(),
                data_type: col.data_type.clone(),
                constraints: col.constraints.clone(),
            }).collect(),
        };
        
        // Store schema in memory (in a real implementation, this would be persisted)
        self.table_schemas.insert(create.table.clone(), schema);
        
        // Store the schema in the database using the transaction
        let schema_key = format!("__schema__:{}", create.table);
        let serialized_schema = self.serialize_schema(&create)?;
        self.transaction.set(schema_key.as_bytes(), serialized_schema)?;

        Ok(ResultSet::CreateTable { 
            table_name: create.table 
        })
    }

    /// Execute a DROP TABLE statement within a transaction
    pub fn execute_drop_table(&mut self, drop: DropTableStatement) -> Result<ResultSet> {
        let schema_key = format!("__schema__:{}", drop.table);
        
        // Check if table exists by looking for its schema
        let table_exists = self.transaction.get(schema_key.as_bytes()).is_some();

        // Handle IF EXISTS logic
        if !table_exists {
            if drop.if_exists {
                // Table doesn't exist but IF EXISTS was specified, so this is not an error
                return Ok(ResultSet::DropTable { 
                    table_name: drop.table,
                    existed: false 
                });
            } else {
                // Table doesn't exist and IF EXISTS was not specified, so this is an error
                return Err(crate::Error::Other(format!(
                    "Table '{}' does not exist", 
                    drop.table
                )));
            }
        }

        // Remove the table schema from storage
        self.transaction.delete(schema_key.as_bytes())?;
        
        // Remove the table schema from memory
        self.table_schemas.remove(&drop.table);

        // Remove all data rows for this table
        // We need to scan for all keys that start with the table name prefix
        let table_prefix = format!("{}:", drop.table);
        let mut keys_to_delete = Vec::new();
        
        // Since we don't have a scan_prefix method, we'll use a range scan
        // from the table prefix to the next possible prefix
        let start_key = table_prefix.as_bytes().to_vec();
        let mut end_key = start_key.clone();
        // Increment the last byte to create an exclusive upper bound
        if let Some(last_byte) = end_key.last_mut() {
            if *last_byte < 255 {
                *last_byte += 1;
            } else {
                end_key.push(0);
            }
        } else {
            end_key.push(0);
        }

        // Scan for all keys with the table prefix
        let scan_results = self.transaction.scan(start_key..end_key)?;
        for (key, _) in scan_results {
            keys_to_delete.push(key);
        }

        // Delete all found keys
        for key in keys_to_delete {
            self.transaction.delete(&key)?;
        }

        Ok(ResultSet::DropTable { 
            table_name: drop.table,
            existed: true 
        })
    }

    /// Evaluate a condition against a row of data
    fn evaluate_condition(&self, condition: &Condition, row_data: &HashMap<String, SqlValue>) -> bool {
        match condition {
            Condition::Comparison { left, operator, right } => {
                if let Some(left_value) = row_data.get(left) {
                    self.compare_values(left_value, operator, right)
                } else {
                    false
                }
            }
            Condition::And(left, right) => {
                self.evaluate_condition(left, row_data) && self.evaluate_condition(right, row_data)
            }
            Condition::Or(left, right) => {
                self.evaluate_condition(left, row_data) || self.evaluate_condition(right, row_data)
            }
        }
    }

    /// Compare two SQL values using the given operator
    fn compare_values(&self, left: &SqlValue, operator: &ComparisonOperator, right: &SqlValue) -> bool {
        use ComparisonOperator::*;
        
        match (left, right) {
            (SqlValue::Integer(l), SqlValue::Integer(r)) => match operator {
                Equal => l == r,
                NotEqual => l != r,
                LessThan => l < r,
                LessThanOrEqual => l <= r,
                GreaterThan => l > r,
                GreaterThanOrEqual => l >= r,
                Like => false, // LIKE doesn't apply to integers
            },
            (SqlValue::Real(l), SqlValue::Real(r)) => match operator {
                Equal => (l - r).abs() < f64::EPSILON,
                NotEqual => (l - r).abs() >= f64::EPSILON,
                LessThan => l < r,
                LessThanOrEqual => l <= r,
                GreaterThan => l > r,
                GreaterThanOrEqual => l >= r,
                Like => false, // LIKE doesn't apply to reals
            },
            (SqlValue::Text(l), SqlValue::Text(r)) => match operator {
                Equal => l == r,
                NotEqual => l != r,
                LessThan => l < r,
                LessThanOrEqual => l <= r,
                GreaterThan => l > r,
                GreaterThanOrEqual => l >= r,
                Like => l.contains(r), // Simplified LIKE implementation
            },
            (SqlValue::Null, SqlValue::Null) => match operator {
                Equal => true,
                NotEqual => false,
                _ => false,
            },
            _ => false, // Type mismatch or comparison with NULL
        }
    }

    /// Serialize a row to bytes using efficient binary format (IOT optimized)
    /// Only stores non-primary key columns in the value, since primary keys are in the storage key
    fn serialize_row(&self, table_name: &str, row_data: &HashMap<String, SqlValue>) -> Result<Vec<u8>> {
        // Get primary key columns to exclude from serialization
        let pk_columns = self.get_primary_key_columns(table_name)?;
        
        // Create a new map excluding primary key columns (IOT optimization)
        let mut non_pk_data = HashMap::new();
        for (col_name, value) in row_data {
            if !pk_columns.contains(col_name) {
                non_pk_data.insert(col_name.clone(), value.clone());
            }
        }
        
        Ok(crate::serialization::BinaryRowSerializer::serialize(&non_pk_data))
    }

    /// Deserialize a row from bytes and reconstruct full row with primary key values
    /// Combines stored non-PK columns with PK values extracted from the storage key
    fn deserialize_row(&self, table_name: &str, key: &[u8], data: &[u8]) -> Result<HashMap<String, SqlValue>> {
        // Deserialize non-primary key columns from stored data
        let mut row_data = crate::serialization::BinaryRowSerializer::deserialize(data)?;
        
        // Extract and add primary key values from the storage key
        let key_str = std::str::from_utf8(key)
            .map_err(|e| crate::Error::Other(format!("Invalid key encoding: {}", e)))?;
        
        // Parse key format: "table_name:pk_value1:pk_value2:..."
        if let Some(key_suffix) = key_str.strip_prefix(&format!("{}:", table_name)) {
            let pk_columns = self.get_primary_key_columns(table_name)?;
            let pk_values_str: Vec<&str> = key_suffix.split(':').collect();
            
            if pk_values_str.len() != pk_columns.len() {
                return Err(crate::Error::Other(format!(
                    "Key format mismatch: expected {} PK values, got {}", 
                    pk_columns.len(), pk_values_str.len()
                )));
            }
            
            // Get schema to determine data types for primary key columns
            let schema = self.table_schemas.get(table_name)
                .ok_or_else(|| crate::Error::Other(format!("Table '{}' not found", table_name)))?;
            
            // Reconstruct primary key values with correct types
            for (pk_col, pk_value_str) in pk_columns.iter().zip(pk_values_str.iter()) {
                // Find the column info to get the data type
                let col_info = schema.columns.iter()
                    .find(|col| &col.name == pk_col)
                    .ok_or_else(|| crate::Error::Other(format!("Primary key column '{}' not found in schema", pk_col)))?;
                
                // Parse the value according to its data type
                let parsed_value = match col_info.data_type {
                    crate::parser::DataType::Integer => {
                        // Remove zero padding and parse
                        let cleaned = pk_value_str.trim_start_matches('0');
                        let cleaned = if cleaned.is_empty() { "0" } else { cleaned };
                        SqlValue::Integer(cleaned.parse::<i64>()
                            .map_err(|e| crate::Error::Other(format!("Failed to parse integer PK value '{}': {}", pk_value_str, e)))?)
                    },
                    crate::parser::DataType::Text => {
                        SqlValue::Text(pk_value_str.to_string())
                    },
                    crate::parser::DataType::Real => {
                        SqlValue::Real(pk_value_str.parse::<f64>()
                            .map_err(|e| crate::Error::Other(format!("Failed to parse real PK value '{}': {}", pk_value_str, e)))?)
                    },
                    crate::parser::DataType::Blob => {
                        // For now, treat BLOB as text in primary keys
                        SqlValue::Text(pk_value_str.to_string())
                    },
                };
                
                row_data.insert(pk_col.clone(), parsed_value);
            }
        } else {
            return Err(crate::Error::Other(format!("Invalid key format for table '{}'", table_name)));
        }
        
        Ok(row_data)
    }

    /// Serialize table schema for storage
    fn serialize_schema(&self, create: &CreateTableStatement) -> Result<Vec<u8>> {
        // Simple schema serialization
        let serialized = create.columns
            .iter()
            .map(|col| {
                let data_type = match col.data_type {
                    crate::parser::DataType::Integer => "INTEGER",
                    crate::parser::DataType::Text => "TEXT",
                    crate::parser::DataType::Real => "REAL",
                    crate::parser::DataType::Blob => "BLOB",
                };
                
                let constraints = col.constraints
                    .iter()
                    .map(|c| match c {
                        crate::parser::ColumnConstraint::PrimaryKey => "PRIMARY_KEY",
                        crate::parser::ColumnConstraint::NotNull => "NOT_NULL",
                        crate::parser::ColumnConstraint::Unique => "UNIQUE",
                    })
                    .collect::<Vec<_>>()
                    .join(",");

                format!("{}:{}:{}", col.name, data_type, constraints)
            })
            .collect::<Vec<_>>()
            .join("|");

        Ok(serialized.into_bytes())
    }

    /// Load all table schemas from the transaction by scanning for schema keys
    /// Note: This method is provided for backward compatibility with tests.
    /// The Database struct provides better schema caching.
    fn load_schemas_from_transaction(transaction: &crate::engine::Transaction<'a>) -> HashMap<String, TableSchema> {
        let mut schemas = HashMap::new();
        
        // Scan for schema keys (format: __schema__:{table_name})
        let schema_range = b"__schema__:".to_vec()..b"__schema__~".to_vec();
        let schema_entries = match transaction.scan(schema_range) {
            Ok(entries) => entries,
            Err(_) => return schemas, // Return empty schema map if scan fails
        };
        
        for (key, value) in schema_entries {
            if let Ok(key_str) = std::str::from_utf8(&key) {
                if let Some(table_name) = key_str.strip_prefix("__schema__:") {
                    if let Ok(schema) = Self::deserialize_schema(&value) {
                        schemas.insert(table_name.to_string(), schema);
                    }
                }
            }
        }
        
        schemas
    }

    /// Deserialize a schema from bytes with proper error handling
    /// Note: This method duplicates logic from Database for backward compatibility
    fn deserialize_schema(data: &[u8]) -> Result<TableSchema> {
        let serialized = std::str::from_utf8(data)
            .map_err(|e| crate::Error::Other(format!("Invalid schema data encoding: {}", e)))?;
        
        if serialized.is_empty() {
            return Err(crate::Error::Other("Empty schema data".to_string()));
        }
        
        let mut columns = Vec::new();
        
        for (idx, column_data) in serialized.split('|').enumerate() {
            let parts: Vec<&str> = column_data.split(':').collect();
            if parts.len() != 3 {
                return Err(crate::Error::Other(format!(
                    "Invalid schema format at column {}: expected 3 parts separated by ':', got {}", 
                    idx, parts.len()
                )));
            }
            
            let name = parts[0].trim().to_string();
            if name.is_empty() {
                return Err(crate::Error::Other(format!(
                    "Empty column name at column {}", idx
                )));
            }
            
            let data_type = match parts[1].trim() {
                "INTEGER" => crate::parser::DataType::Integer,
                "TEXT" => crate::parser::DataType::Text,
                "REAL" => crate::parser::DataType::Real,
                "BLOB" => crate::parser::DataType::Blob,
                unknown => return Err(crate::Error::Other(format!(
                    "Unknown data type '{}' for column '{}'", unknown, name
                ))),
            };
            
            let constraints = if parts[2].trim().is_empty() {
                Vec::new()
            } else {
                let mut parsed_constraints = Vec::new();
                for constraint in parts[2].split(',') {
                    match constraint.trim() {
                        "PRIMARY_KEY" => parsed_constraints.push(crate::parser::ColumnConstraint::PrimaryKey),
                        "NOT_NULL" => parsed_constraints.push(crate::parser::ColumnConstraint::NotNull),
                        "UNIQUE" => parsed_constraints.push(crate::parser::ColumnConstraint::Unique),
                        unknown if !unknown.is_empty() => return Err(crate::Error::Other(format!(
                            "Unknown constraint '{}' for column '{}'", unknown, name
                        ))),
                        _ => {} // Skip empty constraints
                    }
                }
                parsed_constraints
            };
            
            columns.push(ColumnInfo {
                name,
                data_type,
                constraints,
            });
        }
        
        if columns.is_empty() {
            return Err(crate::Error::Other("Schema must have at least one column".to_string()));
        }
        
        Ok(TableSchema { columns })
    }

    /// Get the underlying transaction reference
    #[allow(dead_code)]
    pub fn transaction(&self) -> &crate::engine::Transaction<'a> {
        &self.transaction
    }

    /// Get a mutable reference to the underlying transaction
    pub fn transaction_mut(&mut self) -> &mut crate::engine::Transaction<'a> {
        &mut self.transaction
    }

    /// Get the primary key column(s) for a table
    fn get_primary_key_columns(&self, table_name: &str) -> Result<Vec<String>> {
        if let Some(schema) = self.table_schemas.get(table_name) {
            let pk_columns: Vec<String> = schema.columns
                .iter()
                .filter(|col| col.constraints.contains(&crate::parser::ColumnConstraint::PrimaryKey))
                .map(|col| col.name.clone())
                .collect();
            
            if pk_columns.is_empty() {
                Err(crate::Error::Other(format!(
                    "Table '{}' must have a primary key column", table_name
                )))
            } else {
                Ok(pk_columns)
            }
        } else {
            Err(crate::Error::Other(format!("Table '{}' not found", table_name)))
        }
    }

    /// Generate a row key based on primary key values (IOT approach)
    fn generate_row_key(&self, table_name: &str, row_data: &HashMap<String, SqlValue>) -> Result<String> {
        let pk_columns = self.get_primary_key_columns(table_name)?;
        
        let pk_values: Result<Vec<String>> = pk_columns
            .iter()
            .map(|col| {
                match row_data.get(col) {
                    Some(SqlValue::Integer(i)) => Ok(format!("{:020}", i)), // Zero-padded for sorting
                    Some(SqlValue::Text(s)) => Ok(s.clone()),
                    Some(SqlValue::Real(r)) => Ok(format!("{:020.10}", r)), // Fixed precision for sorting
                    Some(SqlValue::Null) => Err(crate::Error::Other(format!(
                        "Primary key column '{}' cannot be NULL", col
                    ))),
                    None => Err(crate::Error::Other(format!(
                        "Primary key column '{}' is required", col
                    ))),
                }
            })
            .collect();

        let pk_values = pk_values?;
        
        // Create clustered key: table:pk_value1:pk_value2:...
        Ok(format!("{}:{}", table_name, pk_values.join(":")))
    }

    /// Check if a primary key already exists (for duplicate prevention)
    fn primary_key_exists(&mut self, table_name: &str, row_data: &HashMap<String, SqlValue>) -> Result<bool> {
        let row_key = self.generate_row_key(table_name, row_data)?;
        
        // Check if key exists in the transaction state
        let key_bytes = row_key.as_bytes().to_vec();
        Ok(self.transaction.get(&key_bytes).is_some())
    }

    /// Direct lookup by primary key (efficient IOT access)
    fn get_row_by_primary_key(&mut self, table_name: &str, pk_values: &HashMap<String, SqlValue>) -> Result<Option<HashMap<String, SqlValue>>> {
        let row_key = self.generate_row_key(table_name, pk_values)?;
        
        if let Some(value) = self.transaction.get(row_key.as_bytes()) {
            Ok(Some(self.deserialize_row(table_name, row_key.as_bytes(), &value)?))
        } else {
            Ok(None)
        }
    }

    /// Extract primary key values from a row for efficient operations
    #[allow(dead_code)]
    fn extract_primary_key_values(&self, table_name: &str, row_data: &HashMap<String, SqlValue>) -> Result<HashMap<String, SqlValue>> {
        let pk_columns = self.get_primary_key_columns(table_name)?;
        let mut pk_values = HashMap::new();
        
        for pk_col in pk_columns {
            if let Some(value) = row_data.get(&pk_col) {
                pk_values.insert(pk_col, value.clone());
            } else {
                return Err(crate::Error::Other(format!(
                    "Primary key column '{}' missing from row data", pk_col
                )));
            }
        }
        
        Ok(pk_values)
    }
    
    /// Validate data against table schema constraints
    fn validate_row_data(&self, table_name: &str, row_data: &HashMap<String, SqlValue>) -> Result<()> {
        let schema = self.table_schemas.get(table_name)
            .ok_or_else(|| crate::Error::Other(format!("Table '{}' not found", table_name)))?;
        
        // Check for required columns and data type compatibility
        for column in &schema.columns {
            if let Some(value) = row_data.get(&column.name) {
                // Validate data type compatibility
                self.validate_data_type(value, &column.data_type, &column.name)?;
                
                // Check NOT NULL constraint
                if column.constraints.contains(&crate::parser::ColumnConstraint::NotNull) && *value == SqlValue::Null {
                    return Err(crate::Error::Other(format!(
                        "Column '{}' cannot be NULL", column.name
                    )));
                }
            } else {
                // Column is missing - check if it's required
                if column.constraints.contains(&crate::parser::ColumnConstraint::NotNull) ||
                   column.constraints.contains(&crate::parser::ColumnConstraint::PrimaryKey) {
                    return Err(crate::Error::Other(format!(
                        "Required column '{}' is missing", column.name
                    )));
                }
            }
        }
        
        // Check for unknown columns
        for column_name in row_data.keys() {
            if !schema.columns.iter().any(|col| col.name == *column_name) {
                return Err(crate::Error::Other(format!(
                    "Unknown column '{}' for table '{}'", column_name, table_name
                )));
            }
        }
        
        Ok(())
    }
    
    /// Validate that a value matches the expected data type
    fn validate_data_type(&self, value: &SqlValue, expected_type: &crate::parser::DataType, column_name: &str) -> Result<()> {
        use crate::parser::DataType;
        use SqlValue::*;
        
        let is_valid = match (value, expected_type) {
            (Null, _) => true, // NULL is valid for any type (NOT NULL constraint checked separately)
            (Integer(_), DataType::Integer) => true,
            (Text(_), DataType::Text) => true,
            (Real(_), DataType::Real) => true,
            // Allow implicit conversions
            (Integer(_), DataType::Real) => true, // Integer can be converted to Real
            (Integer(_), DataType::Text) => true, // Integer can be converted to Text
            (Real(_), DataType::Text) => true, // Real can be converted to Text
            // For BLOB, we'll accept any type since SqlValue doesn't have Blob variant yet
            (_, DataType::Blob) => true,
            _ => false,
        };
        
        if !is_valid {
            return Err(crate::Error::Other(format!(
                "Type mismatch for column '{}': expected {:?}, got {:?}", 
                column_name, expected_type, self.get_value_type(value)
            )));
        }
        
        Ok(())
    }
    
    /// Get the type name of a SqlValue for error messages
    fn get_value_type(&self, value: &SqlValue) -> &'static str {
        match value {
            SqlValue::Null => "NULL",
            SqlValue::Integer(_) => "INTEGER",
            SqlValue::Real(_) => "REAL", 
            SqlValue::Text(_) => "TEXT",
        }
    }
    
    /// Validate UNIQUE constraints for a table
    fn validate_unique_constraints(&mut self, table_name: &str, row_data: &HashMap<String, SqlValue>, exclude_key: Option<&[u8]>) -> Result<()> {
        let schema = self.table_schemas.get(table_name)
            .ok_or_else(|| crate::Error::Other(format!("Table '{}' not found", table_name)))?;
        
        // Get columns with UNIQUE constraints
        let unique_columns: Vec<&ColumnInfo> = schema.columns
            .iter()
            .filter(|col| col.constraints.contains(&crate::parser::ColumnConstraint::Unique))
            .collect();
        
        if unique_columns.is_empty() {
            return Ok(()); // No UNIQUE constraints to check
        }
        
        // Scan existing rows to check for duplicates
        let table_key_prefix = format!("{}:", table_name);
        let start_key = table_key_prefix.as_bytes().to_vec();
        let end_key = format!("{}~", table_name).as_bytes().to_vec();
        
        let scan_results = self.transaction.scan(start_key..end_key)?;
        
        for (existing_key, existing_value) in scan_results {
            // Skip the row we're updating (if any)
            if let Some(exclude) = exclude_key {
                if existing_key == exclude {
                    continue;
                }
            }
            
            if let Ok(existing_row) = self.deserialize_row(table_name, &existing_key, &existing_value) {
                // Check each UNIQUE column
                for unique_col in &unique_columns {
                    if let (Some(new_val), Some(existing_val)) = (
                        row_data.get(&unique_col.name),
                        existing_row.get(&unique_col.name)
                    ) {
                        if new_val != &SqlValue::Null && existing_val != &SqlValue::Null && new_val == existing_val {
                            return Err(crate::Error::Other(format!(
                                "UNIQUE constraint violation: duplicate value for column '{}'", 
                                unique_col.name
                            )));
                        }
                    }
                }
            }
        }
        
        Ok(())
    }
}
