//! SQL executor that bridges parsed SQL statements with TegDB engine operations
//! 
//! This module provides a SQL executor that can take parsed SQL statements
//! and execute them against a TegDB engine instance using transactions for ACID compliance.

use crate::parser::{
    Statement, SelectStatement, InsertStatement, UpdateStatement, 
    DeleteStatement, CreateTableStatement, DropTableStatement, SqlValue, Condition, 
    ComparisonOperator
};
use crate::Result;
use std::collections::HashMap;

/// A SQL executor that can execute parsed SQL statements against a TegDB engine
pub struct Executor<'a> {
    transaction: crate::engine::Transaction<'a>,
    /// Metadata about tables (simple schema storage)
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
    /// Create a new SQL executor with pre-loaded schemas (more efficient)
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

    /// Create a new SQL executor with the given TegDB transaction and auto-load schemas
    #[allow(dead_code)]
    pub fn new(transaction: crate::engine::Transaction<'a>) -> Self {
        let table_schemas = Self::load_schemas_from_transaction(&transaction);
        Self {
            transaction,
            table_schemas,
            in_transaction: false,
        }
    }

    /// Load all table schemas from the transaction by scanning for schema keys
    fn load_schemas_from_transaction(transaction: &crate::engine::Transaction<'a>) -> HashMap<String, TableSchema> {
        let mut schemas = HashMap::new();
        
        // Scan for schema keys (format: __schema__:{table_name})
        let schema_range = b"__schema__:".to_vec()..b"__schema__~".to_vec();
        let schema_entries = transaction.scan(schema_range);
        
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

    /// Deserialize a schema from bytes
    fn deserialize_schema(data: &[u8]) -> Result<TableSchema> {
        let serialized = std::str::from_utf8(data)
            .map_err(|_| crate::Error::Other("Invalid schema data encoding".to_string()))?;
        
        let mut columns = Vec::new();
        
        for column_data in serialized.split('|') {
            let parts: Vec<&str> = column_data.split(':').collect();
            if parts.len() != 3 {
                continue;
            }
            
            let name = parts[0].to_string();
            let data_type = match parts[1] {
                "INTEGER" => crate::parser::DataType::Integer,
                "TEXT" => crate::parser::DataType::Text,
                "REAL" => crate::parser::DataType::Real,
                "BLOB" => crate::parser::DataType::Blob,
                _ => continue,
            };
            
            let constraints = if parts[2].is_empty() {
                Vec::new()
            } else {
                parts[2].split(',').filter_map(|c| match c {
                    "PRIMARY_KEY" => Some(crate::parser::ColumnConstraint::PrimaryKey),
                    "NOT_NULL" => Some(crate::parser::ColumnConstraint::NotNull),
                    "UNIQUE" => Some(crate::parser::ColumnConstraint::Unique),
                    _ => None,
                }).collect()
            };
            
            columns.push(ColumnInfo {
                name,
                data_type,
                constraints,
            });
        }
        
        Ok(TableSchema { columns })
    }

    /// Execute a parsed SQL statement with explicit transaction control
    pub fn execute(&mut self, statement: Statement) -> Result<ResultSet> {
        match statement {
            Statement::Begin => self.execute_begin(),
            Statement::Commit => self.execute_commit(),
            Statement::Rollback => self.execute_rollback(),
            Statement::Select(select) => {
                if !self.in_transaction {
                    return Err(crate::Error::Other("No active transaction. Use BEGIN to start a transaction.".to_string()));
                }
                self.execute_select(select)
            }
            Statement::Insert(insert) => {
                if !self.in_transaction {
                    return Err(crate::Error::Other("No active transaction. Use BEGIN to start a transaction.".to_string()));
                }
                self.execute_insert(insert)
            }
            Statement::Update(update) => {
                if !self.in_transaction {
                    return Err(crate::Error::Other("No active transaction. Use BEGIN to start a transaction.".to_string()));
                }
                self.execute_update(update)
            }
            Statement::Delete(delete) => {
                if !self.in_transaction {
                    return Err(crate::Error::Other("No active transaction. Use BEGIN to start a transaction.".to_string()));
                }
                self.execute_delete(delete)
            }
            Statement::CreateTable(create) => {
                if !self.in_transaction {
                    return Err(crate::Error::Other("No active transaction. Use BEGIN to start a transaction.".to_string()));
                }
                self.execute_create_table(create)
            }
            Statement::DropTable(drop) => {
                if !self.in_transaction {
                    return Err(crate::Error::Other("No active transaction. Use BEGIN to start a transaction.".to_string()));
                }
                self.execute_drop_table(drop)
            }
        }
    }

    /// Execute a BEGIN statement
    fn execute_begin(&mut self) -> Result<ResultSet> {
        if self.in_transaction {
            return Err(crate::Error::Other("Already in a transaction".to_string()));
        }
        
        self.in_transaction = true;
        
        Ok(ResultSet::Begin)
    }

    /// Execute a COMMIT statement  
    fn execute_commit(&mut self) -> Result<ResultSet> {
        if !self.in_transaction {
            return Err(crate::Error::Other("No active transaction to commit".to_string()));
        }
        
        // Note: The actual commit will happen when the transaction is dropped/committed externally
        self.in_transaction = false;
        
        Ok(ResultSet::Commit)
    }

    /// Execute a ROLLBACK statement
    fn execute_rollback(&mut self) -> Result<ResultSet> {
        if !self.in_transaction {
            return Err(crate::Error::Other("No active transaction to rollback".to_string()));
        }
        
        // Note: The actual rollback will happen when the transaction is dropped/rolled back externally
        self.in_transaction = false;
        
        Ok(ResultSet::Rollback)
    }

    /// Execute a SELECT statement within a transaction
    fn execute_select(&mut self, select: SelectStatement) -> Result<ResultSet> {
        // Get data from the transaction (includes committed data + pending operations)
        let table_key_prefix = format!("{}:", select.table);
        let mut matching_rows: Vec<HashMap<String, SqlValue>> = Vec::new();
        
        let start_key = table_key_prefix.as_bytes().to_vec();
        let end_key = format!("{}~", select.table).as_bytes().to_vec(); // '~' comes after ':'
        
        // The transaction's scan method already includes pending operations
        let scan_results = self.transaction.scan(start_key..end_key);
        
        // Process the scan results
        for (_key, value) in scan_results {
            // Deserialize the row data
            if let Ok(row_data) = self.deserialize_row(&value) {
                // Apply WHERE clause if present
                if let Some(ref where_clause) = select.where_clause {
                    if self.evaluate_condition(&where_clause.condition, &row_data) {
                        matching_rows.push(row_data);
                    }
                } else {
                    matching_rows.push(row_data);
                }
            }
        }

        // Apply column selection
        let result_columns = if select.columns.len() == 1 && select.columns[0] == "*" {
            // Return all columns - for simplicity, we'll use the first row's keys
            if let Some(first_row) = matching_rows.first() {
                first_row.keys().cloned().collect()
            } else {
                vec![]
            }
        } else {
            select.columns
        };

        // Extract selected columns from matching rows
        let result_rows: Vec<Vec<SqlValue>> = matching_rows
            .into_iter()
            .map(|row| {
                result_columns
                    .iter()
                    .map(|col| row.get(col).cloned().unwrap_or(SqlValue::Null))
                    .collect()
            })
            .collect();

        // Apply LIMIT if present
        let limited_rows = if let Some(limit) = select.limit {
            result_rows.into_iter().take(limit as usize).collect()
        } else {
            result_rows
        };

        Ok(ResultSet::Select {
            columns: result_columns,
            rows: limited_rows,
        })
    }

    /// Execute an INSERT statement within a transaction
    fn execute_insert(&mut self, insert: InsertStatement) -> Result<ResultSet> {
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

            // Generate row key based on primary key values (IOT approach)
            let row_key = self.generate_row_key(&insert.table, &row_data)?;
            
            // Check for primary key constraint violation
            if self.primary_key_exists(&insert.table, &row_data)? {
                return Err(crate::Error::Other(format!(
                    "Primary key constraint violation: duplicate key in table '{}'", 
                    insert.table
                )));
            }

            // Serialize the row and store with primary key as the actual storage key
            let serialized_row = self.serialize_row(&row_data)?;
            self.transaction.set(row_key.as_bytes().to_vec(), serialized_row)?;
            rows_affected += 1;
        }

        Ok(ResultSet::Insert { rows_affected })
    }

    /// Execute an UPDATE statement within a transaction
    fn execute_update(&mut self, update: UpdateStatement) -> Result<ResultSet> {
        let mut rows_affected = 0;
        let table_key_prefix = format!("{}:", update.table);
        
        // Get current state using transaction's scan method (includes pending operations)
        let start_key = table_key_prefix.as_bytes().to_vec();
        let end_key = format!("{}~", update.table).as_bytes().to_vec();
        
        let current_data = self.transaction.scan(start_key..end_key);
        
        // Process each row
        for (key, value) in current_data {
            if let Ok(mut row_data) = self.deserialize_row(&value) {
                // Check if row matches WHERE clause
                let matches = if let Some(ref where_clause) = update.where_clause {
                    self.evaluate_condition(&where_clause.condition, &row_data)
                } else {
                    true
                };
                
                if matches {
                    // Apply updates
                    for assignment in &update.assignments {
                        row_data.insert(assignment.column.clone(), assignment.value.clone());
                    }
                    
                    // Serialize updated row and apply directly to transaction
                    let serialized_row = self.serialize_row(&row_data)?;
                    self.transaction.set(key, serialized_row)?;
                    rows_affected += 1;
                }
            }
        }

        Ok(ResultSet::Update { rows_affected })
    }

    /// Execute a DELETE statement within a transaction
    fn execute_delete(&mut self, delete: DeleteStatement) -> Result<ResultSet> {
        let mut rows_affected = 0;
        let table_key_prefix = format!("{}:", delete.table);
        
        // Get current state using transaction's scan method (includes pending operations)
        let start_key = table_key_prefix.as_bytes().to_vec();
        let end_key = format!("{}~", delete.table).as_bytes().to_vec();
        
        let current_data = self.transaction.scan(start_key..end_key);
        
        // Find rows to delete
        for (key, value) in current_data {
            if let Ok(row_data) = self.deserialize_row(&value) {
                // Check if row matches WHERE clause
                let should_delete = if let Some(ref where_clause) = delete.where_clause {
                    self.evaluate_condition(&where_clause.condition, &row_data)
                } else {
                    true // DELETE without WHERE deletes all rows
                };
                
                if should_delete {
                    // Apply deletion directly to transaction
                    self.transaction.delete(key)?;
                    rows_affected += 1;
                }
            }
        }

        Ok(ResultSet::Delete { rows_affected })
    }

    /// Execute a CREATE TABLE statement within a transaction
    fn execute_create_table(&mut self, create: CreateTableStatement) -> Result<ResultSet> {
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
        self.transaction.set(schema_key.as_bytes().to_vec(), serialized_schema)?;

        Ok(ResultSet::CreateTable { 
            table_name: create.table 
        })
    }

    /// Execute a DROP TABLE statement within a transaction
    fn execute_drop_table(&mut self, drop: DropTableStatement) -> Result<ResultSet> {
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
        self.transaction.delete(schema_key.as_bytes().to_vec())?;
        
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
        let scan_results = self.transaction.scan(start_key..end_key);
        for (key, _) in scan_results {
            keys_to_delete.push(key);
        }

        // Delete all found keys
        for key in keys_to_delete {
            self.transaction.delete(key)?;
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

    /// Serialize a row to bytes using efficient binary format
    fn serialize_row(&self, row_data: &HashMap<String, SqlValue>) -> Result<Vec<u8>> {
        Ok(crate::serialization::BinaryRowSerializer::serialize(row_data))
    }

    /// Deserialize a row from bytes using efficient binary format
    fn deserialize_row(&self, data: &[u8]) -> Result<HashMap<String, SqlValue>> {
        crate::serialization::BinaryRowSerializer::deserialize(data)
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
    #[allow(dead_code)]
    fn get_row_by_primary_key(&mut self, table_name: &str, pk_values: &HashMap<String, SqlValue>) -> Result<Option<HashMap<String, SqlValue>>> {
        let row_key = self.generate_row_key(table_name, pk_values)?;
        
        if let Some(value) = self.transaction.get(row_key.as_bytes()) {
            Ok(Some(self.deserialize_row(&value)?))
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
}
