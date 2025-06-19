//! SQL executor that bridges parsed SQL statements with TegDB engine operations
//! 
//! This module provides a SQL executor that can take parsed SQL statements
//! and execute them against a TegDB engine instance using transactions for ACID compliance.

use crate::parser::{
    Statement, SelectStatement, InsertStatement, UpdateStatement, 
    DeleteStatement, CreateTableStatement, SqlValue, Condition, 
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

    /// Create a new SQL executor with the given TegDB transaction (legacy method for tests)
    /// Note: This loads schemas on each executor creation, which is less efficient than new_with_schemas
    pub fn new(transaction: crate::engine::Transaction<'a>) -> Self {
        Self {
            transaction,
            table_schemas: HashMap::new(), // Start with empty schemas for test compatibility
            in_transaction: false,
        }
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
        for (_row_idx, values) in insert.values.iter().enumerate() {
            // Create a simple row ID (in practice, you might want auto-increment or UUID)
            let row_id = format!("row_{}", chrono::Utc::now().timestamp_nanos_opt().unwrap_or(0));
            let key = format!("{}:{}", insert.table, row_id);
            
            // Create row data map
            let mut row_data = HashMap::new();
            for (col_idx, value) in values.iter().enumerate() {
                if let Some(column_name) = insert.columns.get(col_idx) {
                    row_data.insert(column_name.clone(), value.clone());
                }
            }

            // Serialize the row and add directly to the transaction
            let serialized_row = self.serialize_row(&row_data)?;
            self.transaction.set(key.as_bytes().to_vec(), serialized_row)?;
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

    /// Serialize a row to bytes (simplified JSON-like format)
    fn serialize_row(&self, row_data: &HashMap<String, SqlValue>) -> Result<Vec<u8>> {
        // Simple serialization format: column_name:value_type:value|...
        let serialized = row_data
            .iter()
            .map(|(k, v)| match v {
                SqlValue::Integer(i) => format!("{}:int:{}", k, i),
                SqlValue::Real(r) => format!("{}:real:{}", k, r),
                SqlValue::Text(s) => format!("{}:text:{}", k, s),
                SqlValue::Null => format!("{}:null:", k),
            })
            .collect::<Vec<_>>()
            .join("|");

        Ok(serialized.into_bytes())
    }

    /// Deserialize a row from bytes
    fn deserialize_row(&self, data: &[u8]) -> Result<HashMap<String, SqlValue>> {
        let data_str = String::from_utf8_lossy(data);
        let mut row_data = HashMap::new();

        for part in data_str.split('|') {
            if part.is_empty() {
                continue;
            }

            let components: Vec<&str> = part.splitn(3, ':').collect();
            if components.len() >= 3 {
                let column_name = components[0].to_string();
                let value_type = components[1];
                let value_str = components[2];

                let value = match value_type {
                    "int" => SqlValue::Integer(value_str.parse().unwrap_or(0)),
                    "real" => SqlValue::Real(value_str.parse().unwrap_or(0.0)),
                    "text" => SqlValue::Text(value_str.to_string()),
                    "null" => SqlValue::Null,
                    _ => SqlValue::Null,
                };

                row_data.insert(column_name, value);
            }
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

    /// Get the underlying transaction reference
    pub fn transaction(&self) -> &crate::engine::Transaction<'a> {
        &self.transaction
    }

    /// Get a mutable reference to the underlying transaction
    pub fn transaction_mut(&mut self) -> &mut crate::engine::Transaction<'a> {
        &mut self.transaction
    }
}
