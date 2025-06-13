//! SQL executor that bridges parsed SQL statements with TegDB engine operations
//! 
//! This module provides a SQL executor that can take parsed SQL statements
//! and execute them against a TegDB engine instance using transactions for ACID compliance.

use crate::parser::{
    Statement, SelectStatement, InsertStatement, UpdateStatement, 
    DeleteStatement, CreateTableStatement, SqlValue, Condition, 
    ComparisonOperator
};
use crate::{Engine, Result};
use std::collections::HashMap;

/// A SQL executor that can execute parsed SQL statements against a TegDB engine
pub struct Executor {
    engine: Engine,
    /// Metadata about tables (simple schema storage)
    table_schemas: HashMap<String, TableSchema>,
    /// Track if we're in an explicit transaction
    in_transaction: bool,
    /// Transaction ID counter
    transaction_counter: u64,
    /// Pending operations within the current transaction
    pending_operations: Vec<Entry>,
}

/// Entry for batch operations
#[derive(Debug, Clone)]
pub struct Entry {
    pub key: Vec<u8>,
    pub value: Option<Vec<u8>>,
}

impl Entry {
    pub fn new(key: Vec<u8>, value: Option<Vec<u8>>) -> Self {
        Self { key, value }
    }
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
    Begin { 
        transaction_id: String 
    },
    /// Result of a COMMIT operation
    Commit { 
        transaction_id: String 
    },
    /// Result of a ROLLBACK operation
    Rollback { 
        transaction_id: String 
    },
}

impl Executor {
    /// Create a new SQL executor with the given TegDB engine
    pub fn new(engine: Engine) -> Self {
        Self {
            engine,
            table_schemas: HashMap::new(),
            in_transaction: false,
            transaction_counter: 0,
            pending_operations: Vec::new(),
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
        self.transaction_counter += 1;
        let transaction_id = format!("tx_{}", self.transaction_counter);
        
        Ok(ResultSet::Begin { transaction_id })
    }

    /// Execute a COMMIT statement
    fn execute_commit(&mut self) -> Result<ResultSet> {
        if !self.in_transaction {
            return Err(crate::Error::Other("No active transaction to commit".to_string()));
        }
        
        // Apply all pending operations atomically using the engine's batch method
        if !self.pending_operations.is_empty() {
            // Convert our Entry format to the engine's Entry format
            let engine_entries: Vec<crate::Entry> = self.pending_operations
                .iter()
                .map(|op| crate::Entry::new(op.key.clone(), op.value.clone()))
                .collect();
            
            self.engine.batch(engine_entries)?;
        }
        
        // Clear transaction state
        self.in_transaction = false;
        self.pending_operations.clear();
        let transaction_id = format!("tx_{}", self.transaction_counter);
        
        Ok(ResultSet::Commit { transaction_id })
    }

    /// Execute a ROLLBACK statement
    fn execute_rollback(&mut self) -> Result<ResultSet> {
        if !self.in_transaction {
            return Err(crate::Error::Other("No active transaction to rollback".to_string()));
        }
        
        // Discard all pending operations without applying them
        self.in_transaction = false;
        self.pending_operations.clear();
        let transaction_id = format!("tx_{}", self.transaction_counter);
        
        Ok(ResultSet::Rollback { transaction_id })
    }

    /// Execute a SELECT statement within a transaction
    fn execute_select(&mut self, select: SelectStatement) -> Result<ResultSet> {
        // Create a snapshot view combining committed data and pending operations
        let table_key_prefix = format!("{}:", select.table);
        let mut matching_rows = Vec::new();
        
        // Get the current snapshot from the engine
        let start_key = table_key_prefix.as_bytes().to_vec();
        let end_key = format!("{}~", select.table).as_bytes().to_vec(); // '~' comes after ':'
        
        // Scan committed data
        let mut committed_data: HashMap<Vec<u8>, Vec<u8>> = HashMap::new();
        let scan_iter = self.engine.scan(start_key..end_key)?;
        for (key, value) in scan_iter {
            committed_data.insert(key, value.as_ref().to_vec());
        }
        
        // Apply pending operations to get the transaction view
        for operation in &self.pending_operations {
            if operation.key.starts_with(table_key_prefix.as_bytes()) {
                match &operation.value {
                    Some(value) => {
                        committed_data.insert(operation.key.clone(), value.clone());
                    }
                    None => {
                        committed_data.remove(&operation.key);
                    }
                }
            }
        }
        
        // Process the merged data
        for (_key, value) in committed_data {
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

        // Prepare and accumulate each row operation
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

            // Serialize the row and add to pending operations
            let serialized_row = self.serialize_row(&row_data)?;
            self.pending_operations.push(Entry::new(key.as_bytes().to_vec(), Some(serialized_row)));
            rows_affected += 1;
        }

        Ok(ResultSet::Insert { rows_affected })
    }

    /// Execute an UPDATE statement within a transaction
    fn execute_update(&mut self, update: UpdateStatement) -> Result<ResultSet> {
        let mut rows_affected = 0;
        let table_key_prefix = format!("{}:", update.table);
        
        // First, find all rows that match the WHERE clause
        let start_key = table_key_prefix.as_bytes().to_vec();
        let end_key = format!("{}~", update.table).as_bytes().to_vec();
        
        // Get current state (committed + pending)
        let mut current_data: HashMap<Vec<u8>, Vec<u8>> = HashMap::new();
        let scan_iter = self.engine.scan(start_key..end_key)?;
        for (key, value) in scan_iter {
            current_data.insert(key, value.as_ref().to_vec());
        }
        
        // Apply pending operations
        for operation in &self.pending_operations {
            if operation.key.starts_with(table_key_prefix.as_bytes()) {
                match &operation.value {
                    Some(value) => {
                        current_data.insert(operation.key.clone(), value.clone());
                    }
                    None => {
                        current_data.remove(&operation.key);
                    }
                }
            }
        }
        
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
                    
                    // Serialize updated row and add to pending operations
                    let serialized_row = self.serialize_row(&row_data)?;
                    self.pending_operations.push(Entry::new(key, Some(serialized_row)));
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
        
        // Get current state (committed + pending)
        let start_key = table_key_prefix.as_bytes().to_vec();
        let end_key = format!("{}~", delete.table).as_bytes().to_vec();
        
        let mut current_data: HashMap<Vec<u8>, Vec<u8>> = HashMap::new();
        let scan_iter = self.engine.scan(start_key..end_key)?;
        for (key, value) in scan_iter {
            current_data.insert(key, value.as_ref().to_vec());
        }
        
        // Apply pending operations to get current view
        for operation in &self.pending_operations {
            if operation.key.starts_with(table_key_prefix.as_bytes()) {
                match &operation.value {
                    Some(value) => {
                        current_data.insert(operation.key.clone(), value.clone());
                    }
                    None => {
                        current_data.remove(&operation.key);
                    }
                }
            }
        }
        
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
                    // Add deletion to pending operations (None value means delete)
                    self.pending_operations.push(Entry::new(key, None));
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
        
        // In a real implementation, you might want to store the schema in the database
        // For now, we'll just track it in memory and add an entry to indicate the table exists
        let schema_key = format!("__schema__:{}", create.table);
        let serialized_schema = self.serialize_schema(&create)?;
        self.pending_operations.push(Entry::new(schema_key.as_bytes().to_vec(), Some(serialized_schema)));

        Ok(ResultSet::CreateTable { 
            table_name: create.table 
        })
    }

    /// Execute a SELECT statement with its own transaction
    #[allow(dead_code)]
    fn execute_select_with_transaction(&mut self, select: SelectStatement) -> Result<ResultSet> {
        // For SELECT, we can use a read-only transaction snapshot
        let transaction = self.engine.begin_transaction();
        
        // For this implementation, we'll use a simple key-value approach
        // where each row is stored as table_name:row_id -> serialized_row_data
        
        let table_key_prefix = format!("{}:", select.table);
        let mut matching_rows = Vec::new();
        
        // Use transaction's scan to get consistent view
        let start_key = table_key_prefix.as_bytes().to_vec();
        let end_key = format!("{}~", select.table).as_bytes().to_vec(); // '~' comes after ':'
        
        let scan_results = transaction.scan(start_key..end_key);
        
        for (_key, value) in scan_results {
            // Deserialize the row data (simplified JSON-like format)
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

    /// Execute an INSERT statement with its own transaction
    #[allow(dead_code)]
    fn execute_insert_with_transaction(&mut self, insert: InsertStatement) -> Result<ResultSet> {
        let mut rows_affected = 0;

        // Prepare serialized data before transaction
        let mut serialized_entries = Vec::new();
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

            // Serialize the row
            let serialized_row = self.serialize_row(&row_data)?;
            serialized_entries.push((key.as_bytes().to_vec(), serialized_row));
        }

        // Execute within a transaction scope
        {
            let mut transaction = self.engine.begin_transaction();
            
            for (key, serialized_row) in serialized_entries {
                transaction.set(key, serialized_row)?;
                rows_affected += 1;
            }

            // Commit the transaction
            transaction.commit()?;
        }

        Ok(ResultSet::Insert { rows_affected })
    }

    /// Execute an UPDATE statement with its own transaction
    #[allow(dead_code)]
    fn execute_update_with_transaction(&mut self, update: UpdateStatement) -> Result<ResultSet> {
        let mut rows_affected = 0;
        let table_key_prefix = format!("{}:", update.table);

        // First pass: scan and collect data to update (before transaction)
        let scan_data = {
            let transaction = self.engine.begin_transaction();
            let start_key = table_key_prefix.as_bytes().to_vec();
            let end_key = format!("{}~", update.table).as_bytes().to_vec();
            transaction.scan(start_key..end_key)
        };

        // Prepare updates outside of transaction
        let mut updates = Vec::new();
        for (key, value) in scan_data {
            if let Ok(mut row_data) = self.deserialize_row(&value) {
                // Check WHERE clause
                let should_update = if let Some(ref where_clause) = update.where_clause {
                    self.evaluate_condition(&where_clause.condition, &row_data)
                } else {
                    true
                };

                if should_update {
                    // Apply assignments
                    for assignment in &update.assignments {
                        row_data.insert(assignment.column.clone(), assignment.value.clone());
                    }
                    
                    let serialized_row = self.serialize_row(&row_data)?;
                    updates.push((key, serialized_row));
                    rows_affected += 1;
                }
            }
        }

        // Execute updates within a transaction scope
        {
            let mut transaction = self.engine.begin_transaction();
            
            for (key, value) in updates {
                transaction.set(key, value)?;
            }

            // Commit the transaction
            transaction.commit()?;
        }

        Ok(ResultSet::Update { rows_affected })
    }

    /// Execute a DELETE statement with its own transaction
    #[allow(dead_code)]
    fn execute_delete_with_transaction(&mut self, delete: DeleteStatement) -> Result<ResultSet> {
        let mut rows_affected = 0;
        let table_key_prefix = format!("{}:", delete.table);

        // First pass: scan and identify keys to delete (before transaction)
        let scan_data = {
            let transaction = self.engine.begin_transaction();
            let start_key = table_key_prefix.as_bytes().to_vec();
            let end_key = format!("{}~", delete.table).as_bytes().to_vec();
            transaction.scan(start_key..end_key)
        };

        // Prepare deletions outside of transaction
        let mut keys_to_delete = Vec::new();
        for (key, value) in scan_data {
            if let Ok(row_data) = self.deserialize_row(&value) {
                // Check WHERE clause
                let should_delete = if let Some(ref where_clause) = delete.where_clause {
                    self.evaluate_condition(&where_clause.condition, &row_data)
                } else {
                    true
                };

                if should_delete {
                    keys_to_delete.push(key);
                    rows_affected += 1;
                }
            }
        }

        // Execute deletions within a transaction scope
        {
            let mut transaction = self.engine.begin_transaction();
            
            for key in keys_to_delete {
                transaction.delete(key)?;
            }

            // Commit the transaction
            transaction.commit()?;
        }

        Ok(ResultSet::Delete { rows_affected })
    }

    /// Execute a CREATE TABLE statement with its own transaction
    #[allow(dead_code)]
    fn execute_create_table_with_transaction(&mut self, create: CreateTableStatement) -> Result<ResultSet> {
        // Prepare schema data before transaction
        let schema = TableSchema {
            columns: create.columns.iter().map(|col| ColumnInfo {
                name: col.name.clone(),
                data_type: col.data_type.clone(),
                constraints: col.constraints.clone(),
            }).collect(),
        };

        let schema_key = format!("__schema__:{}", create.table);
        let serialized_schema = self.serialize_schema(&create)?;
        let table_name = create.table.clone();

        // Execute within a transaction scope
        {
            let mut transaction = self.engine.begin_transaction();
            
            // Store schema in the database for persistence
            transaction.set(schema_key.as_bytes().to_vec(), serialized_schema)?;

            // Commit the transaction
            transaction.commit()?;
        }

        // Store table schema metadata after successful transaction
        self.table_schemas.insert(table_name.clone(), schema);

        Ok(ResultSet::CreateTable {
            table_name,
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

    /// Get the underlying engine reference
    pub fn engine(&self) -> &Engine {
        &self.engine
    }

    /// Get a mutable reference to the underlying engine
    pub fn engine_mut(&mut self) -> &mut Engine {
        &mut self.engine
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::parser::parse_sql;
    use tempfile::tempdir;

    #[test]
    fn test_executor_create_and_insert() {
        let dir = tempdir().unwrap();
        let db_path = dir.path().join("test.db");
        let engine = Engine::new(db_path).unwrap();
        let mut executor = Executor::new(engine);

        // Begin transaction
        let (_, statement) = parse_sql("BEGIN").unwrap();
        let result = executor.execute(statement).unwrap();
        match result {
            ResultSet::Begin { .. } => {},
            _ => panic!("Expected Begin result"),
        }

        // Create table
        let create_sql = "CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT NOT NULL, age INTEGER)";
        let (_, statement) = parse_sql(create_sql).unwrap();
        let result = executor.execute(statement).unwrap();
        
        match result {
            ResultSet::CreateTable { table_name } => {
                assert_eq!(table_name, "users");
            }
            _ => panic!("Expected CreateTable result"),
        }

        // Insert data
        let insert_sql = "INSERT INTO users (id, name, age) VALUES (1, 'John', 25)";
        let (_, statement) = parse_sql(insert_sql).unwrap();
        let result = executor.execute(statement).unwrap();

        match result {
            ResultSet::Insert { rows_affected } => {
                assert_eq!(rows_affected, 1);
            }
            _ => panic!("Expected Insert result"),
        }

        // Commit transaction
        let (_, statement) = parse_sql("COMMIT").unwrap();
        let result = executor.execute(statement).unwrap();
        match result {
            ResultSet::Commit { .. } => {},
            _ => panic!("Expected Commit result"),
        }
    }

    #[test]
    fn test_executor_select() {
        let dir = tempdir().unwrap();
        let db_path = dir.path().join("test.db");
        let engine = Engine::new(db_path).unwrap();
        let mut executor = Executor::new(engine);

        // Begin transaction
        let (_, statement) = parse_sql("BEGIN").unwrap();
        executor.execute(statement).unwrap();

        // Create table first
        let create_sql = "CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT NOT NULL, age INTEGER)";
        let (_, statement) = parse_sql(create_sql).unwrap();
        executor.execute(statement).unwrap();

        // Insert test data
        let insert_sql = "INSERT INTO users (id, name, age) VALUES (1, 'John', 25), (2, 'Jane', 30)";
        let (_, statement) = parse_sql(insert_sql).unwrap();
        executor.execute(statement).unwrap();

        // Select all
        let select_sql = "SELECT * FROM users";
        let (_, statement) = parse_sql(select_sql).unwrap();
        let result = executor.execute(statement).unwrap();

        match result {
            ResultSet::Select { columns: _, rows } => {
                assert_eq!(rows.len(), 2);
            }
            _ => panic!("Expected Select result"),
        }

        // Commit transaction
        let (_, statement) = parse_sql("COMMIT").unwrap();
        executor.execute(statement).unwrap();
    }

    #[test]
    fn test_transaction_rollback_on_error() {
        let dir = tempdir().unwrap();
        let db_path = dir.path().join("test.db");
        let engine = Engine::new(db_path).unwrap();
        let mut executor = Executor::new(engine);

        // Begin transaction
        let (_, statement) = parse_sql("BEGIN").unwrap();
        executor.execute(statement).unwrap();

        // Create table first
        let create_sql = "CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT NOT NULL, age INTEGER)";
        let (_, statement) = parse_sql(create_sql).unwrap();
        executor.execute(statement).unwrap();

        // Insert initial data
        let insert_sql = "INSERT INTO users (id, name, age) VALUES (1, 'John', 25)";
        let (_, statement) = parse_sql(insert_sql).unwrap();
        executor.execute(statement).unwrap();

        // Verify initial state
        let select_sql = "SELECT * FROM users";
        let (_, statement) = parse_sql(select_sql).unwrap();
        let result = executor.execute(statement).unwrap();
        if let ResultSet::Select { rows, .. } = result {
            assert_eq!(rows.len(), 1);
        }

        // Test rollback
        let (_, statement) = parse_sql("ROLLBACK").unwrap();
        let result = executor.execute(statement).unwrap();
        match result {
            ResultSet::Rollback { .. } => {},
            _ => panic!("Expected Rollback result"),
        }

        // This test demonstrates rollback functionality
        // All operations within the transaction are discarded
    }
}
