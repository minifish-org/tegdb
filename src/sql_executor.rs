//! SQL executor that bridges parsed SQL statements with TegDB engine operations
//! 
//! This module provides a SQL executor that can take parsed SQL statements
//! and execute them against a TegDB engine instance.

use crate::sql::{
    SqlStatement, SelectStatement, InsertStatement, UpdateStatement, 
    DeleteStatement, CreateTableStatement, SqlValue, Condition, 
    ComparisonOperator
};
use crate::{Engine, Result};
use std::collections::HashMap;

/// A SQL executor that can execute parsed SQL statements against a TegDB engine
pub struct SqlExecutor {
    engine: Engine,
    /// Metadata about tables (simple schema storage)
    table_schemas: HashMap<String, TableSchema>,
}

/// Simple table schema representation
#[derive(Debug, Clone)]
pub struct TableSchema {
    pub columns: Vec<ColumnInfo>,
}

#[derive(Debug, Clone)]
pub struct ColumnInfo {
    pub name: String,
    pub data_type: crate::sql::DataType,
    pub constraints: Vec<crate::sql::ColumnConstraint>,
}

/// Result of executing a SQL statement
#[derive(Debug, Clone)]
pub enum SqlResult {
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
}

impl SqlExecutor {
    /// Create a new SQL executor with the given TegDB engine
    pub fn new(engine: Engine) -> Self {
        Self {
            engine,
            table_schemas: HashMap::new(),
        }
    }

    /// Execute a parsed SQL statement
    pub fn execute(&mut self, statement: SqlStatement) -> Result<SqlResult> {
        match statement {
            SqlStatement::Select(select) => self.execute_select(select),
            SqlStatement::Insert(insert) => self.execute_insert(insert),
            SqlStatement::Update(update) => self.execute_update(update),
            SqlStatement::Delete(delete) => self.execute_delete(delete),
            SqlStatement::CreateTable(create) => self.execute_create_table(create),
        }
    }

    /// Execute a SELECT statement
    fn execute_select(&mut self, select: SelectStatement) -> Result<SqlResult> {
        // For this implementation, we'll use a simple key-value approach
        // where each row is stored as table_name:row_id -> serialized_row_data
        
        let table_key_prefix = format!("{}:", select.table);
        let mut matching_rows = Vec::new();
        
        // Scan all keys that start with the table prefix
        let start_key = table_key_prefix.as_bytes().to_vec();
        let end_key = format!("{}~", select.table).as_bytes().to_vec(); // '~' comes after ':'
        
        let iter = self.engine.scan(start_key..end_key)?;
        
        for (_key, value) in iter {
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

        Ok(SqlResult::Select {
            columns: result_columns,
            rows: limited_rows,
        })
    }

    /// Execute an INSERT statement
    fn execute_insert(&mut self, insert: InsertStatement) -> Result<SqlResult> {
        let mut rows_affected = 0;

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

            // Serialize and store the row
            let serialized_row = self.serialize_row(&row_data)?;
            self.engine.set(key.as_bytes(), serialized_row)?;
            rows_affected += 1;
        }

        Ok(SqlResult::Insert { rows_affected })
    }

    /// Execute an UPDATE statement
    fn execute_update(&mut self, update: UpdateStatement) -> Result<SqlResult> {
        let table_key_prefix = format!("{}:", update.table);
        let mut rows_affected = 0;
        
        // Find matching rows
        let start_key = table_key_prefix.as_bytes().to_vec();
        let end_key = format!("{}~", update.table).as_bytes().to_vec();
        
        let iter = self.engine.scan(start_key..end_key)?;
        let mut updates = Vec::new();
        
        for (key, value) in iter {
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

        // Apply updates
        for (key, value) in updates {
            self.engine.set(&key, value)?;
        }

        Ok(SqlResult::Update { rows_affected })
    }

    /// Execute a DELETE statement
    fn execute_delete(&mut self, delete: DeleteStatement) -> Result<SqlResult> {
        let table_key_prefix = format!("{}:", delete.table);
        let mut rows_affected = 0;
        
        // Find matching rows
        let start_key = table_key_prefix.as_bytes().to_vec();
        let end_key = format!("{}~", delete.table).as_bytes().to_vec();
        
        let iter = self.engine.scan(start_key..end_key)?;
        let mut keys_to_delete = Vec::new();
        
        for (key, value) in iter {
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

        // Delete matching rows
        for key in keys_to_delete {
            self.engine.del(&key)?;
        }

        Ok(SqlResult::Delete { rows_affected })
    }

    /// Execute a CREATE TABLE statement
    fn execute_create_table(&mut self, create: CreateTableStatement) -> Result<SqlResult> {
        // Store table schema metadata
        let schema = TableSchema {
            columns: create.columns.iter().map(|col| ColumnInfo {
                name: col.name.clone(),
                data_type: col.data_type.clone(),
                constraints: col.constraints.clone(),
            }).collect(),
        };

        self.table_schemas.insert(create.table.clone(), schema);

        // Store schema in the database for persistence
        let schema_key = format!("__schema__:{}", create.table);
        let serialized_schema = self.serialize_schema(&create)?;
        self.engine.set(schema_key.as_bytes(), serialized_schema)?;

        Ok(SqlResult::CreateTable {
            table_name: create.table,
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
                    crate::sql::DataType::Integer => "INTEGER",
                    crate::sql::DataType::Text => "TEXT",
                    crate::sql::DataType::Real => "REAL",
                    crate::sql::DataType::Blob => "BLOB",
                };
                
                let constraints = col.constraints
                    .iter()
                    .map(|c| match c {
                        crate::sql::ColumnConstraint::PrimaryKey => "PRIMARY_KEY",
                        crate::sql::ColumnConstraint::NotNull => "NOT_NULL",
                        crate::sql::ColumnConstraint::Unique => "UNIQUE",
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
    use crate::sql::parse_sql;
    use tempfile::tempdir;

    #[test]
    fn test_sql_executor_create_and_insert() {
        let dir = tempdir().unwrap();
        let db_path = dir.path().join("test.db");
        let engine = Engine::new(db_path).unwrap();
        let mut executor = SqlExecutor::new(engine);

        // Create table
        let create_sql = "CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT NOT NULL, age INTEGER)";
        let (_, statement) = parse_sql(create_sql).unwrap();
        let result = executor.execute(statement).unwrap();
        
        match result {
            SqlResult::CreateTable { table_name } => {
                assert_eq!(table_name, "users");
            }
            _ => panic!("Expected CreateTable result"),
        }

        // Insert data
        let insert_sql = "INSERT INTO users (id, name, age) VALUES (1, 'John', 25)";
        let (_, statement) = parse_sql(insert_sql).unwrap();
        let result = executor.execute(statement).unwrap();

        match result {
            SqlResult::Insert { rows_affected } => {
                assert_eq!(rows_affected, 1);
            }
            _ => panic!("Expected Insert result"),
        }
    }

    #[test]
    fn test_sql_executor_select() {
        let dir = tempdir().unwrap();
        let db_path = dir.path().join("test.db");
        let engine = Engine::new(db_path).unwrap();
        let mut executor = SqlExecutor::new(engine);

        // Insert test data
        let insert_sql = "INSERT INTO users (id, name, age) VALUES (1, 'John', 25), (2, 'Jane', 30)";
        let (_, statement) = parse_sql(insert_sql).unwrap();
        executor.execute(statement).unwrap();

        // Select all
        let select_sql = "SELECT * FROM users";
        let (_, statement) = parse_sql(select_sql).unwrap();
        let result = executor.execute(statement).unwrap();

        match result {
            SqlResult::Select { columns: _, rows } => {
                assert_eq!(rows.len(), 2);
            }
            _ => panic!("Expected Select result"),
        }
    }
}
