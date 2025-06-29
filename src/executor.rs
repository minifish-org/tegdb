//! Modern executor for TegDB with native row format support
//! 
//! This module provides the core execution engine that works directly with the
//! native binary row format for optimal performance.

use crate::engine::Transaction;
use crate::parser::{
    SqlValue, Statement, SelectStatement, InsertStatement, UpdateStatement, 
    DeleteStatement, CreateTableStatement, DropTableStatement, DataType, 
    ColumnConstraint, ComparisonOperator, Condition
};
use crate::storage_format::StorageFormat;
use crate::{Result, Error};
use std::collections::HashMap;

/// Column information for table schema
#[derive(Debug, Clone)]
pub struct ColumnInfo {
    pub name: String,
    pub data_type: DataType,
    pub constraints: Vec<ColumnConstraint>,
}

/// Table schema definition
#[derive(Debug, Clone)]
pub struct TableSchema {
    pub name: String,
    pub columns: Vec<ColumnInfo>,
}

/// Query execution result
#[derive(Debug, Clone)]
pub enum ResultSet {
    /// SELECT query result
    Select {
        columns: Vec<String>,
        rows: Vec<Vec<SqlValue>>,
    },
    /// INSERT query result
    Insert {
        rows_affected: usize,
    },
    /// UPDATE query result
    Update {
        rows_affected: usize,
    },
    /// DELETE query result
    Delete {
        rows_affected: usize,
    },
    /// CREATE TABLE query result
    CreateTable,
    /// DROP TABLE query result
    DropTable {
        table_name: String,
        existed: bool,
    },
    /// Transaction BEGIN result
    Begin,
    /// Transaction COMMIT result
    Commit,
    /// Transaction ROLLBACK result
    Rollback,
}

/// SQL executor with native row format support
pub struct Executor<'a> {
    transaction: Transaction<'a>,
    table_schemas: HashMap<String, TableSchema>,
    storage_format: StorageFormat,
    transaction_active: bool,
}

impl<'a> Executor<'a> {
    /// Create a new executor with transaction and schemas
    pub fn new_with_schemas(
        transaction: Transaction<'a>,
        table_schemas: HashMap<String, TableSchema>
    ) -> Self {
        let mut executor = Self {
            transaction,
            table_schemas,
            storage_format: StorageFormat::new(), // Always use native format
            transaction_active: false,
        };
        
        // Load additional schemas from storage and merge
        let _ = executor.load_schemas_from_storage();
        
        executor
    }

    /// Create a new executor with transaction
    pub fn new(transaction: Transaction<'a>) -> Self {
        let mut executor = Self {
            transaction,
            table_schemas: HashMap::new(),
            storage_format: StorageFormat::new(),
            transaction_active: false,
        };
        
        // Load schemas from storage into the executor
        let _ = executor.load_schemas_from_storage();
        
        executor
    }

    /// Get reference to the transaction
    pub fn transaction(&self) -> &Transaction<'a> {
        &self.transaction
    }

    /// Get mutable reference to the transaction
    pub fn transaction_mut(&mut self) -> &mut Transaction<'a> {
        &mut self.transaction
    }

    /// Execute a SQL statement
    pub fn execute_statement(&mut self, statement: Statement) -> Result<ResultSet> {
        match statement {
            Statement::Select(select) => self.execute_select(select),
            Statement::Insert(insert) => self.execute_insert(insert),
            Statement::Update(update) => self.execute_update(update),
            Statement::Delete(delete) => self.execute_delete(delete),
            Statement::CreateTable(create) => self.execute_create_table(create),
            Statement::DropTable(drop) => self.execute_drop_table(drop),
            Statement::Begin => self.begin_transaction(),
            Statement::Commit => self.commit_transaction(),
            Statement::Rollback => self.rollback_transaction(),
        }
    }

    /// Execute SELECT statement
    pub fn execute_select(&mut self, select: SelectStatement) -> Result<ResultSet> {
        let table_name = &select.table;
        let schema = self.get_table_schema(table_name)?;
        
        // Determine selected columns
        let selected_columns = if select.columns.contains(&"*".to_string()) {
            schema.columns.iter().map(|col| col.name.clone()).collect()
        } else {
            select.columns
        };

        // Scan table with filter
        let table_prefix = format!("{}:", table_name);
        let start_key = table_prefix.as_bytes().to_vec();
        let end_key = format!("{}~", table_name).as_bytes().to_vec();
        
        let mut rows = Vec::new();
        let scan_results: Vec<_> = self.transaction.scan(start_key..end_key)?.collect();
        
        for (_, value) in scan_results {
            // Deserialize row using native format
            match self.storage_format.deserialize_row(&value, &schema) {
                Ok(row_data) => {
                    // Apply WHERE clause filter if present
                    let matches = if let Some(ref where_clause) = select.where_clause {
                        self.evaluate_condition(&where_clause.condition, &row_data)
                    } else {
                        true
                    };
                    
                    if matches {
                        // Extract selected column values
                        let mut row_values = Vec::new();
                        for col_name in &selected_columns {
                            row_values.push(
                                row_data.get(col_name).cloned().unwrap_or(SqlValue::Null)
                            );
                        }
                        rows.push(row_values);
                    }
                }
                Err(_) => continue, // Skip rows that can't be deserialized
            }
        }

        // Apply LIMIT if present
        if let Some(limit) = select.limit {
            rows.truncate(limit as usize);
        }

        Ok(ResultSet::Select {
            columns: selected_columns,
            rows,
        })
    }

    /// Execute INSERT statement
    pub fn execute_insert(&mut self, insert: InsertStatement) -> Result<ResultSet> {
        let schema = self.get_table_schema(&insert.table)?;
        let mut rows_affected = 0;

        // Convert column-oriented data to row-oriented
        let rows = self.convert_insert_to_rows(&insert)?;

        for row_data in &rows {
            // Validate row data
            self.validate_row_data(&insert.table, row_data, &schema)?;
            
            // Build primary key
            let key = self.build_primary_key(&insert.table, row_data, &schema)?;
            
            // Check for primary key conflicts
            if self.transaction.get(key.as_bytes()).is_some() {
                return Err(Error::Other(format!(
                    "Primary key constraint violation for table '{}'", insert.table
                )));
            }
            
            // Serialize and store row
            let serialized = self.storage_format.serialize_row(row_data, &schema)?;
            self.transaction.set(key.as_bytes(), serialized)?;
            rows_affected += 1;
        }

        Ok(ResultSet::Insert { rows_affected })
    }

    /// Execute UPDATE statement
    pub fn execute_update(&mut self, update: UpdateStatement) -> Result<ResultSet> {
        let schema = self.get_table_schema(&update.table)?;
        let table_prefix = format!("{}:", update.table);
        let start_key = table_prefix.as_bytes().to_vec();
        let end_key = format!("{}~", update.table).as_bytes().to_vec();
        
        let mut rows_affected = 0;
        let scan_results: Vec<_> = self.transaction.scan(start_key..end_key)?.collect();
        
        for (key, value) in scan_results {
            // Deserialize row
            if let Ok(mut row_data) = self.storage_format.deserialize_row(&value, &schema) {
                // Check WHERE clause
                let matches = if let Some(ref where_clause) = update.where_clause {
                    self.evaluate_condition(&where_clause.condition, &row_data)
                } else {
                    true
                };
                
                if matches {
                    // Apply assignments
                    for assignment in &update.assignments {
                        row_data.insert(assignment.column.clone(), assignment.value.clone());
                    }
                    
                    // Validate updated row (exclude current row from UNIQUE checks)
                    let key_str = String::from_utf8_lossy(&key);
                    let exclude_key = Some(key_str.as_ref());
                    self.validate_row_data_excluding(&update.table, &row_data, &schema, exclude_key)?;
                    
                    // Serialize and update
                    let serialized = self.storage_format.serialize_row(&row_data, &schema)?;
                    self.transaction.set(&key, serialized)?;
                    rows_affected += 1;
                }
            }
        }

        Ok(ResultSet::Update { rows_affected })
    }

    /// Execute DELETE statement
    pub fn execute_delete(&mut self, delete: DeleteStatement) -> Result<ResultSet> {
        let schema = self.get_table_schema(&delete.table)?;
        let table_prefix = format!("{}:", delete.table);
        let start_key = table_prefix.as_bytes().to_vec();
        let end_key = format!("{}~", delete.table).as_bytes().to_vec();
        
        let mut rows_affected = 0;
        let scan_results: Vec<_> = self.transaction.scan(start_key..end_key)?.collect();
        
        for (key, value) in scan_results {
            // Deserialize row to check WHERE clause
            if let Ok(row_data) = self.storage_format.deserialize_row(&value, &schema) {
                let matches = if let Some(ref where_clause) = delete.where_clause {
                    self.evaluate_condition(&where_clause.condition, &row_data)
                } else {
                    true
                };
                
                if matches {
                    self.transaction.delete(&key)?;
                    rows_affected += 1;
                }
            }
        }

        Ok(ResultSet::Delete { rows_affected })
    }

    /// Execute CREATE TABLE statement
    pub fn execute_create_table(&mut self, create: CreateTableStatement) -> Result<ResultSet> {
        // Convert to internal schema format
        let columns: Vec<ColumnInfo> = create.columns.iter().map(|col| ColumnInfo {
            name: col.name.clone(),
            data_type: col.data_type.clone(),
            constraints: col.constraints.clone(),
        }).collect();
        
        let schema = TableSchema {
            name: create.table.clone(),
            columns,
        };
        
        // Store schema metadata (use simple string serialization for now)
        let schema_key = format!("__schema__:{}", create.table);
        let schema_data = create.columns.iter()
            .map(|col| {
                let constraints_str = col.constraints.iter()
                    .map(|c| match c {
                        crate::parser::ColumnConstraint::PrimaryKey => "PRIMARY_KEY",
                        crate::parser::ColumnConstraint::NotNull => "NOT_NULL",
                        crate::parser::ColumnConstraint::Unique => "UNIQUE",
                    })
                    .collect::<Vec<_>>()
                    .join(",");
                
                if constraints_str.is_empty() {
                    format!("{}:{:?}", col.name, col.data_type)
                } else {
                    format!("{}:{:?}:{}", col.name, col.data_type, constraints_str)
                }
            })
            .collect::<Vec<_>>()
            .join("|");
        self.transaction.set(schema_key.as_bytes(), schema_data.as_bytes().to_vec())?;
        
        // Update local schema cache
        self.table_schemas.insert(create.table, schema);
        
        Ok(ResultSet::CreateTable)
    }

    /// Execute DROP TABLE statement
    pub fn execute_drop_table(&mut self, drop: DropTableStatement) -> Result<ResultSet> {
        // Check if table exists
        let table_existed = self.table_schemas.contains_key(&drop.table);
        
        if !drop.if_exists && !table_existed {
            return Err(Error::Other(format!("Table '{}' does not exist", drop.table)));
        }
        
        if table_existed {
            // Delete schema metadata
            let schema_key = format!("__schema__:{}", drop.table);
            self.transaction.delete(schema_key.as_bytes())?;
            
            // Delete all table data
            let table_prefix = format!("{}:", drop.table);
            let start_key = table_prefix.as_bytes().to_vec();
            let end_key = format!("{}~", drop.table).as_bytes().to_vec();
            
            let keys_to_delete: Vec<_> = self.transaction.scan(start_key..end_key)?
                .map(|(key, _)| key)
                .collect();
                
            for key in keys_to_delete {
                self.transaction.delete(&key)?;
            }
            
            // Remove from local schema cache
            self.table_schemas.remove(&drop.table);
        }
        
        Ok(ResultSet::DropTable {
            table_name: drop.table,
            existed: table_existed,
        })
    }

    /// Begin transaction
    pub fn begin_transaction(&mut self) -> Result<ResultSet> {
        if self.transaction_active {
            return Err(Error::Other("Transaction already active. Nested transactions are not supported.".to_string()));
        }
        
        self.transaction_active = true;
        Ok(ResultSet::Begin)
    }

    /// Commit transaction
    pub fn commit_transaction(&mut self) -> Result<ResultSet> {
        if !self.transaction_active {
            return Err(Error::Other("No active transaction to commit".to_string()));
        }
        
        self.transaction_active = false;
        Ok(ResultSet::Commit)
    }

    /// Rollback transaction
    pub fn rollback_transaction(&mut self) -> Result<ResultSet> {
        if !self.transaction_active {
            return Err(Error::Other("No active transaction to rollback".to_string()));
        }
        
        self.transaction_active = false;
        Ok(ResultSet::Rollback)
    }

    /// Execute a query execution plan
    pub fn execute_plan(&mut self, plan: crate::planner::ExecutionPlan) -> Result<ResultSet> {
        use crate::planner::ExecutionPlan;
        
        match plan {
            ExecutionPlan::PrimaryKeyLookup { table, pk_values, selected_columns, additional_filter } => {
                self.execute_primary_key_lookup(&table, &pk_values, &selected_columns, additional_filter.as_ref())
            }
            ExecutionPlan::TableScan { table, selected_columns, filter, limit, early_termination: _ } => {
                self.execute_table_scan(&table, &selected_columns, filter.as_ref(), limit)
            }
            ExecutionPlan::Insert { table, rows, conflict_resolution: _ } => {
                self.execute_insert_plan(&table, &rows)
            }
            ExecutionPlan::Update { table, assignments, scan_plan } => {
                self.execute_update_plan(&table, &assignments, *scan_plan)
            }
            ExecutionPlan::Delete { table, scan_plan } => {
                self.execute_delete_plan(&table, *scan_plan)
            }
            ExecutionPlan::CreateTable { table, schema } => {
                self.execute_create_table_plan(&table, &schema)
            }
            ExecutionPlan::DropTable { table, if_exists } => {
                self.execute_drop_table_plan(&table, if_exists)
            }
            ExecutionPlan::Begin => {
                self.begin_transaction()
            }
            ExecutionPlan::Commit => {
                self.commit_transaction()
            }
            ExecutionPlan::Rollback => {
                self.rollback_transaction()
            }
            // For any other plans, return an error for now
            _ => {
                Err(Error::Other("Unsupported execution plan".to_string()))
            }
        }
    }

    /// Execute primary key lookup plan
    fn execute_primary_key_lookup(
        &mut self,
        table: &str,
        pk_values: &HashMap<String, SqlValue>,
        selected_columns: &[String],
        additional_filter: Option<&Condition>
    ) -> Result<ResultSet> {
        // Get table schema
        let schema = self.get_table_schema(table)?;
        
        // Build primary key
        let key = self.build_primary_key(table, pk_values, &schema)?;
        
        // Get the row
        if let Some(value) = self.transaction.get(key.as_bytes()) {
            // Deserialize row
            if let Ok(row_data) = self.storage_format.deserialize_row(&value, &schema) {
                // Apply additional filter if present
                let matches = if let Some(filter) = additional_filter {
                    self.evaluate_condition(filter, &row_data)
                } else {
                    true
                };
                
                if matches {
                    // Extract selected columns
                    let mut row_values = Vec::new();
                    for col_name in selected_columns {
                        row_values.push(
                            row_data.get(col_name).cloned().unwrap_or(SqlValue::Null)
                        );
                    }
                    
                    return Ok(ResultSet::Select {
                        columns: selected_columns.to_vec(),
                        rows: vec![row_values],
                    });
                }
            }
        }
        
        // No matching row found
        Ok(ResultSet::Select {
            columns: selected_columns.to_vec(),
            rows: vec![],
        })
    }

    /// Execute table scan plan
    fn execute_table_scan(
        &mut self,
        table: &str,
        selected_columns: &[String],
        filter: Option<&Condition>,
        limit: Option<u64>
    ) -> Result<ResultSet> {
        let schema = self.get_table_schema(table)?;
        let table_prefix = format!("{}:", table);
        let start_key = table_prefix.as_bytes().to_vec();
        let end_key = format!("{}~", table).as_bytes().to_vec();
        
        let mut rows = Vec::new();
        let scan_results: Vec<_> = self.transaction.scan(start_key..end_key)?.collect();
        
        for (_, value) in scan_results {
            if let Ok(row_data) = self.storage_format.deserialize_row(&value, &schema) {
                // Apply filter if present
                let matches = if let Some(condition) = filter {
                    self.evaluate_condition(condition, &row_data)
                } else {
                    true
                };
                
                if matches {
                    // Extract selected columns
                    let mut row_values = Vec::new();
                    for col_name in selected_columns {
                        row_values.push(
                            row_data.get(col_name).cloned().unwrap_or(SqlValue::Null)
                        );
                    }
                    rows.push(row_values);
                    
                    // Apply limit if present
                    if let Some(limit_val) = limit {
                        if rows.len() >= limit_val as usize {
                            break;
                        }
                    }
                }
            }
        }
        
        Ok(ResultSet::Select {
            columns: selected_columns.to_vec(),
            rows,
        })
    }

    /// Execute insert plan
    fn execute_insert_plan(&mut self, table: &str, rows: &[HashMap<String, SqlValue>]) -> Result<ResultSet> {
        let schema = self.get_table_schema(table)?;
        let mut rows_affected = 0;
        
        for row_data in rows {
            // Validate row data
            self.validate_row_data(table, row_data, &schema)?;
            
            // Build primary key
            let key = self.build_primary_key(table, row_data, &schema)?;
            
            // Check for primary key conflicts
            if self.transaction.get(key.as_bytes()).is_some() {
                return Err(Error::Other(format!(
                    "Primary key constraint violation for table '{}'", table
                )));
            }
            
            // Serialize and store row
            let serialized = self.storage_format.serialize_row(row_data, &schema)?;
            self.transaction.set(key.as_bytes(), serialized)?;
            rows_affected += 1;
        }
        
        Ok(ResultSet::Insert { rows_affected })
    }

    /// Execute update plan
    fn execute_update_plan(
        &mut self,
        table: &str,
        assignments: &[crate::planner::Assignment],
        scan_plan: crate::planner::ExecutionPlan
    ) -> Result<ResultSet> {
        // First execute the scan plan to find rows to update
        let scan_result = self.execute_plan(scan_plan)?;
        
        match scan_result {
            ResultSet::Select { columns, rows } => {
                let schema = self.get_table_schema(table)?;
                let mut rows_affected = 0;
                
                for row_values in rows {
                    // Reconstruct row data from scan result
                    let mut row_data = HashMap::new();
                    for (i, col_name) in columns.iter().enumerate() {
                        if let Some(value) = row_values.get(i) {
                            row_data.insert(col_name.clone(), value.clone());
                        }
                    }
                    
                    // Apply assignments
                    let mut updated_row = row_data.clone();
                    for assignment in assignments {
                        updated_row.insert(assignment.column.clone(), assignment.value.clone());
                    }
                    
                    // Validate updated row (exclude current row from UNIQUE checks)
                    let original_key = self.build_primary_key(table, &row_data, &schema)?;
                    let exclude_key = Some(original_key.as_str());
                    self.validate_row_data_excluding(table, &updated_row, &schema, exclude_key)?;
                    
                    // Build original key and update
                    let original_key = self.build_primary_key(table, &row_data, &schema)?;
                    let serialized = self.storage_format.serialize_row(&updated_row, &schema)?;
                    self.transaction.set(original_key.as_bytes(), serialized)?;
                    rows_affected += 1;
                }
                
                Ok(ResultSet::Update { rows_affected })
            }
            _ => Err(Error::Other("Invalid scan result for update".to_string())),
        }
    }

    /// Execute delete plan
    fn execute_delete_plan(
        &mut self,
        table: &str,
        scan_plan: crate::planner::ExecutionPlan
    ) -> Result<ResultSet> {
        // First execute the scan plan to find rows to delete
        let scan_result = self.execute_plan(scan_plan)?;
        
        match scan_result {
            ResultSet::Select { columns, rows } => {
                let schema = self.get_table_schema(table)?;
                let mut rows_affected = 0;
                
                for row_values in rows {
                    // Reconstruct row data from scan result
                    let mut row_data = HashMap::new();
                    for (i, col_name) in columns.iter().enumerate() {
                        if let Some(value) = row_values.get(i) {
                            row_data.insert(col_name.clone(), value.clone());
                        }
                    }
                    
                    // Build key and delete
                    let row_key = self.build_primary_key(table, &row_data, &schema)?;
                    self.transaction.delete(row_key.as_bytes())?;
                    rows_affected += 1;
                }
                
                Ok(ResultSet::Delete { rows_affected })
            }
            _ => Err(Error::Other("Invalid scan result for delete".to_string())),
        }
    }

    /// Execute create table plan
    fn execute_create_table_plan(&mut self, table: &str, schema: &TableSchema) -> Result<ResultSet> {
        // Convert to CreateTableStatement format
        use crate::parser::{CreateTableStatement, ColumnDefinition};
        
        let create_stmt = CreateTableStatement {
            table: table.to_string(),
            columns: schema.columns.iter().map(|col| ColumnDefinition {
                name: col.name.clone(),
                data_type: col.data_type.clone(),
                constraints: col.constraints.clone(),
            }).collect(),
        };
        
        self.execute_create_table(create_stmt)
    }

    /// Execute drop table plan
    fn execute_drop_table_plan(&mut self, table: &str, if_exists: bool) -> Result<ResultSet> {
        use crate::parser::DropTableStatement;
        
        let drop_stmt = DropTableStatement {
            table: table.to_string(),
            if_exists,
        };
        
        self.execute_drop_table(drop_stmt)
    }

    /// Load table schemas from storage
    fn load_schemas_from_storage(&mut self) -> Result<()> {
        // Scan for all schema keys
        let schema_prefix = "__schema__:".as_bytes().to_vec();
        let schema_end = "__schema__~".as_bytes().to_vec();
        
        let scan_results: Vec<_> = self.transaction.scan(schema_prefix..schema_end)?.collect();
        
        for (key, value) in scan_results {
            let key_str = String::from_utf8_lossy(&key);
            if let Some(table_name) = key_str.strip_prefix("__schema__:") {
                // Only load from storage if we don't already have this schema
                if !self.table_schemas.contains_key(table_name) {
                    // Parse the simple schema format we're using
                    if let Ok(schema_data) = String::from_utf8(value.to_vec()) {
                        if let Some(schema) = self.parse_schema_data(table_name, &schema_data) {
                            self.table_schemas.insert(table_name.to_string(), schema);
                        }
                    }
                }
            }
        }
        
        Ok(())
    }
    
    /// Parse schema data from our simple string format
    fn parse_schema_data(&self, table_name: &str, schema_data: &str) -> Option<TableSchema> {
        // Format: "col1:DataType:constraints|col2:DataType:constraints|..."
        let mut columns = Vec::new();
        
        for column_part in schema_data.split('|') {
            if column_part.is_empty() {
                continue;
            }

            let components: Vec<&str> = column_part.splitn(3, ':').collect();
            if components.len() >= 2 {
                let column_name = components[0].to_string();
                let data_type_str = components[1];
                let constraints_str = if components.len() > 2 { components[2] } else { "" };

                let data_type = match data_type_str {
                    "Integer" => crate::parser::DataType::Integer,
                    "Text" => crate::parser::DataType::Text,
                    "Real" => crate::parser::DataType::Real,
                    "Blob" => crate::parser::DataType::Blob,
                    // Also accept uppercase for backward compatibility
                    "INTEGER" => crate::parser::DataType::Integer,
                    "TEXT" => crate::parser::DataType::Text,
                    "REAL" => crate::parser::DataType::Real,
                    "BLOB" => crate::parser::DataType::Blob,
                    _ => continue, // Skip unknown types
                };

                let constraints = if constraints_str.is_empty() {
                    Vec::new()
                } else {
                    constraints_str
                        .split(',')
                        .filter_map(|c| match c {
                            "PRIMARY_KEY" => Some(crate::parser::ColumnConstraint::PrimaryKey),
                            "NOT_NULL" => Some(crate::parser::ColumnConstraint::NotNull),
                            "UNIQUE" => Some(crate::parser::ColumnConstraint::Unique),
                            _ => None,
                        })
                        .collect()
                };

                columns.push(ColumnInfo {
                    name: column_name,
                    data_type,
                    constraints,
                });
            }
        }

        if columns.is_empty() {
            None
        } else {
            Some(TableSchema {
                name: table_name.to_string(),
                columns,
            })
        }
    }

    /// Get table schema
    fn get_table_schema(&self, table_name: &str) -> Result<TableSchema> {
        self.table_schemas.get(table_name)
            .cloned()
            .ok_or_else(|| Error::Other(format!("Table '{}' not found", table_name)))
    }

    /// Validate row data against schema
    fn validate_row_data(
        &self,
        table_name: &str,
        row_data: &HashMap<String, SqlValue>,
        schema: &TableSchema
    ) -> Result<()> {
        // Check for unknown columns
        let valid_columns: std::collections::HashSet<_> = schema.columns.iter().map(|col| &col.name).collect();
        for column_name in row_data.keys() {
            if !valid_columns.contains(column_name) {
                return Err(Error::Other(format!(
                    "Unknown column '{}' for table '{}'", 
                    column_name, table_name
                )));
            }
        }
        
        // Check required columns
        for column in &schema.columns {
            if column.constraints.contains(&ColumnConstraint::NotNull) ||
               column.constraints.contains(&ColumnConstraint::PrimaryKey) {
                if !row_data.contains_key(&column.name) {
                    return Err(Error::Other(format!(
                        "Missing required column '{}' for table '{}'", 
                        column.name, table_name
                    )));
                }
                
                if row_data.get(&column.name) == Some(&SqlValue::Null) {
                    return Err(Error::Other(format!(
                        "Column '{}' cannot be NULL", column.name
                    )));
                }
            }
        }
        
        // Check UNIQUE constraints
        for column in &schema.columns {
            if column.constraints.contains(&ColumnConstraint::Unique) {
                if let Some(value) = row_data.get(&column.name) {
                    if value != &SqlValue::Null {
                        // Check if this value already exists in the table
                        if self.check_unique_constraint_violation(table_name, &column.name, value, schema)? {
                            return Err(Error::Other(format!(
                                "UNIQUE constraint violation for column '{}' in table '{}'", 
                                column.name, table_name
                            )));
                        }
                    }
                }
            }
        }
        
        Ok(())
    }
    
    /// Validate row data with option to exclude a specific primary key from UNIQUE checks
    fn validate_row_data_excluding(
        &self,
        table_name: &str,
        row_data: &HashMap<String, SqlValue>,
        schema: &TableSchema,
        exclude_key: Option<&str>
    ) -> Result<()> {
        // Check for unknown columns
        let valid_columns: std::collections::HashSet<_> = schema.columns.iter().map(|col| &col.name).collect();
        for column_name in row_data.keys() {
            if !valid_columns.contains(column_name) {
                return Err(Error::Other(format!(
                    "Unknown column '{}' for table '{}'", 
                    column_name, table_name
                )));
            }
        }
        
        // Check required columns
        for column in &schema.columns {
            if column.constraints.contains(&ColumnConstraint::NotNull) ||
               column.constraints.contains(&ColumnConstraint::PrimaryKey) {
                if !row_data.contains_key(&column.name) {
                    return Err(Error::Other(format!(
                        "Missing required column '{}' for table '{}'", 
                        column.name, table_name
                    )));
                }
                
                if row_data.get(&column.name) == Some(&SqlValue::Null) {
                    return Err(Error::Other(format!(
                        "Column '{}' cannot be NULL", column.name
                    )));
                }
            }
        }
        
        // Check UNIQUE constraints
        for column in &schema.columns {
            if column.constraints.contains(&ColumnConstraint::Unique) {
                if let Some(value) = row_data.get(&column.name) {
                    if value != &SqlValue::Null {
                        // Check if this value already exists in the table (excluding current row)
                        if self.check_unique_constraint_violation_excluding(table_name, &column.name, value, schema, exclude_key)? {
                            return Err(Error::Other(format!(
                                "UNIQUE constraint violation for column '{}' in table '{}'", 
                                column.name, table_name
                            )));
                        }
                    }
                }
            }
        }
        
        Ok(())
    }

    /// Check if a value violates a UNIQUE constraint by scanning existing data
    fn check_unique_constraint_violation(
        &self,
        table_name: &str,
        column_name: &str,
        value: &SqlValue,
        schema: &TableSchema
    ) -> Result<bool> {
        self.check_unique_constraint_violation_excluding(table_name, column_name, value, schema, None)
    }
    
    /// Check if a value violates a UNIQUE constraint, optionally excluding a specific key
    fn check_unique_constraint_violation_excluding(
        &self,
        table_name: &str,
        column_name: &str,
        value: &SqlValue,
        schema: &TableSchema,
        exclude_key: Option<&str>
    ) -> Result<bool> {
        let table_prefix = format!("{}:", table_name);
        let start_key = table_prefix.as_bytes().to_vec();
        let end_key = format!("{}~", table_name).as_bytes().to_vec();
        
        let scan_results: Vec<_> = self.transaction.scan(start_key..end_key)?.collect();
        
        for (key, stored_value) in scan_results {
            // If we have a key to exclude, skip it
            if let Some(exclude_key_str) = exclude_key {
                let key_str = String::from_utf8_lossy(&key);
                if key_str == exclude_key_str {
                    continue;
                }
            }
            
            if let Ok(row_data) = self.storage_format.deserialize_row(&stored_value, schema) {
                if let Some(existing_value) = row_data.get(column_name) {
                    if existing_value == value {
                        return Ok(true); // Violation found
                    }
                }
            }
        }
        
        Ok(false) // No violation
    }

    /// Build primary key string for a row
    fn build_primary_key(
        &self,
        table_name: &str,
        row_data: &HashMap<String, SqlValue>,
        schema: &TableSchema
    ) -> Result<String> {
        // Find primary key columns
        let pk_columns: Vec<_> = schema.columns.iter()
            .filter(|col| col.constraints.contains(&ColumnConstraint::PrimaryKey))
            .collect();
            
        if pk_columns.is_empty() {
            return Err(Error::Other(format!(
                "Table '{}' has no primary key", table_name
            )));
        }
        
        let mut key_parts = Vec::new();
        for pk_col in pk_columns {
            if let Some(value) = row_data.get(&pk_col.name) {
                key_parts.push(self.value_to_key_string(value));
            } else {
                return Err(Error::Other(format!(
                    "Missing primary key value for column '{}'", pk_col.name
                )));
            }
        }
        
        Ok(format!("{}:{}", table_name, key_parts.join(":")))
    }

    /// Convert SqlValue to key string representation
    fn value_to_key_string(&self, value: &SqlValue) -> String {
        match value {
            SqlValue::Integer(i) => i.to_string(),
            SqlValue::Real(r) => r.to_string(),
            SqlValue::Text(t) => t.clone(),
            SqlValue::Null => "NULL".to_string(),
        }
    }

    /// Evaluate condition against row data
    fn evaluate_condition(&self, condition: &Condition, row_data: &HashMap<String, SqlValue>) -> bool {
        match condition {
            Condition::Comparison { left, operator, right } => {
                let row_value = row_data.get(left).unwrap_or(&SqlValue::Null);
                self.compare_values(row_value, operator, right)
            }
            Condition::And(left, right) => {
                self.evaluate_condition(left, row_data) && self.evaluate_condition(right, row_data)
            }
            Condition::Or(left, right) => {
                self.evaluate_condition(left, row_data) || self.evaluate_condition(right, row_data)
            }
        }
    }

    /// Compare two SqlValues using the given operator
    fn compare_values(&self, left: &SqlValue, operator: &ComparisonOperator, right: &SqlValue) -> bool {
        use ComparisonOperator::*;
        
        match operator {
            Equal => left == right,
            NotEqual => left != right,
            LessThan => match (left, right) {
                (SqlValue::Integer(a), SqlValue::Integer(b)) => a < b,
                (SqlValue::Real(a), SqlValue::Real(b)) => a < b,
                (SqlValue::Integer(a), SqlValue::Real(b)) => (*a as f64) < *b,
                (SqlValue::Real(a), SqlValue::Integer(b)) => *a < (*b as f64),
                (SqlValue::Text(a), SqlValue::Text(b)) => a < b,
                _ => false,
            },
            LessThanOrEqual => match (left, right) {
                (SqlValue::Integer(a), SqlValue::Integer(b)) => a <= b,
                (SqlValue::Real(a), SqlValue::Real(b)) => a <= b,
                (SqlValue::Integer(a), SqlValue::Real(b)) => (*a as f64) <= *b,
                (SqlValue::Real(a), SqlValue::Integer(b)) => *a <= (*b as f64),
                (SqlValue::Text(a), SqlValue::Text(b)) => a <= b,   
                _ => false,
            },
            GreaterThan => match (left, right) {
                (SqlValue::Integer(a), SqlValue::Integer(b)) => a > b,
                (SqlValue::Real(a), SqlValue::Real(b)) => a > b,
                (SqlValue::Integer(a), SqlValue::Real(b)) => (*a as f64) > *b,
                (SqlValue::Real(a), SqlValue::Integer(b)) => *a > (*b as f64),
                (SqlValue::Text(a), SqlValue::Text(b)) => a > b,
                _ => false,
            },
            GreaterThanOrEqual => match (left, right) {
                (SqlValue::Integer(a), SqlValue::Integer(b)) => a >= b,
                (SqlValue::Real(a), SqlValue::Real(b)) => a >= b,
                (SqlValue::Integer(a), SqlValue::Real(b)) => (*a as f64) >= *b,
                (SqlValue::Real(a), SqlValue::Integer(b)) => *a >= (*b as f64),
                (SqlValue::Text(a), SqlValue::Text(b)) => a >= b,
                _ => false,
            },
            Like => {
                // Simple LIKE implementation - just checking if right is substring of left
                match (left, right) {
                    (SqlValue::Text(a), SqlValue::Text(b)) => {
                        // Simple pattern matching - convert SQL LIKE to contains for now
                        let pattern = b.replace('%', "");
                        a.contains(&pattern)
                    }
                    _ => false,
                }
            }
        }
    }

    /// Convert INSERT statement to row-oriented format
    fn convert_insert_to_rows(&self, insert: &InsertStatement) -> Result<Vec<HashMap<String, SqlValue>>> {
        let mut rows = Vec::new();
        
        for value_row in &insert.values {
            if value_row.len() != insert.columns.len() {
                return Err(Error::Other(format!(
                    "Column count mismatch: expected {}, got {}", 
                    insert.columns.len(), 
                    value_row.len()
                )));
            }
            
            let mut row_data = HashMap::new();
            for (i, column_name) in insert.columns.iter().enumerate() {
                row_data.insert(column_name.clone(), value_row[i].clone());
            }
            rows.push(row_data);
        }
        
        Ok(rows)
    }
}

impl ResultSet {
    /// Get the number of rows for SELECT results
    pub fn row_count(&self) -> usize {
        match self {
            ResultSet::Select { rows, .. } => rows.len(),
            ResultSet::Insert { rows_affected } => *rows_affected,
            ResultSet::Update { rows_affected } => *rows_affected,
            ResultSet::Delete { rows_affected } => *rows_affected,
            _ => 0,
        }
    }

    /// Check if this is a SELECT result
    pub fn is_select(&self) -> bool {
        matches!(self, ResultSet::Select { .. })
    }

    /// Get column names for SELECT results
    pub fn columns(&self) -> Option<&[String]> {
        match self {
            ResultSet::Select { columns, .. } => Some(columns),
            _ => None,
        }
    }

    /// Get rows for SELECT results
    pub fn rows(&self) -> Option<&[Vec<SqlValue>]> {
        match self {
            ResultSet::Select { rows, .. } => Some(rows),
            _ => None,
        }
    }
}
