//! Modern query processor for TegDB with native row format support
//!
//! This module provides the core query execution engine that works directly with the
//! native binary row format for optimal performance.

use crate::parser::{
    ColumnConstraint, Condition, CreateTableStatement, DataType, DropTableStatement, SqlValue,
};
use crate::sql_utils;
use crate::storage_engine::Transaction;
use crate::storage_format::StorageFormat;
use crate::{Error, Result};
use std::collections::HashMap;

/// Type alias for scan iterator to reduce complexity
type ScanIterator<'a> = Box<dyn Iterator<Item = (Vec<u8>, std::sync::Arc<[u8]>)> + 'a>;

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

/// Streaming iterator for SELECT query results
/// This provides a streaming interface that yields rows on-demand
pub struct SelectRowIterator<'a> {
    /// Iterator over the scan results
    scan_iter: ScanIterator<'a>,
    /// Schema for deserializing rows
    schema: TableSchema,
    /// Columns to select
    selected_columns: Vec<String>,
    /// Pre-computed column indices for faster access
    column_indices: Vec<usize>,
    /// Optional filter condition
    filter: Option<Condition>,
    /// Storage format for deserialization
    storage_format: StorageFormat,
    /// Optional limit on number of rows
    limit: Option<u64>,
    /// Current count of yielded rows
    count: u64,
}

impl<'a> SelectRowIterator<'a> {
    /// Create a new select row iterator
    pub fn new(
        scan_iter: ScanIterator<'a>,
        schema: TableSchema,
        selected_columns: Vec<String>,
        filter: Option<Condition>,
        limit: Option<u64>,
    ) -> Self {
        // Pre-compute column indices for faster row extraction
        let column_indices: Vec<usize> = selected_columns
            .iter()
            .map(|col_name| {
                schema
                    .columns
                    .iter()
                    .position(|col| &col.name == col_name)
                    .unwrap_or(usize::MAX) // Will be handled in next()
            })
            .collect();

        Self {
            scan_iter,
            schema,
            selected_columns,
            column_indices,
            filter,
            storage_format: StorageFormat::new(),
            limit,
            count: 0,
        }
    }

    /// Collect all remaining rows into a Vec for backward compatibility
    pub fn collect_rows(self) -> Result<Vec<Vec<SqlValue>>> {
        self.collect()
    }
}

impl<'a> Iterator for SelectRowIterator<'a> {
    type Item = Result<Vec<SqlValue>>;

    fn next(&mut self) -> Option<Self::Item> {
        // Check limit
        if let Some(limit) = self.limit {
            if self.count >= limit {
                return None;
            }
        }

        // Process rows until we find one that matches the filter
        for (_, value) in self.scan_iter.by_ref() {
            // Deserialize the row
            match self.storage_format.deserialize_row(&value, &self.schema) {
                Ok(row_data) => {
                    // Apply filter if present
                    let matches = if let Some(ref filter) = self.filter {
                        crate::sql_utils::evaluate_condition(filter, &row_data)
                    } else {
                        true
                    };

                    if matches {
                        // Extract selected columns using pre-computed indices
                        let mut row_values = Vec::with_capacity(self.selected_columns.len());
                        for &col_idx in &self.column_indices {
                            if col_idx != usize::MAX {
                                // Use direct index access for better performance
                                if let Some(col) = self.schema.columns.get(col_idx) {
                                    row_values.push(
                                        row_data.get(&col.name).cloned().unwrap_or(SqlValue::Null),
                                    );
                                } else {
                                    row_values.push(SqlValue::Null);
                                }
                            } else {
                                // Fallback to HashMap lookup for columns not found in schema
                                let col_name = &self.selected_columns[row_values.len()];
                                row_values.push(
                                    row_data.get(col_name).cloned().unwrap_or(SqlValue::Null),
                                );
                            }
                        }

                        self.count += 1;
                        return Some(Ok(row_values));
                    }
                    // If row doesn't match filter, continue to next row
                }
                Err(e) => return Some(Err(e)),
            }
        }

        // No more matching rows found
        None
    }
}

impl<'a> std::fmt::Debug for SelectRowIterator<'a> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SelectRowIterator")
            .field("schema", &self.schema.name)
            .field("selected_columns", &self.selected_columns)
            .field("filter", &self.filter)
            .field("limit", &self.limit)
            .field("count", &self.count)
            .finish()
    }
}

/// Query execution result
#[derive(Debug)]
pub enum ResultSet<'a> {
    /// SELECT query result with streaming support
    Select {
        columns: Vec<String>,
        rows: Box<SelectRowIterator<'a>>,
    },
    /// INSERT query result
    Insert { rows_affected: usize },
    /// UPDATE query result
    Update { rows_affected: usize },
    /// DELETE query result
    Delete { rows_affected: usize },
    /// CREATE TABLE query result
    CreateTable,
    /// DROP TABLE query result
    DropTable,
    /// Transaction BEGIN result
    Begin,
    /// Transaction COMMIT result
    Commit,
    /// Transaction ROLLBACK result
    Rollback,
}

impl<'a> ResultSet<'a> {
    // No methods needed - columns() is provided by QueryResult in database.rs
}

/// SQL query processor with native row format support
pub struct QueryProcessor<'a> {
    transaction: Transaction<'a>,
    table_schemas: HashMap<String, TableSchema>,
    storage_format: StorageFormat,
    transaction_active: bool,
}

impl<'a> QueryProcessor<'a> {
    /// Create a new query processor with transaction and schemas
    pub fn new_with_schemas(
        transaction: Transaction<'a>,
        table_schemas: HashMap<String, TableSchema>,
    ) -> Self {
        let mut processor = Self {
            transaction,
            table_schemas,
            storage_format: StorageFormat::new(), // Always use native format
            transaction_active: false,
        };

        // Load additional schemas from storage and merge
        let _ = processor.load_schemas_from_storage();

        processor
    }

    /// Get mutable reference to the transaction
    pub fn transaction_mut(&mut self) -> &mut Transaction<'a> {
        &mut self.transaction
    }

    /// Execute CREATE TABLE statement
    pub fn execute_create_table(&mut self, create: CreateTableStatement) -> Result<ResultSet<'_>> {
        // Validate that we don't have composite primary keys
        let pk_count = create
            .columns
            .iter()
            .filter(|col| col.constraints.contains(&ColumnConstraint::PrimaryKey))
            .count();

        if pk_count > 1 {
            return Err(Error::Other(format!(
                "Table '{}' has composite primary key, but TegDB only supports single-column primary keys", 
                create.table
            )));
        }

        if pk_count == 0 {
            return Err(Error::Other(format!(
                "Table '{}' must have exactly one primary key column",
                create.table
            )));
        }

        // Convert to internal schema format
        let columns: Vec<ColumnInfo> = create
            .columns
            .iter()
            .map(|col| ColumnInfo {
                name: col.name.clone(),
                data_type: col.data_type.clone(),
                constraints: col.constraints.clone(),
            })
            .collect();

        let schema = TableSchema {
            name: create.table.clone(),
            columns,
        };

        // Store schema metadata (use simple string serialization for now)
        let schema_key = format!("S:{}", create.table);

        // Optimized schema serialization to reduce allocations
        let mut schema_data = Vec::new();
        for (i, col) in create.columns.iter().enumerate() {
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
        self.transaction.set(schema_key.as_bytes(), schema_data)?;

        // Update local schema cache
        self.table_schemas.insert(create.table, schema);

        Ok(ResultSet::CreateTable)
    }

    /// Execute DROP TABLE statement
    pub fn execute_drop_table(&mut self, drop: DropTableStatement) -> Result<ResultSet<'_>> {
        // Check if table exists
        let table_existed = self.table_schemas.contains_key(&drop.table);

        if !drop.if_exists && !table_existed {
            return Err(Error::Other(format!(
                "Table '{}' does not exist",
                drop.table
            )));
        }

        if table_existed {
            // Delete schema metadata
            let schema_key = format!("S:{}", drop.table);
            self.transaction.delete(schema_key.as_bytes())?;

            // Delete all table data
            let table_prefix = format!("{}:", drop.table);
            let start_key = table_prefix.as_bytes().to_vec();
            let end_key = format!("{}~", drop.table).as_bytes().to_vec();

            let keys_to_delete: Vec<_> = self
                .transaction
                .scan(start_key..end_key)?
                .map(|(key, _)| key)
                .collect();

            for key in keys_to_delete {
                self.transaction.delete(&key)?;
            }

            // Remove from local schema cache
            self.table_schemas.remove(&drop.table);
        }

        Ok(ResultSet::DropTable)
    }

    /// Begin transaction
    pub fn begin_transaction(&mut self) -> Result<ResultSet<'_>> {
        if self.transaction_active {
            return Err(Error::Other(
                "Transaction already active. Nested transactions are not supported.".to_string(),
            ));
        }

        self.transaction_active = true;
        Ok(ResultSet::Begin)
    }

    /// Commit transaction
    pub fn commit_transaction(&mut self) -> Result<ResultSet<'_>> {
        if !self.transaction_active {
            return Err(Error::Other("No active transaction to commit".to_string()));
        }

        self.transaction_active = false;
        Ok(ResultSet::Commit)
    }

    /// Rollback transaction
    pub fn rollback_transaction(&mut self) -> Result<ResultSet<'_>> {
        if !self.transaction_active {
            return Err(Error::Other(
                "No active transaction to rollback".to_string(),
            ));
        }

        self.transaction_active = false;
        Ok(ResultSet::Rollback)
    }

    /// Execute a query execution plan
    pub fn execute_plan(&mut self, plan: crate::planner::ExecutionPlan) -> Result<ResultSet<'_>> {
        use crate::planner::ExecutionPlan;

        match plan {
            // For SELECT operations, use streaming execution and collect results
            ExecutionPlan::PrimaryKeyLookup { .. } | ExecutionPlan::TableScan { .. } => {
                self.execute_select_plan_streaming(plan)
            }
            // Non-SELECT operations remain the same
            ExecutionPlan::Insert {
                table,
                rows,
                conflict_resolution: _,
            } => self.execute_insert_plan(&table, &rows),
            ExecutionPlan::Update {
                table,
                assignments,
                scan_plan,
            } => self.execute_update_plan(&table, &assignments, *scan_plan),
            ExecutionPlan::Delete { table, scan_plan } => {
                self.execute_delete_plan(&table, *scan_plan)
            }
            ExecutionPlan::CreateTable { table, schema } => {
                self.execute_create_table_plan(&table, &schema)
            }
            ExecutionPlan::DropTable { table, if_exists } => {
                self.execute_drop_table_plan(&table, if_exists)
            }
            ExecutionPlan::Begin => self.begin_transaction(),
            ExecutionPlan::Commit => self.commit_transaction(),
            ExecutionPlan::Rollback => self.rollback_transaction(),
        }
    }

    /// Execute SELECT plans using streaming and collect results
    /// This eliminates duplicate code by using a single streaming implementation
    fn execute_select_plan_streaming(
        &mut self,
        plan: crate::planner::ExecutionPlan,
    ) -> Result<ResultSet<'_>> {
        use crate::planner::ExecutionPlan;

        match plan {
            ExecutionPlan::PrimaryKeyLookup {
                table,
                pk_values,
                selected_columns,
                additional_filter,
            } => {
                let schema = self.get_table_schema(&table)?;
                let key = self.build_primary_key(&table, &pk_values, &schema)?;

                // Create an iterator that returns at most one row if the key exists and matches
                let key_bytes = key.as_bytes().to_vec();
                let scan_iter = if let Some(value) = self.transaction.get(&key_bytes) {
                    // Create a single-item iterator if the key exists
                    let single_result = vec![(key_bytes, value)];
                    Box::new(single_result.into_iter())
                        as Box<dyn Iterator<Item = (Vec<u8>, std::sync::Arc<[u8]>)>>
                } else {
                    // Create an empty iterator if the key doesn't exist
                    Box::new(std::iter::empty())
                        as Box<dyn Iterator<Item = (Vec<u8>, std::sync::Arc<[u8]>)>>
                };

                let row_iter = SelectRowIterator::new(
                    scan_iter,
                    schema,
                    selected_columns.clone(),
                    additional_filter,
                    Some(1), // PK lookup returns at most 1 row
                );

                Ok(ResultSet::Select {
                    columns: selected_columns,
                    rows: Box::new(row_iter),
                })
            }
            ExecutionPlan::TableScan {
                table,
                selected_columns,
                filter,
                limit,
                ..
            } => {
                let schema = self.get_table_schema(&table)?;
                let table_prefix = format!("{table}:");
                let start_key = table_prefix.as_bytes().to_vec();
                let end_key = format!("{table}~").as_bytes().to_vec();

                // Create streaming iterator for table scan
                let scan_iter = self.transaction.scan(start_key..end_key)?;
                let row_iter = SelectRowIterator::new(
                    scan_iter,
                    schema,
                    selected_columns.clone(),
                    filter,
                    limit,
                );

                Ok(ResultSet::Select {
                    columns: selected_columns,
                    rows: Box::new(row_iter),
                })
            }
            _ => Err(Error::Other("Expected SELECT execution plan".to_string())),
        }
    }

    /// Execute insert plan
    fn execute_insert_plan(
        &mut self,
        table: &str,
        rows: &[HashMap<String, SqlValue>],
    ) -> Result<ResultSet<'_>> {
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
                    "Primary key constraint violation for table '{table}'"
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
        scan_plan: crate::planner::ExecutionPlan,
    ) -> Result<ResultSet<'_>> {
        let schema = self.get_table_schema(table)?;
        let mut rows_affected = 0;

        // We need to collect the keys first because the scan iterator will borrow the transaction,
        // and we can't borrow it mutably inside the loop to perform the update.
        let keys_to_update = {
            // Extract columns before consuming the plan
            let columns = match &scan_plan {
                crate::planner::ExecutionPlan::PrimaryKeyLookup {
                    selected_columns, ..
                } => selected_columns.clone(),
                crate::planner::ExecutionPlan::TableScan {
                    selected_columns, ..
                } => selected_columns.clone(),
                _ => return Err(Error::Other("Unsupported scan plan for update".to_string())),
            };

            // Get the plan results and materialize immediately to avoid lifetime conflicts
            let materialized_rows = self.execute_plan_materialized(scan_plan)?;

            let mut keys = Vec::new();
            for row_values in materialized_rows {
                let mut row_data = HashMap::new();
                for (i, col_name) in columns.iter().enumerate() {
                    if let Some(value) = row_values.get(i) {
                        row_data.insert(col_name.clone(), value.clone());
                    }
                }
                let key = self.build_primary_key(table, &row_data, &schema)?;
                keys.push(key);
            }
            keys
        };

        for key in keys_to_update {
            if let Some(value) = self.transaction.get(key.as_bytes()) {
                if let Ok(old_row_data) = self.storage_format.deserialize_row(&value, &schema) {
                    let mut row_data = old_row_data.clone();

                    // Apply assignments
                    for assignment in assignments {
                        let new_value = assignment.value.evaluate(&row_data).map_err(|e| {
                            crate::Error::Other(format!("Expression evaluation error: {e}"))
                        })?;
                        row_data.insert(assignment.column.clone(), new_value);
                    }

                    // Validate updated row (exclude current row from UNIQUE checks)
                    let original_key = self.build_primary_key(table, &row_data, &schema)?;
                    let exclude_key = Some(original_key.as_str());
                    self.validate_row_data_excluding(table, &row_data, &schema, exclude_key)?;

                    // Serialize and store the updated row
                    let serialized = self.storage_format.serialize_row(&row_data, &schema)?;
                    self.transaction.set(key.as_bytes(), serialized)?;

                    rows_affected += 1;
                }
            }
        }

        Ok(ResultSet::Update { rows_affected })
    }

    /// Execute delete plan
    fn execute_delete_plan(
        &mut self,
        table: &str,
        scan_plan: crate::planner::ExecutionPlan,
    ) -> Result<ResultSet<'_>> {
        let schema = self.get_table_schema(table)?;

        // This approach avoids collecting all full rows in memory first.
        // It scans, collects keys, and then deletes.
        let keys_to_delete = self.execute_scan_and_collect_keys(&scan_plan, &schema)?;
        let rows_affected = keys_to_delete.len();

        for key in &keys_to_delete {
            self.transaction.delete(key.as_bytes())?;
        }

        Ok(ResultSet::Delete { rows_affected })
    }

    /// Execute create table plan
    fn execute_create_table_plan(
        &mut self,
        table: &str,
        schema: &TableSchema,
    ) -> Result<ResultSet<'_>> {
        // Convert to CreateTableStatement format
        use crate::parser::{ColumnDefinition, CreateTableStatement};

        let create_stmt = CreateTableStatement {
            table: table.to_string(),
            columns: schema
                .columns
                .iter()
                .map(|col| ColumnDefinition {
                    name: col.name.clone(),
                    data_type: col.data_type.clone(),
                    constraints: col.constraints.clone(),
                })
                .collect(),
        };

        self.execute_create_table(create_stmt)
    }

    /// Execute drop table plan
    fn execute_drop_table_plan(&mut self, table: &str, if_exists: bool) -> Result<ResultSet<'_>> {
        use crate::parser::DropTableStatement;

        let drop_stmt = DropTableStatement {
            table: table.to_string(),
            if_exists,
        };

        self.execute_drop_table(drop_stmt)
    }

    /// Helper function to execute a scan plan and collect the primary keys of the resulting rows.
    /// This is more memory-efficient than collecting the full rows.
    fn execute_scan_and_collect_keys(
        &mut self,
        scan_plan: &crate::planner::ExecutionPlan,
        schema: &TableSchema,
    ) -> Result<Vec<String>> {
        use crate::planner::ExecutionPlan;
        let mut keys = Vec::new();

        match scan_plan {
            ExecutionPlan::PrimaryKeyLookup {
                table,
                pk_values,
                additional_filter,
                ..
            } => {
                let key = self.build_primary_key(table, pk_values, schema)?;
                if let Some(value) = self.transaction.get(key.as_bytes()) {
                    let matches = if let Some(filter) = additional_filter {
                        if let Ok(row_data) = self.storage_format.deserialize_row(&value, schema) {
                            self.evaluate_condition(filter, &row_data)
                        } else {
                            false
                        }
                    } else {
                        true
                    };

                    if matches {
                        keys.push(key);
                    }
                }
            }
            ExecutionPlan::TableScan {
                table,
                filter,
                limit,
                ..
            } => {
                let table_prefix = format!("{table}:");
                let start_key = table_prefix.as_bytes().to_vec();
                let end_key = format!("{table}~").as_bytes().to_vec();
                let mut count = 0;

                let scan_iter = self.transaction.scan(start_key..end_key)?;

                for (key, value_arc) in scan_iter {
                    if let Some(limit) = limit {
                        if count >= *limit {
                            break;
                        }
                    }

                    let matches = if let Some(filter_cond) = filter {
                        self.storage_format
                            .matches_condition(&value_arc, schema, filter_cond)
                            .unwrap_or(false)
                    } else {
                        true
                    };

                    if matches {
                        if let Ok(key_str) = String::from_utf8(key) {
                            keys.push(key_str);
                            count += 1;
                        }
                    }
                }
            }
            _ => {
                return Err(crate::Error::Other(
                    "Unsupported scan plan for key collection".to_string(),
                ))
            }
        }
        Ok(keys)
    }

    /// Load table schemas from storage
    fn load_schemas_from_storage(&mut self) -> Result<()> {
        // Scan for all schema keys
        let schema_prefix = "S:".as_bytes().to_vec();
        let schema_end = "S~".as_bytes().to_vec();

        let scan_results: Vec<_> = self.transaction.scan(schema_prefix..schema_end)?.collect();

        for (key, value_arc) in scan_results {
            let key_str = String::from_utf8_lossy(&key);
            if let Some(table_name) = key_str.strip_prefix("S:") {
                // Only load from storage if we don't already have this schema
                if !self.table_schemas.contains_key(table_name) {
                    // Parse the schema using centralized utility
                    if let Ok(schema_data) = String::from_utf8(value_arc.to_vec()) {
                        if let Some(schema) = sql_utils::parse_schema_data(table_name, &schema_data)
                        {
                            self.table_schemas.insert(table_name.to_string(), schema);
                        }
                    }
                }
            }
        }

        Ok(())
    }

    /// Get table schema
    fn get_table_schema(&self, table_name: &str) -> Result<TableSchema> {
        self.table_schemas
            .get(table_name)
            .cloned()
            .ok_or_else(|| Error::Other(format!("Table '{table_name}' not found")))
    }

    /// Validate row data against schema
    fn validate_row_data(
        &self,
        table_name: &str,
        row_data: &HashMap<String, SqlValue>,
        schema: &TableSchema,
    ) -> Result<()> {
        // Check for unknown columns
        let valid_columns: std::collections::HashSet<_> =
            schema.columns.iter().map(|col| &col.name).collect();
        for column_name in row_data.keys() {
            if !valid_columns.contains(column_name) {
                return Err(Error::Other(format!(
                    "Unknown column '{column_name}' for table '{table_name}'"
                )));
            }
        }

        // Check required columns
        for column in &schema.columns {
            if column.constraints.contains(&ColumnConstraint::NotNull)
                || column.constraints.contains(&ColumnConstraint::PrimaryKey)
            {
                if !row_data.contains_key(&column.name) {
                    return Err(Error::Other(format!(
                        "Missing required column '{}' for table '{}'",
                        column.name, table_name
                    )));
                }

                if row_data.get(&column.name) == Some(&SqlValue::Null) {
                    return Err(Error::Other(format!(
                        "Column '{}' cannot be NULL",
                        column.name
                    )));
                }
            }
        }

        // Check UNIQUE constraints
        for column in &schema.columns {
            if column.constraints.contains(&ColumnConstraint::Unique) {
                if let Some(value) = row_data.get(&column.name) {
                    if value != &SqlValue::Null {
                        // Check if this value already exists in the table using table scan
                        if self.check_unique_constraint_violation_table_scan(
                            table_name,
                            &column.name,
                            value,
                            schema,
                        )? {
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
        exclude_key: Option<&str>,
    ) -> Result<()> {
        // Check for unknown columns
        let valid_columns: std::collections::HashSet<_> =
            schema.columns.iter().map(|col| &col.name).collect();
        for column_name in row_data.keys() {
            if !valid_columns.contains(column_name) {
                return Err(Error::Other(format!(
                    "Unknown column '{column_name}' for table '{table_name}'"
                )));
            }
        }

        // Check required columns
        for column in &schema.columns {
            if column.constraints.contains(&ColumnConstraint::NotNull)
                || column.constraints.contains(&ColumnConstraint::PrimaryKey)
            {
                if !row_data.contains_key(&column.name) {
                    return Err(Error::Other(format!(
                        "Missing required column '{}' for table '{}'",
                        column.name, table_name
                    )));
                }

                if row_data.get(&column.name) == Some(&SqlValue::Null) {
                    return Err(Error::Other(format!(
                        "Column '{}' cannot be NULL",
                        column.name
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
                        if self.check_unique_constraint_violation_table_scan_excluding(
                            table_name,
                            &column.name,
                            value,
                            schema,
                            exclude_key,
                        )? {
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
    fn check_unique_constraint_violation_table_scan(
        &self,
        table_name: &str,
        column_name: &str,
        value: &SqlValue,
        schema: &TableSchema,
    ) -> Result<bool> {
        self.check_unique_constraint_violation_table_scan_excluding(
            table_name,
            column_name,
            value,
            schema,
            None,
        )
    }

    /// Check if a value violates a UNIQUE constraint using table scan, optionally excluding a specific key
    fn check_unique_constraint_violation_table_scan_excluding(
        &self,
        table_name: &str,
        column_name: &str,
        value: &SqlValue,
        schema: &TableSchema,
        exclude_key: Option<&str>,
    ) -> Result<bool> {
        // Use table scan for unique constraint checking - O(n) operation
        let table_prefix = format!("{table_name}:");
        let start_key = table_prefix.as_bytes().to_vec();
        let end_key = format!("{table_name}~").as_bytes().to_vec();

        let scan_iter = self.transaction.scan(start_key..end_key)?;

        for (key, value_bytes_arc) in scan_iter {
            let key_str = String::from_utf8_lossy(&key);

            // If we have a key to exclude, check if this is the same key
            if let Some(exclude_key_str) = exclude_key {
                if key_str == exclude_key_str {
                    continue; // Skip the row being updated
                }
            }

            // Deserialize the row and check the column value
            if let Ok(row_data) = self
                .storage_format
                .deserialize_row(&value_bytes_arc, schema)
            {
                if let Some(existing_value) = row_data.get(column_name) {
                    if existing_value == value && existing_value != &SqlValue::Null {
                        return Ok(true); // Violation found - value exists for a different primary key
                    }
                }
            }
        }

        Ok(false) // No violation - value doesn't exist in table
    }

    /// Build primary key string for a row
    /// Note: TegDB only supports single-column primary keys
    fn build_primary_key(
        &self,
        table_name: &str,
        row_data: &HashMap<String, SqlValue>,
        schema: &TableSchema,
    ) -> Result<String> {
        // Find primary key columns
        let pk_columns: Vec<_> = schema
            .columns
            .iter()
            .filter(|col| col.constraints.contains(&ColumnConstraint::PrimaryKey))
            .collect();

        if pk_columns.is_empty() {
            return Err(Error::Other(format!(
                "Table '{table_name}' has no primary key"
            )));
        }

        // TegDB only supports single-column primary keys
        if pk_columns.len() > 1 {
            return Err(Error::Other(format!(
                "Table '{table_name}' has composite primary key, but TegDB only supports single-column primary keys"
            )));
        }

        let pk_col = &pk_columns[0];
        if let Some(value) = row_data.get(&pk_col.name) {
            Ok(format!(
                "{}:{}",
                table_name,
                self.value_to_key_string(value)
            ))
        } else {
            Err(Error::Other(format!(
                "Missing primary key value for column '{}'",
                pk_col.name
            )))
        }
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
    fn evaluate_condition(
        &self,
        condition: &Condition,
        row_data: &HashMap<String, SqlValue>,
    ) -> bool {
        crate::sql_utils::evaluate_condition(condition, row_data)
    }

    /// Execute a plan and immediately materialize SELECT results for internal use
    /// This is used by UPDATE/DELETE operations that need to collect keys
    fn execute_plan_materialized(
        &mut self,
        plan: crate::planner::ExecutionPlan,
    ) -> Result<Vec<Vec<SqlValue>>> {
        let result = self.execute_plan(plan)?;
        match result {
            ResultSet::Select { rows, .. } => rows.collect_rows(),
            _ => Err(Error::Other(
                "Expected SELECT result for materialization".to_string(),
            )),
        }
    }
}
