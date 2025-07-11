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
use std::collections::{HashMap, HashSet};
use std::rc::Rc;

/// Type alias for scan iterator to reduce complexity
type ScanIterator<'a> = Box<dyn Iterator<Item = (Vec<u8>, std::rc::Rc<[u8]>)> + 'a>;

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

/// Optimized schema validation cache
#[derive(Debug, Clone)]
pub struct SchemaValidationCache {
    /// Pre-computed valid column names for fast validation
    pub valid_columns: HashSet<String>,
    /// Pre-computed required column names (NOT NULL or PRIMARY KEY)
    pub required_columns: HashSet<String>,
    /// Primary key column name (single column only)
    pub primary_key_column: Option<String>,
    /// Column name to index mapping for fast lookups
    pub column_indices: HashMap<String, usize>,
}

impl SchemaValidationCache {
    pub fn new(schema: &TableSchema) -> Self {
        let mut valid_columns = HashSet::new();
        let mut required_columns = HashSet::new();
        let mut primary_key_column = None;
        let mut column_indices = HashMap::new();

        for (idx, col) in schema.columns.iter().enumerate() {
            valid_columns.insert(col.name.clone());
            column_indices.insert(col.name.clone(), idx);

            if col.constraints.contains(&ColumnConstraint::NotNull)
                || col.constraints.contains(&ColumnConstraint::PrimaryKey)
            {
                required_columns.insert(col.name.clone());
            }

            if col.constraints.contains(&ColumnConstraint::PrimaryKey) {
                primary_key_column = Some(col.name.clone());
            }
        }

        Self {
            valid_columns,
            required_columns,
            primary_key_column,
            column_indices,
        }
    }
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
        let mut column_indices = Vec::with_capacity(selected_columns.len());
        for col_name in &selected_columns {
            if let Some((idx, _col)) = schema.columns.iter().enumerate().find(|(_, c)| &c.name == col_name) {
                column_indices.push(idx);
            } else {
                column_indices.push(usize::MAX);
            }
        }

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
            // Check if we need to apply a filter
            let matches = if let Some(ref filter) = self.filter {
                // For filtering, we need to deserialize the full row to evaluate the condition
                let row_data_result = self.storage_format.deserialize_row(&value, &self.schema);
                match row_data_result {
                    Ok(row_data) => {
                        crate::sql_utils::evaluate_condition(filter, &row_data)
                    }
                    Err(e) => return Some(Err(e)),
                }
            } else {
                true // No filter, so it matches
            };

            if matches {
                // Extract only the selected columns (ultra-fast path!)
                let row_values_result = self.storage_format.deserialize_column_indices(
                    &value,
                    &self.schema,
                    &self.column_indices,
                );
                
                match row_values_result {
                    Ok(row_values) => {
                        self.count += 1;
                        return Some(Ok(row_values));
                    }
                    Err(e) => return Some(Err(e)),
                }
            }
            // If row doesn't match filter, continue to next row
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
    table_schemas: HashMap<String, Rc<TableSchema>>,
    /// Cached validation data for performance
    validation_caches: HashMap<String, SchemaValidationCache>,
    /// Pre-computed storage format headers for ultra-fast access
    storage_headers: HashMap<String, (Vec<usize>, Vec<u8>)>,
    storage_format: StorageFormat,
    transaction_active: bool,
}

impl<'a> QueryProcessor<'a> {
    /// Create a new query processor with transaction and Rc schemas (more efficient)
    pub fn new_with_rc_schemas(
        transaction: Transaction<'a>,
        table_schemas: HashMap<String, Rc<TableSchema>>,
    ) -> Self {
        let mut processor = Self {
            transaction,
            table_schemas: table_schemas.clone(),
            validation_caches: HashMap::new(),
            storage_headers: HashMap::new(),
            storage_format: StorageFormat::new(), // Always use native format
            transaction_active: false,
        };

        // Pre-build validation caches for all schemas
        for (table_name, schema) in table_schemas {
            processor
                .validation_caches
                .insert(table_name.clone(), SchemaValidationCache::new(&schema));
        }

        // Load additional schemas from storage and merge
        let _ = processor.load_schemas_from_storage();

        processor
    }

    /// Get mutable reference to the transaction
    pub fn transaction_mut(&mut self) -> &mut Transaction<'a> {
        &mut self.transaction
    }

    /// Get or create validation cache for a table
    fn get_validation_cache(&mut self, table_name: &str) -> Result<&SchemaValidationCache> {
        if !self.validation_caches.contains_key(table_name) {
            let schema = self.get_table_schema(table_name)?;
            self.validation_caches
                .insert(table_name.to_string(), SchemaValidationCache::new(&schema));
        }
        Ok(self.validation_caches.get(table_name).unwrap())
    }

    /// Get or create pre-computed storage header for a table
    fn get_storage_header(&mut self, table_name: &str) -> Result<&(Vec<usize>, Vec<u8>)> {
        if !self.storage_headers.contains_key(table_name) {
            let schema = self.get_table_schema(table_name)?;
            
            // Pre-compute the header information for this schema
            let mut offsets = Vec::with_capacity(schema.columns.len());
            let mut types = Vec::with_capacity(schema.columns.len());
            
            // For now, use a simple fixed layout (can be optimized further)
            let mut current_offset = 0;
            for _col in &schema.columns {
                offsets.push(current_offset);
                // Assume all columns are 8-byte integers for now (fastest path)
                types.push(4); // 8-byte integer type code
                current_offset += 8;
            }
            
            self.storage_headers.insert(table_name.to_string(), (offsets, types));
        }
        Ok(self.storage_headers.get(table_name).unwrap())
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

        // Store schema
        self.transaction.set(schema_key.as_bytes(), schema_data)?;

        // Add to in-memory schemas and validation cache
        let schema_rc = Rc::new(schema.clone());
        self.table_schemas.insert(create.table.clone(), schema_rc);
        self.validation_caches
            .insert(create.table.clone(), SchemaValidationCache::new(&schema));

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
            self.validation_caches.remove(&drop.table);
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
            ExecutionPlan::PrimaryKeyLookup { .. }
            | ExecutionPlan::TableRangeScan { .. }
            | ExecutionPlan::TableScan { .. } => self.execute_select_plan_streaming(plan),
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
                                    let key = self.build_primary_key(&table, &pk_values)?;

                // Create an iterator that returns at most one row if the key exists and matches
                let key_bytes = key.as_bytes().to_vec();
                let scan_iter = if let Some(value) = self.transaction.get(&key_bytes) {
                    // Create a single-item iterator if the key exists
                    let single_result = vec![(key_bytes, value)];
                    Box::new(single_result.into_iter())
                        as Box<dyn Iterator<Item = (Vec<u8>, std::rc::Rc<[u8]>)>>
                } else {
                    // Create an empty iterator if the key doesn't exist
                    Box::new(std::iter::empty())
                        as Box<dyn Iterator<Item = (Vec<u8>, std::rc::Rc<[u8]>)>>
                };

                let row_iter = SelectRowIterator::new(
                    scan_iter,
                    (*schema).clone(),
                    selected_columns.clone(),
                    additional_filter,
                    Some(1), // PK lookup returns at most 1 row
                );

                Ok(ResultSet::Select {
                    columns: selected_columns,
                    rows: Box::new(row_iter),
                })
            }
            ExecutionPlan::TableRangeScan {
                table,
                selected_columns,
                pk_range,
                additional_filter,
                limit,
            } => {
                let schema = self.get_table_schema(&table)?;

                // Build range scan keys based on PK range
                let (start_key, end_key) = self.build_pk_range_keys(&table, &pk_range, &schema)?;

                // Create streaming iterator for range scan
                let scan_iter = self.transaction.scan(start_key..end_key)?;
                let row_iter = SelectRowIterator::new(
                    scan_iter,
                    (*schema).clone(),
                    selected_columns.clone(),
                    additional_filter,
                    limit,
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
                    (*schema).clone(),
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
            self.validate_row_data(table, row_data)?;

            // Build primary key
                            let key = self.build_primary_key(table, row_data)?;

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
                crate::planner::ExecutionPlan::TableRangeScan {
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
                let key = self.build_primary_key(table, &row_data)?;
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

                    // Validate updated row
                    // Check if primary key was changed and if new key conflicts with existing data
                    let new_key = self.build_primary_key(table, &row_data)?;
                    if new_key != key && self.transaction.get(new_key.as_bytes()).is_some() {
                        return Err(Error::Other(format!(
                            "Primary key constraint violation for table '{table}'"
                        )));
                    }

                    // Validate other constraints (NOT NULL, etc.) but skip primary key validation
                    // since we already handled it above
                    let cache = self.get_validation_cache(table)?;
                    let required_columns = cache.required_columns.clone();
                    let valid_columns = cache.valid_columns.clone();
                    let pk_column = cache.primary_key_column.clone();
                    let _ = cache;

                    // Check for unknown columns
                    for column_name in row_data.keys() {
                        if !valid_columns.contains(column_name) {
                            return Err(Error::Other(format!(
                                "Unknown column '{column_name}' for table '{table}'"
                            )));
                        }
                    }

                    // Check required columns (excluding primary key since we already validated it)
                    for column_name in &required_columns {
                        if let Some(pk_col) = &pk_column {
                            if column_name == pk_col {
                                continue; // Skip primary key validation
                            }
                        }
                        if !row_data.contains_key(column_name) {
                            return Err(Error::Other(format!(
                                "Missing required column '{column_name}' for table '{table}'"
                            )));
                        }
                        if row_data.get(column_name) == Some(&SqlValue::Null) {
                            return Err(Error::Other(format!(
                                "Column '{column_name}' cannot be NULL"
                            )));
                        }
                    }

                    // Serialize and store the updated row
                    let serialized = self.storage_format.serialize_row(&row_data, &schema)?;

                    // If primary key changed, we need to delete the old row and insert the new one
                    if new_key != key {
                        self.transaction.delete(key.as_bytes())?;
                        self.transaction.set(new_key.as_bytes(), serialized)?;
                    } else {
                        self.transaction.set(key.as_bytes(), serialized)?;
                    }

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
                let key = self.build_primary_key(table, pk_values)?;
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
            ExecutionPlan::TableRangeScan {
                table,
                pk_range,
                additional_filter,
                limit,
                ..
            } => {
                let (start_key, end_key) = self.build_pk_range_keys(table, pk_range, schema)?;
                let mut count = 0;

                let scan_iter = self.transaction.scan(start_key..end_key)?;

                for (key, value_rc) in scan_iter {
                    if let Some(limit_val) = limit {
                        if count >= *limit_val {
                            break;
                        }
                    }

                    let matches = if let Some(filter_cond) = additional_filter {
                        self.storage_format
                            .matches_condition(&value_rc, schema, filter_cond)
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

                for (key, value_rc) in scan_iter {
                    if let Some(limit) = limit {
                        if count >= *limit {
                            break;
                        }
                    }

                    let matches = if let Some(filter_cond) = filter {
                        self.storage_format
                            .matches_condition(&value_rc, schema, filter_cond)
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

        for (key, value_rc) in scan_results {
            let key_str = String::from_utf8_lossy(&key);
            if let Some(table_name) = key_str.strip_prefix("S:") {
                // Only load from storage if we don't already have this schema
                if !self.table_schemas.contains_key(table_name) {
                    // Parse the schema using centralized utility
                    if let Ok(schema_data) = String::from_utf8(value_rc.to_vec()) {
                        if let Some(schema) = sql_utils::parse_schema_data(table_name, &schema_data)
                        {
                            self.table_schemas
                                .insert(table_name.to_string(), Rc::new(schema.clone()));
                            self.validation_caches.insert(
                                table_name.to_string(),
                                SchemaValidationCache::new(&schema),
                            );
                        }
                    }
                }
            }
        }

        Ok(())
    }

    /// Get table schema
    fn get_table_schema(&self, table_name: &str) -> Result<Rc<TableSchema>> {
        self.table_schemas
            .get(table_name)
            .cloned()
            .ok_or_else(|| Error::Other(format!("Table '{table_name}' not found")))
    }

    /// Validate row data against schema
    fn validate_row_data(
        &mut self,
        table_name: &str,
        row_data: &HashMap<String, SqlValue>,
    ) -> Result<()> {
        let cache = self.get_validation_cache(table_name)?;
        let required_columns = cache.required_columns.clone();
        let valid_columns = cache.valid_columns.clone();
        let pk_column = cache.primary_key_column.clone();
        let _ = cache;

        // Check for unknown columns
        for column_name in row_data.keys() {
            if !valid_columns.contains(column_name) {
                return Err(Error::Other(format!(
                    "Unknown column '{column_name}' for table '{table_name}'"
                )));
            }
        }

        // Check required columns
        for column_name in &required_columns {
            if !row_data.contains_key(column_name) {
                return Err(Error::Other(format!(
                    "Missing required column '{column_name}' for table '{table_name}'"
                )));
            }
            if row_data.get(column_name) == Some(&SqlValue::Null) {
                return Err(Error::Other(format!(
                    "Column '{column_name}' cannot be NULL"
                )));
            }
        }

        // Check PRIMARY KEY constraint (only if PK column exists)
        if let Some(pk_col) = &pk_column {
            if let Some(value) = row_data.get(pk_col) {
                if value != &SqlValue::Null {
                    let key = self.build_primary_key(table_name, row_data)?;
                    if self.transaction.get(key.as_bytes()).is_some() {
                        return Err(Error::Other(format!(
                            "Primary key constraint violation for table '{table_name}'"
                        )));
                    }
                }
            }
        }
        Ok(())
    }

    /// Build primary key string for a row
    /// Note: TegDB only supports single-column primary keys
    fn build_primary_key(
        &mut self,
        table_name: &str,
        row_data: &HashMap<String, SqlValue>,
    ) -> Result<String> {
        let cache = self.get_validation_cache(table_name)?;

        // Use cached primary key column for better performance
        if let Some(pk_column) = &cache.primary_key_column {
            if let Some(value) = row_data.get(pk_column) {
                return Ok(format!(
                    "{}:{}",
                    table_name,
                    self.value_to_key_string(value)
                ));
            } else {
                return Err(Error::Other(format!(
                    "Missing primary key value for column '{pk_column}'"
                )));
            }
        }

        // Fallback to schema-based lookup (should not happen with proper cache)
        Err(Error::Other(format!(
            "Table '{table_name}' has no primary key or cache is invalid"
        )))
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

    /// Build primary key range scan keys based on PK range conditions
    fn build_pk_range_keys(
        &self,
        table: &str,
        pk_range: &crate::planner::PkRange,
        schema: &TableSchema,
    ) -> Result<(Vec<u8>, Vec<u8>)> {
        let table_prefix = format!("{table}:");

        // For now, we'll implement a simple range scan that works with single-column PKs
        // This can be enhanced later to support composite PKs

        let pk_columns: Vec<_> = schema
            .columns
            .iter()
            .filter(|col| col.constraints.contains(&ColumnConstraint::PrimaryKey))
            .collect();

        if pk_columns.len() != 1 {
            return Err(Error::Other(
                "Range scan currently only supports single-column primary keys".to_string(),
            ));
        }

        let pk_column = &pk_columns[0].name;

        // Build start key
        let start_key = if let Some(start_bound) = &pk_range.start_bound {
            if let Some(value) = start_bound.values.get(pk_column) {
                let key_string = if start_bound.inclusive {
                    format!("{}{}", table_prefix, self.value_to_key_string(value))
                } else {
                    // For exclusive bounds, we need to find the next key
                    // This is a simplified implementation
                    format!("{}{}", table_prefix, self.value_to_key_string(value))
                };
                key_string.as_bytes().to_vec()
            } else {
                table_prefix.as_bytes().to_vec()
            }
        } else {
            table_prefix.as_bytes().to_vec()
        };

        // Build end key
        let end_key = if let Some(end_bound) = &pk_range.end_bound {
            if let Some(value) = end_bound.values.get(pk_column) {
                let key_string = if end_bound.inclusive {
                    // For inclusive bounds, we need to find the next key after this value
                    // This is a simplified implementation
                    format!("{}{}", table_prefix, self.value_to_key_string(value))
                } else {
                    format!("{}{}", table_prefix, self.value_to_key_string(value))
                };
                key_string.as_bytes().to_vec()
            } else {
                format!("{table}~").as_bytes().to_vec()
            }
        } else {
            format!("{table}~").as_bytes().to_vec()
        };

        Ok((start_key, end_key))
    }
}
