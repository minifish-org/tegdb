//! Modern query processor for TegDB with native row format support
//!
//! This module provides the core query execution engine that works directly with the
//! native binary row format for optimal performance.

use crate::parser::{
    ColumnConstraint, Condition, CreateTableStatement, DataType, DropTableStatement, SqlValue,
};

use crate::storage_engine::Transaction;
use crate::storage_format::StorageFormat;
use crate::{Error, Result};
use std::collections::HashMap;
use std::rc::Rc;

/// Type alias for scan iterator to reduce complexity
type ScanIterator<'a> = Box<dyn Iterator<Item = (Vec<u8>, std::rc::Rc<[u8]>)> + 'a>;

/// Native primary key types that avoid string conversion
#[derive(Debug, Clone)]
pub enum NativeKey {
    Integer(i64),
    Real(f64),
    Text(String),
    Null,
}

impl NativeKey {
    /// Convert SqlValue to NativeKey (zero-copy where possible)
    pub fn from_sql_value(value: &SqlValue) -> Result<Self> {
        match value {
            SqlValue::Integer(i) => Ok(NativeKey::Integer(*i)),
            SqlValue::Real(r) => Ok(NativeKey::Real(*r)),
            SqlValue::Text(t) => Ok(NativeKey::Text(t.clone())), // Only clone needed
            SqlValue::Null => Ok(NativeKey::Null),
            SqlValue::Parameter(_) => Err(Error::Other(
                "Parameter placeholder found in key generation - parameter binding failed"
                    .to_string(),
            )),
        }
    }

    /// Convert NativeKey back to SqlValue (zero-copy where possible)
    pub fn to_sql_value(&self) -> SqlValue {
        match self {
            NativeKey::Integer(i) => SqlValue::Integer(*i),
            NativeKey::Real(r) => SqlValue::Real(*r),
            NativeKey::Text(t) => SqlValue::Text(t.clone()), // Only clone needed
            NativeKey::Null => SqlValue::Null,
        }
    }

    /// Serialize to bytes for storage (efficient binary format)
    pub fn to_bytes(&self) -> Vec<u8> {
        match self {
            NativeKey::Integer(i) => {
                let mut bytes = vec![0x01]; // Type tag for Integer
                bytes.extend_from_slice(&i.to_be_bytes()); // Use big-endian for correct ordering
                bytes
            }
            NativeKey::Real(r) => {
                let mut bytes = vec![0x02]; // Type tag for Real
                bytes.extend_from_slice(&r.to_be_bytes()); // Use big-endian for correct ordering
                bytes
            }
            NativeKey::Text(t) => {
                let mut bytes = vec![0x03]; // Type tag for Text
                bytes.extend_from_slice(&(t.len() as u32).to_le_bytes());
                bytes.extend_from_slice(t.as_bytes());
                bytes
            }
            NativeKey::Null => {
                vec![0x00] // Type tag for Null
            }
        }
    }

    /// Deserialize from bytes
    pub fn from_bytes(bytes: &[u8]) -> Result<Self> {
        if bytes.is_empty() {
            return Err(Error::Other("Empty key bytes".to_string()));
        }

        match bytes[0] {
            0x00 => Ok(NativeKey::Null),
            0x01 => {
                if bytes.len() != 9 {
                    return Err(Error::Other("Invalid integer key length".to_string()));
                }
                let i = i64::from_be_bytes(bytes[1..9].try_into().unwrap()); // Use big-endian
                Ok(NativeKey::Integer(i))
            }
            0x02 => {
                if bytes.len() != 9 {
                    return Err(Error::Other("Invalid real key length".to_string()));
                }
                let r = f64::from_be_bytes(bytes[1..9].try_into().unwrap()); // Use big-endian
                Ok(NativeKey::Real(r))
            }
            0x03 => {
                if bytes.len() < 5 {
                    return Err(Error::Other("Invalid text key length".to_string()));
                }
                let len = u32::from_le_bytes(bytes[1..5].try_into().unwrap()) as usize;
                if bytes.len() != 5 + len {
                    return Err(Error::Other("Text key length mismatch".to_string()));
                }
                let text = String::from_utf8(bytes[5..].to_vec())
                    .map_err(|_| Error::Other("Invalid UTF-8 in text key".to_string()))?;
                Ok(NativeKey::Text(text))
            }
            _ => Err(Error::Other("Unknown key type".to_string())),
        }
    }
}

/// High-level primary key that combines table name with native key
#[derive(Debug, Clone)]
pub struct PrimaryKey {
    table_name: String,
    key: NativeKey,
}

impl PrimaryKey {
    /// Create a new primary key
    pub fn new(table_name: String, key: NativeKey) -> Self {
        Self { table_name, key }
    }

    /// Create from table name and SqlValue
    pub fn from_sql_value(table_name: String, value: &SqlValue) -> Result<Self> {
        let native_key = NativeKey::from_sql_value(value)?;
        Ok(Self::new(table_name, native_key))
    }

    /// Get the native key
    pub fn key(&self) -> &NativeKey {
        &self.key
    }

    /// Get the table name
    pub fn table_name(&self) -> &str {
        &self.table_name
    }

    /// Convert to storage bytes (efficient binary format)
    pub fn to_storage_bytes(&self) -> Vec<u8> {
        let mut bytes = Vec::new();

        // Table name prefix with length
        bytes.extend_from_slice(&(self.table_name.len() as u32).to_le_bytes());
        bytes.extend_from_slice(self.table_name.as_bytes());
        bytes.push(b':'); // Separator

        // Native key bytes
        bytes.extend_from_slice(&self.key.to_bytes());

        bytes
    }

    /// Create from storage bytes
    pub fn from_storage_bytes(bytes: &[u8]) -> Result<Self> {
        if bytes.len() < 5 {
            return Err(Error::Other("Invalid primary key bytes".to_string()));
        }

        // Parse table name length
        let table_name_len = u32::from_le_bytes(bytes[0..4].try_into().unwrap()) as usize;

        if bytes.len() < 5 + table_name_len {
            return Err(Error::Other("Primary key bytes too short".to_string()));
        }

        // Parse table name
        let table_name = String::from_utf8(bytes[4..4 + table_name_len].to_vec())
            .map_err(|_| Error::Other("Invalid UTF-8 in table name".to_string()))?;

        // Check separator
        if bytes[4 + table_name_len] != b':' {
            return Err(Error::Other("Invalid primary key separator".to_string()));
        }

        // Parse native key
        let key_bytes = &bytes[5 + table_name_len..];
        let key = NativeKey::from_bytes(key_bytes)?;

        Ok(Self::new(table_name, key))
    }

    /// Create range start key (for range scans)
    pub fn range_start(table_name: &str, start_key: &NativeKey, inclusive: bool) -> Self {
        let mut pk = Self::new(table_name.to_string(), start_key.clone());
        if !inclusive {
            // For exclusive bounds, we need to increment the key
            // This ensures we start after the specified value
            match &mut pk.key {
                NativeKey::Integer(i) => *i += 1,
                NativeKey::Real(r) => *r += f64::EPSILON,
                NativeKey::Text(s) => {
                    // For text, append a character that sorts after the current string
                    s.push('\u{10FFFF}'); // Highest Unicode character
                }
                NativeKey::Null => {
                    // For null, we can't increment, so we'll use a special marker
                    pk.key = NativeKey::Text("".to_string());
                }
            }
        }
        pk
    }

    /// Create range end key (for range scans)
    pub fn range_end(table_name: &str, end_key: &NativeKey, inclusive: bool) -> Self {
        let mut pk = Self::new(table_name.to_string(), end_key.clone());
        match &mut pk.key {
            NativeKey::Integer(i) => {
                if inclusive {
                    *i += 1;
                }
                // else: leave as is for exclusive
            }
            NativeKey::Real(r) => {
                if inclusive {
                    *r = f64::from_bits(r.to_bits() + 1); // next representable float
                }
                // else: leave as is for exclusive
            }
            NativeKey::Text(s) => {
                if inclusive {
                    s.push('\u{10FFFF}');
                }
                // else: leave as is for exclusive
            }
            NativeKey::Null => {
                pk.key = NativeKey::Text("".to_string());
            }
        }
        pk
    }

    /// Create table prefix for full table scans
    pub fn table_prefix(table_name: &str) -> Vec<u8> {
        let mut bytes = Vec::new();
        bytes.extend_from_slice(&(table_name.len() as u32).to_le_bytes());
        bytes.extend_from_slice(table_name.as_bytes());
        bytes.push(b':');
        bytes
    }

    /// Create table end marker for full table scans
    pub fn table_end_marker(table_name: &str) -> Vec<u8> {
        let mut bytes = Vec::new();
        bytes.extend_from_slice(&(table_name.len() as u32).to_le_bytes());
        bytes.extend_from_slice(table_name.as_bytes());
        bytes.push(b'~'); // End marker
        bytes
    }
}

/// Column information for table schema with embedded storage metadata
#[derive(Debug, Clone)]
pub struct ColumnInfo {
    pub name: String,
    pub data_type: DataType,
    pub constraints: Vec<ColumnConstraint>,
    // Embedded storage metadata for ultra-fast access
    pub storage_offset: usize,
    pub storage_size: usize,
    pub storage_type_code: u8,
}

/// Table schema definition
#[derive(Debug, Clone)]
pub struct TableSchema {
    pub name: String,
    pub columns: Vec<ColumnInfo>,
}

/// Optimized schema validation methods
impl TableSchema {
    /// Check if a column exists in this schema (optimized with early return)
    pub fn has_column(&self, column_name: &str) -> bool {
        self.columns.iter().any(|col| col.name == column_name)
    }

    /// Get column index by name (optimized with early return)
    pub fn get_column_index(&self, column_name: &str) -> Option<usize> {
        self.columns.iter().position(|col| col.name == column_name)
    }

    /// Get primary key column name (cached lookup)
    pub fn get_primary_key_column(&self) -> Option<&str> {
        // Use find() which stops at first match
        self.columns
            .iter()
            .find(|col| col.constraints.contains(&ColumnConstraint::PrimaryKey))
            .map(|col| col.name.as_str())
    }

    /// Check if a column is required (NOT NULL or PRIMARY KEY) - optimized
    pub fn is_column_required(&self, column_name: &str) -> bool {
        // Use find() which stops at first match
        self.columns
            .iter()
            .find(|col| col.name == column_name)
            .map(|col| {
                col.constraints.contains(&ColumnConstraint::NotNull)
                    || col.constraints.contains(&ColumnConstraint::PrimaryKey)
            })
            .unwrap_or(false)
    }

    /// Get all column names as a vector
    pub fn get_column_names(&self) -> Vec<&str> {
        self.columns.iter().map(|col| col.name.as_str()).collect()
    }

    /// Get column by name (optimized)
    pub fn get_column(&self, column_name: &str) -> Option<&ColumnInfo> {
        self.columns.iter().find(|col| col.name == column_name)
    }
}

/// Query schema for fast column access
#[derive(Clone)]
pub struct QuerySchema {
    pub column_names: Vec<String>,
    pub column_indices: Vec<usize>,
}

impl QuerySchema {
    pub fn new(selected_columns: &[String], schema: &TableSchema) -> Self {
        let (column_names, column_indices) =
            if selected_columns.len() == 1 && selected_columns[0] == "*" {
                let names: Vec<String> = schema.columns.iter().map(|c| c.name.clone()).collect();
                let indices: Vec<usize> = (0..schema.columns.len()).collect();
                (names, indices)
            } else {
                let mut indices = Vec::with_capacity(selected_columns.len());
                for col_name in selected_columns {
                    if let Some((idx, _col)) = schema
                        .columns
                        .iter()
                        .enumerate()
                        .find(|(_, c)| &c.name == col_name)
                    {
                        indices.push(idx);
                    } else {
                        indices.push(usize::MAX);
                    }
                }
                (selected_columns.to_vec(), indices)
            };
        Self {
            column_names,
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
    schema: std::rc::Rc<TableSchema>,
    /// Query schema for fast column access
    query_schema: QuerySchema,
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
        schema: std::rc::Rc<TableSchema>,
        query_schema: QuerySchema,
        filter: Option<Condition>,
        limit: Option<u64>,
    ) -> Self {
        let storage_format = StorageFormat::new();

        Self {
            scan_iter,
            schema,
            query_schema,
            filter,
            storage_format,
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
                // Use cached metadata for ultra-fast condition evaluation
                match self.storage_format.matches_condition_with_metadata(
                    &value,
                    &self.schema,
                    filter,
                ) {
                    Ok(matches) => matches,
                    Err(_) => {
                        return Some(Err(Error::Other(
                            "Failed to evaluate condition".to_string(),
                        )))
                    }
                }
            } else {
                true // No filter, so it matches
            };

            if matches {
                // Use cached metadata for ultra-fast column access
                let row_values_result = self.storage_format.get_columns_by_indices_with_metadata(
                    &value,
                    &self.schema,
                    &self.query_schema.column_indices,
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
            .field("selected_columns", &self.query_schema.column_names)
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

impl TableSchema {
    // Storage metadata is now embedded in columns, no separate computation needed
}

/// SQL query processor with native row format support
pub struct QueryProcessor<'a> {
    transaction: Transaction<'a>,
    table_schemas: HashMap<String, Rc<TableSchema>>,
    storage_format: StorageFormat,
    transaction_active: bool,
}

impl<'a> QueryProcessor<'a> {
    /// Create a new query processor with transaction and Rc schemas (optimized)
    pub fn new_with_rc_schemas(
        transaction: Transaction<'a>,
        table_schemas: HashMap<String, Rc<TableSchema>>,
    ) -> Self {
        Self {
            transaction,
            table_schemas,
            storage_format: StorageFormat::new(), // Always use native format
            transaction_active: false,
        }
    }

    /// Get mutable reference to the transaction
    pub fn transaction_mut(&mut self) -> &mut Transaction<'a> {
        &mut self.transaction
    }

    /// Get table schema by name
    fn get_table_schema(&self, table_name: &str) -> Result<Rc<TableSchema>> {
        self.table_schemas
            .get(table_name)
            .cloned()
            .ok_or_else(|| Error::Other(format!("Table '{table_name}' does not exist")))
    }

    /// Validate row data against table schema
    fn validate_row_data(
        &self,
        table_name: &str,
        row_data: &HashMap<String, SqlValue>,
    ) -> Result<()> {
        let schema = self.get_table_schema(table_name)?;

        // Check that all provided columns exist
        for column_name in row_data.keys() {
            if !schema.has_column(column_name) {
                return Err(Error::Other(format!(
                    "Column '{column_name}' does not exist in table '{table_name}'"
                )));
            }
        }

        // Check that all required columns are provided
        for col in &schema.columns {
            if schema.is_column_required(&col.name) && !row_data.contains_key(&col.name) {
                return Err(Error::Other(format!(
                    "Required column '{}' is missing for table '{}'",
                    col.name, table_name
                )));
            }
        }

        Ok(())
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
                storage_offset: 0,    // Placeholder, will be set later
                storage_size: 0,      // Placeholder, will be set later
                storage_type_code: 0, // Placeholder, will be set later
            })
            .collect();

        let mut schema = TableSchema {
            name: create.table.clone(),
            columns,
        };
        // Compute storage metadata automatically when adding to catalog
        let _ = crate::catalog::Catalog::compute_table_metadata(&mut schema);

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

        // Compute storage metadata automatically when adding to catalog
        let _ = crate::catalog::Catalog::compute_table_metadata(&mut schema);

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
                pk_value,
                selected_columns,
                additional_filter,
            } => {
                let schema = self.get_table_schema(&table)?;
                let query_schema = QuerySchema::new(&selected_columns, &schema);
                let key = self.build_primary_key_from_value(&table, &pk_value);

                // Create an iterator that returns at most one row if the key exists and matches
                let key_bytes = key.to_storage_bytes();
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
                    schema.clone(),
                    query_schema,
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
                let query_schema = QuerySchema::new(&selected_columns, &schema);

                // Build range scan keys based on PK range
                let (start_key, end_key) = self.build_pk_range_keys(&table, &pk_range, &schema)?;

                // Create streaming iterator for range scan
                let scan_iter = self.transaction.scan(start_key..end_key)?;
                let row_iter = SelectRowIterator::new(
                    scan_iter,
                    schema.clone(),
                    query_schema,
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
                let query_schema = QuerySchema::new(&selected_columns, &schema);
                let start_key = PrimaryKey::table_prefix(&table);
                let end_key = PrimaryKey::table_end_marker(&table);
                // Create streaming iterator for table scan
                let scan_iter = self.transaction.scan(start_key..end_key)?;
                let row_iter = SelectRowIterator::new(
                    scan_iter,
                    schema.clone(),
                    query_schema,
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
            let key = self.build_primary_key_from_value(
                table,
                row_data
                    .get(schema.get_primary_key_column().unwrap())
                    .unwrap(),
            );
            // Check for primary key conflicts
            if self.transaction.get(&key.to_storage_bytes()).is_some() {
                return Err(Error::Other(format!(
                    "Primary key constraint violation for table '{table}'"
                )));
            }

            // Serialize and store row
            let serialized = self.storage_format.serialize_row(row_data, &schema)?;
            self.transaction.set(&key.to_storage_bytes(), serialized)?;

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
                let key = self.build_primary_key_from_value(
                    table,
                    row_data
                        .get(schema.get_primary_key_column().unwrap())
                        .unwrap(),
                );
                keys.push(key);
            }
            keys
        };

        for key in keys_to_update {
            if let Some(value) = self.transaction.get(&key.to_storage_bytes()) {
                if let Ok(old_row_data) = self.storage_format.deserialize_row_full(&value, &schema)
                {
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
                    let new_key = self.build_primary_key_from_value(
                        table,
                        row_data
                            .get(schema.get_primary_key_column().unwrap())
                            .unwrap(),
                    );
                    let new_key_bytes = new_key.to_storage_bytes();
                    let key_bytes = key.to_storage_bytes();
                    if new_key_bytes != key_bytes && self.transaction.get(&new_key_bytes).is_some()
                    {
                        return Err(Error::Other(format!(
                            "Primary key constraint violation for table '{table}'"
                        )));
                    }

                    // Validate other constraints (NOT NULL, etc.) but skip primary key validation
                    // since we already handled it above
                    let _ = self.validate_row_data(table, &row_data);

                    // Serialize and store the updated row
                    let serialized = self.storage_format.serialize_row(&row_data, &schema)?;

                    // If primary key changed, we need to delete the old row and insert the new one
                    if new_key_bytes != key_bytes {
                        self.transaction.delete(&key_bytes)?;
                        self.transaction.set(&new_key_bytes, serialized)?;
                    } else {
                        self.transaction.set(&key_bytes, serialized)?;
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

        for key_bytes in &keys_to_delete {
            self.transaction.delete(key_bytes)?;
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
    ) -> Result<Vec<Vec<u8>>> {
        use crate::planner::ExecutionPlan;
        let mut keys = Vec::new();

        match scan_plan {
            ExecutionPlan::PrimaryKeyLookup {
                table,
                pk_value,
                additional_filter,
                ..
            } => {
                let key = self.build_primary_key_from_value(table, pk_value);
                if let Some(value) = self.transaction.get(&key.to_storage_bytes()) {
                    let matches = if let Some(filter) = additional_filter {
                        self.storage_format
                            .matches_condition(&value, schema, filter)
                            .unwrap_or(false)
                    } else {
                        true
                    };

                    if matches {
                        keys.push(key.to_storage_bytes());
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
                        // Use pre-computed metadata from schema
                        self.storage_format
                            .matches_condition_with_metadata(&value_rc, schema, filter_cond)
                            .unwrap_or(false)
                    } else {
                        true
                    };

                    if matches {
                        keys.push(key);
                        count += 1;
                    }
                }
            }
            ExecutionPlan::TableScan {
                table,
                filter,
                limit,
                ..
            } => {
                let start_key = PrimaryKey::table_prefix(table);
                let end_key = PrimaryKey::table_end_marker(table);
                let mut count = 0;

                let scan_iter = self.transaction.scan(start_key..end_key)?;

                for (key, value_rc) in scan_iter {
                    if let Some(limit) = limit {
                        if count >= *limit {
                            break;
                        }
                    }

                    let matches = if let Some(filter_cond) = filter {
                        // Use pre-computed metadata from schema
                        self.storage_format
                            .matches_condition_with_metadata(&value_rc, schema, filter_cond)
                            .unwrap_or(false)
                    } else {
                        true
                    };

                    if matches {
                        keys.push(key);
                        count += 1;
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

    /// Build primary key string for a row
    /// Note: TegDB only supports single-column primary keys
    fn build_primary_key_from_value(&self, table_name: &str, pk_value: &SqlValue) -> PrimaryKey {
        let native_key = NativeKey::from_sql_value(pk_value).unwrap();
        PrimaryKey::new(table_name.to_string(), native_key)
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

        // Build start key
        let start_key = if let Some(start_bound) = &pk_range.start_bound {
            let value = &start_bound.value;
            let native_key = NativeKey::from_sql_value(value).unwrap();
            let key = PrimaryKey::range_start(table, &native_key, start_bound.inclusive);
            key.to_storage_bytes()
        } else {
            PrimaryKey::table_prefix(table)
        };

        // Build end key
        let end_key = if let Some(end_bound) = &pk_range.end_bound {
            let value = &end_bound.value;
            let native_key = NativeKey::from_sql_value(value).unwrap();
            let key = PrimaryKey::range_end(table, &native_key, end_bound.inclusive);
            key.to_storage_bytes()
        } else {
            PrimaryKey::table_end_marker(table)
        };

        // Ensure start_key <= end_key for BTreeMap range scan
        if start_key > end_key {
            return Err(Error::Other(
                "Invalid range: start key is greater than end key".to_string(),
            ));
        }

        Ok((start_key, end_key))
    }

    pub fn execute_plan_with_query_schema(
        &mut self,
        plan: crate::planner::ExecutionPlan,
        query_schema: &QuerySchema,
    ) -> Result<ResultSet<'_>> {
        use crate::planner::ExecutionPlan;
        match plan {
            ExecutionPlan::PrimaryKeyLookup {
                table,
                pk_value,
                selected_columns: _,
                additional_filter,
            } => {
                let schema = self.get_table_schema(&table)?;
                let key = self.build_primary_key_from_value(&table, &pk_value);
                let key_bytes = key.to_storage_bytes();
                let scan_iter = if let Some(value) = self.transaction.get(&key_bytes) {
                    let single_result = vec![(key_bytes, value)];
                    Box::new(single_result.into_iter())
                        as Box<dyn Iterator<Item = (Vec<u8>, std::rc::Rc<[u8]>)>>
                } else {
                    Box::new(std::iter::empty())
                        as Box<dyn Iterator<Item = (Vec<u8>, std::rc::Rc<[u8]>)>>
                };
                let row_iter = SelectRowIterator::new(
                    scan_iter,
                    schema.clone(),
                    query_schema.clone(),
                    additional_filter,
                    Some(1),
                );
                Ok(ResultSet::Select {
                    columns: query_schema.column_names.clone(),
                    rows: Box::new(row_iter),
                })
            }
            ExecutionPlan::TableRangeScan {
                table,
                selected_columns: _,
                pk_range,
                additional_filter,
                limit,
            } => {
                let schema = self.get_table_schema(&table)?;
                let (start_key, end_key) = self.build_pk_range_keys(&table, &pk_range, &schema)?;
                let scan_iter = self.transaction.scan(start_key..end_key)?;
                let row_iter = SelectRowIterator::new(
                    scan_iter,
                    schema.clone(),
                    query_schema.clone(),
                    additional_filter,
                    limit,
                );
                Ok(ResultSet::Select {
                    columns: query_schema.column_names.clone(),
                    rows: Box::new(row_iter),
                })
            }
            ExecutionPlan::TableScan {
                table,
                selected_columns: _,
                filter,
                limit,
            } => {
                let schema = self.get_table_schema(&table)?;
                let start_key = PrimaryKey::table_prefix(&table);
                let end_key = PrimaryKey::table_end_marker(&table);
                let scan_iter = self.transaction.scan(start_key..end_key)?;
                let row_iter = SelectRowIterator::new(
                    scan_iter,
                    schema.clone(),
                    query_schema.clone(),
                    filter,
                    limit,
                );
                Ok(ResultSet::Select {
                    columns: query_schema.column_names.clone(),
                    rows: Box::new(row_iter),
                })
            }
            _ => self.execute_plan(plan),
        }
    }
}
