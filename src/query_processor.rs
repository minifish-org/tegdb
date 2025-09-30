//! Modern query processor for TegDB with native row format support
//!
//! This module provides the core query execution engine that works directly with the
//! native binary row format for optimal performance.

use crate::catalog::IndexInfo;
use crate::parser::{
    ColumnConstraint, Condition, CreateTableStatement, DataType, DropTableStatement, Expression,
    IndexType, OrderDirection, SqlValue,
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
    Vector(Vec<f64>),
    Null,
}

impl NativeKey {
    /// Convert SqlValue to NativeKey (zero-copy where possible)
    pub fn from_sql_value(value: &SqlValue) -> Result<Self> {
        match value {
            SqlValue::Integer(i) => Ok(NativeKey::Integer(*i)),
            SqlValue::Real(r) => Ok(NativeKey::Real(*r)),
            SqlValue::Text(t) => Ok(NativeKey::Text(t.clone())), // Only clone needed
            SqlValue::Vector(v) => Ok(NativeKey::Vector(v.clone())), // Clone needed for vector
            SqlValue::Null => Ok(NativeKey::Null),
            SqlValue::Parameter(_) => Err(Error::SqlError(
                "Parameter placeholder found in key generation - parameter binding failed"
                    .to_string(),
            )),
        }
    }

    /// Serialize to bytes for storage (efficient binary format)
    pub fn to_bytes(&self) -> Vec<u8> {
        match self {
            NativeKey::Integer(i) => {
                // Pre-allocate exact size to avoid reallocations
                let mut bytes = Vec::with_capacity(9);
                bytes.push(0x01); // Type tag for Integer
                bytes.extend_from_slice(&i.to_be_bytes()); // Use big-endian for correct ordering
                bytes
            }
            NativeKey::Real(r) => {
                // Pre-allocate exact size to avoid reallocations
                let mut bytes = Vec::with_capacity(9);
                bytes.push(0x02); // Type tag for Real
                bytes.extend_from_slice(&r.to_be_bytes()); // Use big-endian for correct ordering
                bytes
            }
            NativeKey::Text(t) => {
                // Pre-allocate exact size to avoid reallocations
                let mut bytes = Vec::with_capacity(5 + t.len());
                bytes.push(0x03); // Type tag for Text
                bytes.extend_from_slice(&(t.len() as u32).to_le_bytes());
                bytes.extend_from_slice(t.as_bytes());
                bytes
            }
            NativeKey::Vector(v) => {
                // Pre-allocate exact size to avoid reallocations
                let mut bytes = Vec::with_capacity(5 + v.len() * 8);
                bytes.push(0x04); // Type tag for Vector
                bytes.extend_from_slice(&(v.len() as u32).to_le_bytes());
                for &val in v {
                    bytes.extend_from_slice(&val.to_be_bytes()); // Use big-endian for correct ordering
                }
                bytes
            }
            NativeKey::Null => {
                vec![0x00] // Type tag for Null
            }
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

    /// Serialize to storage bytes (efficient binary format)
    pub fn to_storage_bytes(&self) -> Vec<u8> {
        // Pre-allocate with exact capacity to avoid reallocations
        let mut bytes = Vec::with_capacity(4 + self.table_name.len() + 1 + 9); // table_len + table + separator + key
        bytes.extend_from_slice(&(self.table_name.len() as u32).to_le_bytes());
        bytes.extend_from_slice(self.table_name.as_bytes());
        bytes.push(crate::catalog::STORAGE_SEPARATOR); // Separator
        bytes.extend_from_slice(&self.key.to_bytes());
        bytes
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
                NativeKey::Vector(v) => {
                    // For vectors, add a small epsilon to the first element
                    if !v.is_empty() {
                        v[0] += f64::EPSILON;
                    }
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
            NativeKey::Vector(v) => {
                if inclusive && !v.is_empty() {
                    v[0] = f64::from_bits(v[0].to_bits() + 1); // next representable float
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
        // Pre-allocate with exact capacity to avoid reallocations
        let mut bytes = Vec::with_capacity(4 + table_name.len() + 1);
        bytes.extend_from_slice(&(table_name.len() as u32).to_le_bytes());
        bytes.extend_from_slice(table_name.as_bytes());
        bytes.push(crate::catalog::STORAGE_SEPARATOR); // Separator
        bytes
    }

    /// Create table end marker for full table scans
    pub fn table_end_marker(table_name: &str) -> Vec<u8> {
        // Pre-allocate with exact capacity to avoid reallocations
        let mut bytes = Vec::with_capacity(4 + table_name.len() + 1);
        bytes.extend_from_slice(&(table_name.len() as u32).to_le_bytes());
        bytes.extend_from_slice(table_name.as_bytes());
        bytes.push(crate::catalog::TABLE_END_SENTINEL); // End marker
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
    pub indexes: Vec<IndexInfo>,
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
#[derive(Clone, Debug)]
pub struct QuerySchema {
    pub column_names: Vec<String>,
    pub column_indices: Vec<usize>,
    pub expressions: Option<Vec<Expression>>, // New field for expressions
}

impl QuerySchema {
    pub fn new(selected_columns: &[String], schema: &TableSchema) -> Self {
        let mut column_indices = Vec::new();
        for column_name in selected_columns {
            if let Some(index) = schema.get_column_index(column_name) {
                column_indices.push(index);
            } else {
                // For expressions or non-existent columns, use a placeholder index
                column_indices.push(0);
            }
        }
        Self {
            column_names: selected_columns.to_vec(),
            column_indices,
            expressions: None,
        }
    }

    pub fn new_with_expressions(
        selected_columns: &[crate::parser::Expression],
        schema: &TableSchema,
    ) -> Self {
        let mut column_names = Vec::new();
        let mut column_indices = Vec::new();
        let mut expressions = Vec::new();

        for (i, expr) in selected_columns.iter().enumerate() {
            match expr {
                crate::parser::Expression::Column(name) => {
                    // Special case: "*" is not a real column, treat as expression
                    if name == "*" {
                        column_names.push(format!("expr_{i}"));
                        column_indices.push(0); // Placeholder for expressions
                        expressions.push(expr.clone());
                    } else {
                        // This is a regular column
                        column_names.push(name.clone());
                        if let Some(index) = schema.get_column_index(name) {
                            column_indices.push(index);
                        } else {
                            column_indices.push(0); // Placeholder
                        }
                        expressions.push(expr.clone());
                    }
                }
                _ => {
                    // This is an expression (function call, etc.)
                    column_names.push(format!("expr_{i}"));
                    column_indices.push(0); // Placeholder for expressions
                    expressions.push(expr.clone());
                }
            }
        }

        Self {
            column_names,
            column_indices,
            expressions: Some(expressions),
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
    /// Aggregate mode - if Some, contains the aggregate result to return
    aggregate_result: Option<Vec<SqlValue>>,
    /// Sorted mode - if Some, contains the sorted results to return
    sorted_results: Option<std::vec::IntoIter<Vec<SqlValue>>>,
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
            aggregate_result: None,
            sorted_results: None,
        }
    }

    /// Collect all remaining rows into a Vec for backward compatibility
    /// Optimized to reduce memory allocations and copying
    pub fn collect_rows(self) -> Result<Vec<Vec<SqlValue>>> {
        // Use collect() which is already optimized by the standard library
        // The iterator will yield rows one by one, avoiding large memmove operations
        self.collect()
    }

    fn evaluate_expression(
        expression: &Expression,
        row_data: &HashMap<String, SqlValue>,
        _row_bytes: &[u8],
    ) -> Result<SqlValue> {
        match expression {
            Expression::Value(value) => Ok(value.clone()),
            Expression::Column(column_name) => row_data
                .get(column_name)
                .cloned()
                .ok_or_else(|| crate::Error::Other(format!("Column '{column_name}' not found"))),
            Expression::BinaryOp {
                left,
                operator,
                right,
            } => {
                let left_val = Self::evaluate_expression(left, row_data, _row_bytes)?;
                let right_val = Self::evaluate_expression(right, row_data, _row_bytes)?;

                match (left_val, right_val) {
                    (SqlValue::Integer(a), SqlValue::Integer(b)) => {
                        let result = match operator {
                            crate::parser::ArithmeticOperator::Add => a + b,
                            crate::parser::ArithmeticOperator::Subtract => a - b,
                            crate::parser::ArithmeticOperator::Multiply => a * b,
                            crate::parser::ArithmeticOperator::Divide => {
                                if b == 0 {
                                    return Err(crate::Error::Other(format!(
                                        "Division by zero in expression: {a} / {b}"
                                    )));
                                }
                                a / b
                            }
                            crate::parser::ArithmeticOperator::Modulo => {
                                if b == 0 {
                                    return Err(crate::Error::Other(format!(
                                        "Modulo by zero in expression: {a} % {b}"
                                    )));
                                }
                                a % b
                            }
                        };
                        Ok(SqlValue::Integer(result))
                    }
                    (SqlValue::Real(a), SqlValue::Real(b)) => {
                        let result = match operator {
                            crate::parser::ArithmeticOperator::Add => a + b,
                            crate::parser::ArithmeticOperator::Subtract => a - b,
                            crate::parser::ArithmeticOperator::Multiply => a * b,
                            crate::parser::ArithmeticOperator::Divide => {
                                if b == 0.0 {
                                    return Err(crate::Error::Other(format!(
                                        "Division by zero in expression: {a} / {b}"
                                    )));
                                }
                                a / b
                            }
                            crate::parser::ArithmeticOperator::Modulo => {
                                if b == 0.0 {
                                    return Err(crate::Error::Other(format!(
                                        "Modulo by zero in expression: {a} % {b}"
                                    )));
                                }
                                a % b
                            }
                        };
                        Ok(SqlValue::Real(result))
                    }
                    _ => Err(crate::Error::Other(format!(
                        "Unsupported operation for mixed types: {operator:?}"
                    ))),
                }
            }
            Expression::FunctionCall { name, args } => {
                // Evaluate all arguments first
                let evaluated_args: Result<Vec<Expression>> = args
                    .iter()
                    .map(|arg| {
                        let value = Self::evaluate_expression(arg, row_data, _row_bytes)?;
                        Ok(Expression::Value(value))
                    })
                    .collect();

                let evaluated_args = evaluated_args?;

                // Create a new function call with evaluated arguments
                let func_call = Expression::FunctionCall {
                    name: name.clone(),
                    args: evaluated_args,
                };

                // Evaluate the function call using the Expression::evaluate method
                func_call
                    .evaluate(row_data)
                    .map_err(|e| crate::Error::Other(format!("Function evaluation error: {e}")))
            }
            Expression::AggregateFunction { name, arg } => {
                // For now, we'll evaluate the argument but not perform aggregation
                // This will be handled by the query processor during execution
                let _arg_value = Self::evaluate_expression(arg, row_data, _row_bytes)?;
                match name.to_uppercase().as_str() {
                    "COUNT" => Ok(SqlValue::Integer(1)), // Placeholder
                    "SUM" => Ok(SqlValue::Integer(0)),   // Placeholder
                    "AVG" => Ok(SqlValue::Real(0.0)),    // Placeholder
                    "MAX" => Ok(SqlValue::Integer(0)),   // Placeholder
                    "MIN" => Ok(SqlValue::Integer(0)),   // Placeholder
                    _ => Err(crate::Error::Other(format!(
                        "Aggregate function '{name}' is not implemented. Supported functions: COUNT, SUM, AVG, MAX, MIN"
                    ))),
                }
            }
        }
    }
}

impl<'a> Iterator for SelectRowIterator<'a> {
    type Item = Result<Vec<SqlValue>>;

    fn next(&mut self) -> Option<Self::Item> {
        // Handle aggregate result mode
        if let Some(ref aggregate_result) = self.aggregate_result {
            if self.count == 0 {
                self.count += 1;
                return Some(Ok(aggregate_result.clone()));
            } else {
                return None;
            }
        }

        // Handle sorted results mode
        if let Some(ref mut sorted_iter) = self.sorted_results {
            if let Some(row) = sorted_iter.next() {
                self.count += 1;
                return Some(Ok(row));
            } else {
                return None;
            }
        }

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
                        // If we have expressions, evaluate them
                        let final_values = if let Some(ref expressions) =
                            self.query_schema.expressions
                        {
                            // Expression case: evaluate each expression
                            let mut final_values = Vec::new();

                            // Create row data for expression evaluation
                            let mut row_data = HashMap::new();
                            for (i, &col_idx) in self.query_schema.column_indices.iter().enumerate()
                            {
                                if let Some(col_name) =
                                    self.schema.columns.get(col_idx).map(|c| &c.name)
                                {
                                    if i < row_values.len() {
                                        row_data.insert(col_name.clone(), row_values[i].clone());
                                    }
                                }
                            }

                            // Evaluate each expression
                            for expr in expressions {
                                match Self::evaluate_expression(expr, &row_data, &value) {
                                    Ok(value) => final_values.push(value),
                                    Err(e) => return Some(Err(e)),
                                }
                            }
                            final_values
                        } else {
                            // Standard case: column names match row values
                            row_values
                        };

                        self.count += 1;
                        return Some(Ok(final_values));
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
    /// CREATE INDEX result
    CreateIndex,
    /// DROP INDEX result
    DropIndex,
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
            .ok_or_else(|| Error::TableNotFound(table_name.to_string()))
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
                let available_columns = schema.get_column_names().join(", ");
                return Err(Error::ColumnNotFound(format!(
                    "Column '{column_name}' does not exist in table '{table_name}'. Available columns: {available_columns}"
                )));
            }
        }

        // Check that all required columns are provided
        for col in &schema.columns {
            if schema.is_column_required(&col.name) {
                match row_data.get(&col.name) {
                    None => {
                        let col_name = &col.name;
                        return Err(Error::SqlError(format!(
                            "Required column '{col_name}' is missing for table '{table_name}'"
                        )));
                    }
                    Some(SqlValue::Null) => {
                        let col_name = &col.name;
                        return Err(Error::SqlError(format!(
                            "Column '{col_name}' in table '{table_name}' does not allow NULL values"
                        )));
                    }
                    Some(_) => {}
                }
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
            let table_name = &create.table;
            return Err(Error::SqlError(format!(
                "Table '{table_name}' has composite primary key, but TegDB only supports single-column primary keys"
            )));
        }

        if pk_count == 0 {
            let table_name = &create.table;
            return Err(Error::SqlError(format!(
                "Table '{table_name}' must have exactly one primary key column"
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
            indexes: vec![], // Initialize indexes as empty
        };
        // Compute storage metadata and persist schema via central serializer
        let _ = crate::catalog::Catalog::compute_table_metadata(&mut schema);
        let schema_key = crate::catalog::Catalog::get_schema_storage_key(&create.table);
        let schema_data = crate::catalog::Catalog::serialize_schema_to_bytes(&schema);
        self.transaction.set(schema_key.as_bytes(), schema_data)?;

        // Add to in-memory schemas and validation cache
        let schema_rc = Rc::new(schema.clone());
        self.table_schemas.insert(create.table.clone(), schema_rc);

        Ok(ResultSet::CreateTable)
    }

    /// Execute DROP TABLE statement
    pub fn execute_drop_table(&mut self, drop: DropTableStatement) -> Result<ResultSet<'_>> {
        // Check if table exists
        let table_existed = self.table_schemas.contains_key(&drop.table);

        if !drop.if_exists && !table_existed {
            let table_name = &drop.table;
            let available_tables = self
                .table_schemas
                .keys()
                .cloned()
                .collect::<Vec<_>>()
                .join(", ");
            return Err(Error::TableNotFound(format!(
                "Table '{table_name}' does not exist. Available tables: {available_tables}"
            )));
        }

        if table_existed {
            // Delete schema metadata
            let schema_key = crate::catalog::Catalog::get_schema_storage_key(&drop.table);
            self.transaction.delete(schema_key.as_bytes())?;

            // Delete all table data using canonical key range helpers
            let start_key = PrimaryKey::table_prefix(&drop.table);
            let end_key = PrimaryKey::table_end_marker(&drop.table);

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

    /// Execute CREATE INDEX statement
    pub fn execute_create_index(
        &mut self,
        create: crate::parser::CreateIndexStatement,
    ) -> Result<ResultSet<'_>> {
        // Check if table exists
        if !self.table_schemas.contains_key(&create.table_name) {
            let table_name = &create.table_name;
            let available_tables = self
                .table_schemas
                .keys()
                .cloned()
                .collect::<Vec<_>>()
                .join(", ");
            return Err(Error::TableNotFound(format!(
                "Table '{table_name}' does not exist. Available tables: {available_tables}"
            )));
        }

        // Check if column exists in the table
        let schema = self.get_table_schema(&create.table_name)?;
        if !schema.has_column(&create.column_name) {
            let column_name = &create.column_name;
            let table_name = &create.table_name;
            let available_columns = schema.get_column_names().join(", ");
            return Err(Error::ColumnNotFound(format!(
                "Column '{column_name}' does not exist in table '{table_name}'. Available columns: {available_columns}"
            )));
        }

        // Check if index already exists
        if schema
            .indexes
            .iter()
            .any(|idx| idx.name == create.index_name)
        {
            let index_name = &create.index_name;
            return Err(Error::SqlError(format!(
                "Index '{index_name}' already exists"
            )));
        }

        let column_info = schema
            .get_column(&create.column_name)
            .ok_or_else(|| Error::ColumnNotFound(create.column_name.clone()))?;

        let requested_index_type = create.index_type.unwrap_or({
            if matches!(column_info.data_type, DataType::Vector(_)) {
                IndexType::HNSW
            } else {
                IndexType::BTree
            }
        });

        // Enforce compatibility between column data type, uniqueness, and index type
        match (&column_info.data_type, requested_index_type) {
            (DataType::Vector(_), IndexType::BTree) => {
                return Err(Error::Other(
                    "BTree indexes are not supported on VECTOR columns".to_string(),
                ));
            }
            (DataType::Vector(_), _) => {
                if create.unique {
                    return Err(Error::Other(
                        "Unique constraints are not supported on vector indexes".to_string(),
                    ));
                }
            }
            (_, IndexType::HNSW | IndexType::IVF | IndexType::LSH) => {
                return Err(Error::Other(format!(
                    "Index type '{requested_index_type:?}' requires a VECTOR column"
                )));
            }
            _ => {}
        }

        // Create index info
        let index = crate::catalog::IndexInfo {
            name: create.index_name.clone(),
            table_name: create.table_name.clone(),
            column_name: create.column_name.clone(),
            unique: create.unique,
            index_type: requested_index_type,
        };

        // Store index metadata
        let index_key = crate::catalog::Catalog::get_index_storage_key(&create.index_name);
        let index_data = crate::catalog::Catalog::serialize_index_to_bytes(&index);
        self.transaction.set(index_key.as_bytes(), index_data)?;

        // Add to in-memory schema
        let mut schema = schema.as_ref().clone();
        schema.indexes.push(index.clone());
        self.table_schemas
            .insert(create.table_name.clone(), Rc::new(schema));

        // Populate the index with existing data (only needed for BTree indexes currently)
        if matches!(requested_index_type, IndexType::BTree) {
            self.populate_index_with_existing_data(&create.table_name, &index)?;
        }

        Ok(ResultSet::CreateIndex)
    }

    /// Execute DROP INDEX statement
    pub fn execute_drop_index(
        &mut self,
        drop: crate::parser::DropIndexStatement,
    ) -> Result<ResultSet<'_>> {
        // Find the index in any table
        let mut found = false;
        for (table_name, schema_rc) in &self.table_schemas {
            let schema = schema_rc.as_ref();
            if schema.indexes.iter().any(|idx| idx.name == drop.index_name) {
                found = true;

                // Remove index metadata from storage
                let index_key = crate::catalog::Catalog::get_index_storage_key(&drop.index_name);
                self.transaction.delete(index_key.as_bytes())?;

                // Remove from in-memory schema
                let mut new_schema = schema.clone();
                if let Some(pos) = new_schema
                    .indexes
                    .iter()
                    .position(|idx| idx.name == drop.index_name)
                {
                    let index_info = new_schema.indexes.remove(pos);

                    if matches!(index_info.index_type, IndexType::BTree) {
                        let (range_start, range_end) =
                            crate::catalog::index_full_range(table_name, &index_info.name);
                        let keys: Vec<Vec<u8>> = self
                            .transaction
                            .scan(range_start..range_end)?
                            .map(|(key, _)| key)
                            .collect();
                        for key in keys {
                            self.transaction.delete(&key)?;
                        }
                    }
                }
                self.table_schemas
                    .insert(table_name.clone(), Rc::new(new_schema));
                break;
            }
        }

        if !found && !drop.if_exists {
            let index_name = &drop.index_name;
            return Err(Error::Other(format!("Index '{index_name}' does not exist")));
        }

        Ok(ResultSet::DropIndex)
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
            | ExecutionPlan::TableScan { .. }
            | ExecutionPlan::IndexScan { .. }
            | ExecutionPlan::VectorSearch { .. } => self.execute_select_plan_streaming(plan),
            ExecutionPlan::Sort {
                input_plan,
                order_by_items,
                schema,
                query_schema,
                limit,
            } => {
                // For ORDER BY, we need to get the full row data to sort by columns not in SELECT
                // We need to extract the full rows from the input plan, not just the selected columns
                let full_rows = match &*input_plan {
                    ExecutionPlan::TableScan { table, filter, .. } => {
                        let start_key = PrimaryKey::table_prefix(table);
                        let end_key = PrimaryKey::table_end_marker(table);
                        let scan_iter = self.transaction.scan(start_key..end_key)?;
                        let table_schema = self.get_table_schema(table)?;

                        let mut rows = Vec::new();
                        for (_, value) in scan_iter {
                            // Apply filter if present
                            let matches = if let Some(ref filter_condition) = filter {
                                match self.storage_format.matches_condition_with_metadata(
                                    &value,
                                    &table_schema,
                                    filter_condition,
                                ) {
                                    Ok(matches) => matches,
                                    Err(_) => continue, // Skip rows that don't match filter
                                }
                            } else {
                                true // No filter, so it matches
                            };

                            if matches {
                                // Get the full row data
                                let row_values =
                                    self.storage_format.get_columns_by_indices_with_metadata(
                                        &value,
                                        &table_schema,
                                        &(0..table_schema.columns.len()).collect::<Vec<_>>(),
                                    )?;
                                rows.push(row_values);
                            }
                        }
                        rows
                    }
                    ExecutionPlan::VectorSearch { table, .. } => {
                        // For VectorSearch, we need to get the full rows to sort properly
                        let start_key = PrimaryKey::table_prefix(table);
                        let end_key = PrimaryKey::table_end_marker(table);
                        let scan_iter = self.transaction.scan(start_key..end_key)?;
                        let table_schema = self.get_table_schema(table)?;

                        let mut rows = Vec::new();
                        for (_, value) in scan_iter {
                            // Get the full row data
                            let row_values =
                                self.storage_format.get_columns_by_indices_with_metadata(
                                    &value,
                                    &table_schema,
                                    &(0..table_schema.columns.len()).collect::<Vec<_>>(),
                                )?;
                            rows.push(row_values);
                        }
                        rows
                    }
                    _ => {
                        // For other plan types, fall back to materialized execution
                        self.execute_plan_materialized(*input_plan.clone())?
                    }
                };

                // Create a mapping from full row to selected columns
                let mut row_mapping: Vec<(Vec<SqlValue>, Vec<SqlValue>)> = Vec::new();
                for full_row in full_rows {
                    // Extract selected columns from full row
                    let mut selected_values = Vec::new();
                    for col_name in &query_schema.column_names {
                        if let Some(col_idx) =
                            schema.columns.iter().position(|c| c.name == *col_name)
                        {
                            if col_idx < full_row.len() {
                                selected_values.push(full_row[col_idx].clone());
                            }
                        }
                    }
                    row_mapping.push((full_row, selected_values));
                }

                // Sort the full rows based on order_by_items
                row_mapping.sort_by(|(a_full, _), (b_full, _)| {
                    for item in &order_by_items {
                        if let Expression::Column(column_name) = &item.expression {
                            if let Some(col_idx) =
                                schema.columns.iter().position(|c| c.name == *column_name)
                            {
                                if col_idx < a_full.len() && col_idx < b_full.len() {
                                    let cmp = match (&a_full[col_idx], &b_full[col_idx]) {
                                        (SqlValue::Integer(a), SqlValue::Integer(b)) => a.cmp(b),
                                        (SqlValue::Real(a), SqlValue::Real(b)) => {
                                            a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal)
                                        }
                                        (SqlValue::Text(a), SqlValue::Text(b)) => a.cmp(b),
                                        (SqlValue::Vector(a), SqlValue::Vector(b)) => a
                                            .iter()
                                            .zip(b.iter())
                                            .map(|(x, y)| {
                                                x.partial_cmp(y)
                                                    .unwrap_or(std::cmp::Ordering::Equal)
                                            })
                                            .find(|&ord| ord != std::cmp::Ordering::Equal)
                                            .unwrap_or_else(|| a.len().cmp(&b.len())),
                                        (SqlValue::Null, SqlValue::Null) => {
                                            std::cmp::Ordering::Equal
                                        }
                                        (SqlValue::Null, _) => std::cmp::Ordering::Less,
                                        (_, SqlValue::Null) => std::cmp::Ordering::Greater,
                                        _ => std::cmp::Ordering::Equal,
                                    };

                                    if cmp != std::cmp::Ordering::Equal {
                                        return match item.direction {
                                            OrderDirection::Asc => cmp,
                                            OrderDirection::Desc => cmp.reverse(),
                                        };
                                    }
                                }
                            }
                        }
                    }
                    std::cmp::Ordering::Equal
                });

                // Extract the sorted selected columns
                let mut sorted_rows: Vec<Vec<SqlValue>> = row_mapping
                    .into_iter()
                    .map(|(_, selected)| selected)
                    .collect();

                // Apply LIMIT if specified
                if let Some(limit) = limit {
                    sorted_rows.truncate(limit as usize);
                }

                // Create a SelectRowIterator with sorted results
                let mut sorted_iter = SelectRowIterator::new(
                    Box::new(std::iter::empty::<(Vec<u8>, std::rc::Rc<[u8]>)>()),
                    schema,
                    query_schema.clone(),
                    None,
                    None,
                );
                sorted_iter.sorted_results = Some(sorted_rows.into_iter());

                Ok(ResultSet::Select {
                    columns: query_schema.column_names.clone(),
                    rows: Box::new(sorted_iter),
                })
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
            ExecutionPlan::CreateIndex {
                index_name,
                table_name,
                column_name,
                unique,
            } => {
                let create_stmt = crate::parser::CreateIndexStatement {
                    index_name,
                    table_name,
                    column_name,
                    unique,
                    index_type: None, // Default to BTree for now
                };
                self.execute_create_index(create_stmt)
            }
            ExecutionPlan::DropIndex {
                index_name,
                if_exists,
            } => {
                let drop_stmt = crate::parser::DropIndexStatement {
                    index_name,
                    if_exists,
                };
                self.execute_drop_index(drop_stmt)
            }
            ExecutionPlan::Begin => self.begin_transaction(),
            ExecutionPlan::Commit => self.commit_transaction(),
            ExecutionPlan::Rollback => self.rollback_transaction(),
        }
    }

    /// Check if the selected columns contain aggregate functions
    fn has_aggregate_functions(&self, selected_columns: &[crate::parser::Expression]) -> bool {
        use crate::parser::Expression;
        selected_columns
            .iter()
            .any(|expr| matches!(expr, Expression::AggregateFunction { .. }))
    }

    /// Execute aggregate query by processing all rows and computing aggregates
    fn execute_aggregate_query(
        &mut self,
        plan: crate::planner::ExecutionPlan,
        query_schema: QuerySchema,
    ) -> Result<ResultSet<'_>> {
        use crate::planner::ExecutionPlan;

        let mut row_maps: Vec<HashMap<String, SqlValue>> = Vec::new();

        match plan {
            ExecutionPlan::PrimaryKeyLookup {
                table,
                pk_value,
                additional_filter,
                ..
            } => {
                let schema = self.get_table_schema(&table)?;
                let key = self.build_primary_key_from_value(&table, &pk_value);
                if let Some(value) = self.transaction.get(&key.to_storage_bytes()) {
                    if self.row_matches_condition(
                        &schema,
                        value.as_ref(),
                        additional_filter.as_ref(),
                    )? {
                        row_maps.push(
                            self.storage_format
                                .deserialize_row_full(value.as_ref(), &schema)?,
                        );
                    }
                }
            }
            ExecutionPlan::TableRangeScan {
                table,
                pk_range,
                additional_filter,
                ..
            } => {
                let schema = self.get_table_schema(&table)?;
                let (start_key, end_key) = self.build_pk_range_keys(&table, &pk_range, &schema)?;
                for (_, value) in self.transaction.scan(start_key..end_key)? {
                    if self.row_matches_condition(
                        &schema,
                        value.as_ref(),
                        additional_filter.as_ref(),
                    )? {
                        row_maps.push(
                            self.storage_format
                                .deserialize_row_full(value.as_ref(), &schema)?,
                        );
                    }
                }
            }
            ExecutionPlan::TableScan { table, filter, .. } => {
                let schema = self.get_table_schema(&table)?;
                let start_key = PrimaryKey::table_prefix(&table);
                let end_key = PrimaryKey::table_end_marker(&table);
                for (_, value) in self.transaction.scan(start_key..end_key)? {
                    if self.row_matches_condition(&schema, value.as_ref(), filter.as_ref())? {
                        row_maps.push(
                            self.storage_format
                                .deserialize_row_full(value.as_ref(), &schema)?,
                        );
                    }
                }
            }
            ExecutionPlan::IndexScan {
                table,
                index,
                column_value,
                additional_filter,
                ..
            } => {
                let schema = self.get_table_schema(&table)?;
                let (index_start, index_end) =
                    crate::catalog::index_prefix_range(&table, &index, &column_value);

                for (key, _value) in self.transaction.scan(index_start..index_end)? {
                    if let Some((_table, _index, _col_val, pk_str)) =
                        crate::catalog::decode_index_key(&key)
                    {
                        let pk_value = if let Ok(pk_int) = pk_str.parse::<i64>() {
                            SqlValue::Integer(pk_int)
                        } else {
                            SqlValue::Text(pk_str)
                        };
                        let pk_key = self.build_primary_key_from_value(&table, &pk_value);
                        if let Some(value) = self.transaction.get(&pk_key.to_storage_bytes()) {
                            if self.row_matches_condition(
                                &schema,
                                value.as_ref(),
                                additional_filter.as_ref(),
                            )? {
                                row_maps.push(
                                    self.storage_format
                                        .deserialize_row_full(value.as_ref(), &schema)?,
                                );
                            }
                        }
                    }
                }
            }
            other => {
                return Err(Error::Other(format!(
                    "Aggregate execution not supported for plan: {other:?}"
                )));
            }
        }

        let aggregate_results = self.build_aggregate_row(&query_schema, &row_maps)?;

        let empty_iter = Box::new(std::iter::empty::<(Vec<u8>, std::rc::Rc<[u8]>)>());
        let mut aggregate_iter = SelectRowIterator::new(
            empty_iter,
            std::rc::Rc::new(TableSchema {
                name: "aggregate_result".to_string(),
                columns: vec![],
                indexes: vec![],
            }),
            query_schema.clone(),
            None,
            None,
        );
        aggregate_iter.aggregate_result = Some(aggregate_results);

        Ok(ResultSet::Select {
            columns: query_schema.column_names.clone(),
            rows: Box::new(aggregate_iter),
        })
    }

    fn build_aggregate_row(
        &self,
        query_schema: &QuerySchema,
        rows: &[HashMap<String, SqlValue>],
    ) -> Result<Vec<SqlValue>> {
        use crate::parser::Expression;

        if let Some(expressions) = &query_schema.expressions {
            let mut results = Vec::with_capacity(expressions.len());
            for expr in expressions {
                match expr {
                    Expression::AggregateFunction { name, arg } => {
                        results.push(self.compute_aggregate(name, arg, rows)?);
                    }
                    _ => {
                        let value = if let Some(first_row) = rows.first() {
                            // Special case: "*" column should not be evaluated in aggregate context
                            if matches!(expr, Expression::Column(name) if name == "*") {
                                SqlValue::Null // "*" is not a real column
                            } else {
                                expr.evaluate(first_row).map_err(|e| {
                                    Error::Other(format!("Expression evaluation error: {e}"))
                                })?
                            }
                        } else {
                            let empty_context: HashMap<String, SqlValue> = HashMap::new();
                            match expr.evaluate(&empty_context) {
                                Ok(v) => v,
                                Err(_) => SqlValue::Null,
                            }
                        };
                        results.push(value);
                    }
                }
            }
            Ok(results)
        } else {
            Ok(vec![SqlValue::Integer(rows.len() as i64)])
        }
    }

    /// Compute aggregate function result across fully materialized rows
    fn compute_aggregate(
        &self,
        func_name: &str,
        arg: &crate::parser::Expression,
        rows: &[HashMap<String, SqlValue>],
    ) -> Result<crate::parser::SqlValue> {
        use crate::parser::Expression;

        if matches!(arg, Expression::Column(col) if col == "*")
            && func_name.eq_ignore_ascii_case("COUNT")
        {
            return Ok(SqlValue::Integer(rows.len() as i64));
        }

        let mut values: Vec<SqlValue> = Vec::with_capacity(rows.len());
        for row in rows {
            let value = match arg {
                Expression::Column(col_name) => {
                    row.get(col_name).cloned().unwrap_or(SqlValue::Null)
                }
                _ => match arg.evaluate(row) {
                    Ok(v) => v,
                    Err(_) => SqlValue::Null,
                },
            };
            values.push(value);
        }

        match func_name.to_uppercase().as_str() {
            "COUNT" => {
                let count = values
                    .iter()
                    .filter(|v| !matches!(v, SqlValue::Null))
                    .count();
                Ok(SqlValue::Integer(count as i64))
            }
            "SUM" => {
                let mut has_value = false;
                let mut sum_f64: f64 = 0.0;

                for value in values.iter() {
                    match value {
                        SqlValue::Integer(i) => {
                            sum_f64 += *i as f64;
                            has_value = true;
                        }
                        SqlValue::Real(r) => {
                            sum_f64 += *r;
                            has_value = true;
                        }
                        _ => {}
                    }
                }

                if !has_value {
                    Ok(SqlValue::Null)
                } else {
                    // Always return Real for SUM to match SQL standard behavior
                    Ok(SqlValue::Real(sum_f64))
                }
            }
            "AVG" => {
                let mut count = 0;
                let mut sum = 0.0;
                for value in values.iter() {
                    match value {
                        SqlValue::Integer(i) => {
                            sum += *i as f64;
                            count += 1;
                        }
                        SqlValue::Real(r) => {
                            sum += *r;
                            count += 1;
                        }
                        _ => {}
                    }
                }
                if count == 0 {
                    Ok(SqlValue::Null)
                } else {
                    Ok(SqlValue::Real(sum / count as f64))
                }
            }
            "MAX" => self.extremum(&values, std::cmp::Ordering::Greater),
            "MIN" => self.extremum(&values, std::cmp::Ordering::Less),
            _ => Err(Error::Other(format!(
                "Unsupported aggregate function: {func_name}"
            ))),
        }
    }

    fn row_matches_condition(
        &self,
        schema: &TableSchema,
        row: &[u8],
        condition: Option<&crate::parser::Condition>,
    ) -> Result<bool> {
        if let Some(cond) = condition {
            self.storage_format
                .matches_condition_with_metadata(row, schema, cond)
                .map_err(|e| Error::Other(format!("Failed to evaluate condition: {e}")))
        } else {
            Ok(true)
        }
    }

    fn extremum(&self, values: &[SqlValue], target_order: std::cmp::Ordering) -> Result<SqlValue> {
        let mut best: Option<SqlValue> = None;
        for value in values {
            if matches!(value, SqlValue::Null) {
                continue;
            }

            match &best {
                Some(current) => {
                    if let Some(ordering) = Self::compare_sql_values(value, current) {
                        if ordering == target_order {
                            best = Some(value.clone());
                        }
                    }
                }
                None => {
                    best = Some(value.clone());
                }
            }
        }

        Ok(best.unwrap_or(SqlValue::Null))
    }

    fn compare_sql_values(left: &SqlValue, right: &SqlValue) -> Option<std::cmp::Ordering> {
        use SqlValue::*;
        match (left, right) {
            (Integer(a), Integer(b)) => Some(a.cmp(b)),
            (Real(a), Real(b)) => a.partial_cmp(b),
            (Integer(a), Real(b)) => (*a as f64).partial_cmp(b),
            (Real(a), Integer(b)) => a.partial_cmp(&(*b as f64)),
            (Text(a), Text(b)) => Some(a.cmp(b)),
            _ => None,
        }
    }

    /// Execute SELECT plans using streaming and collect results
    /// This eliminates duplicate code by using a single streaming implementation
    fn execute_select_plan_streaming(
        &mut self,
        plan: crate::planner::ExecutionPlan,
    ) -> Result<ResultSet<'_>> {
        use crate::planner::ExecutionPlan;

        // Clone the plan for aggregate function detection
        let plan_clone = plan.clone();

        match plan {
            ExecutionPlan::PrimaryKeyLookup {
                table,
                pk_value,
                selected_columns,
                additional_filter,
            } => {
                let schema = self.get_table_schema(&table)?;
                let query_schema = QuerySchema::new_with_expressions(&selected_columns, &schema);

                // Check if this is an aggregate query
                if self.has_aggregate_functions(&selected_columns) {
                    return self.execute_aggregate_query(plan_clone.clone(), query_schema);
                }

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
                    query_schema.clone(),
                    additional_filter,
                    Some(1), // PK lookup returns at most 1 row
                );

                Ok(ResultSet::Select {
                    columns: query_schema.column_names.clone(),
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
                let query_schema = QuerySchema::new_with_expressions(&selected_columns, &schema);

                // Check if this is an aggregate query
                if self.has_aggregate_functions(&selected_columns) {
                    return self.execute_aggregate_query(plan_clone.clone(), query_schema);
                }

                // Build range scan keys based on PK range
                let (start_key, end_key) = self.build_pk_range_keys(&table, &pk_range, &schema)?;

                // Create streaming iterator for range scan
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
            ExecutionPlan::IndexScan {
                table,
                index,
                column_value,
                selected_columns,
                additional_filter,
            } => {
                let schema = self.get_table_schema(&table)?;
                let query_schema = QuerySchema::new_with_expressions(&selected_columns, &schema);

                // Check if this is an aggregate query
                if self.has_aggregate_functions(&selected_columns) {
                    return self.execute_aggregate_query(plan_clone.clone(), query_schema);
                }

                // For now, use the existing non-streaming index scan implementation
                // TODO: Implement proper streaming index scan iterator
                let (index_start, index_end) =
                    crate::catalog::index_prefix_range(&table, &index, &column_value);

                let mut row_maps = Vec::new();
                for (key, _value) in self.transaction.scan(index_start..index_end)? {
                    if let Some((_table, _index, _col_val, pk_str)) =
                        crate::catalog::decode_index_key(&key)
                    {
                        let pk_value = if let Ok(pk_int) = pk_str.parse::<i64>() {
                            SqlValue::Integer(pk_int)
                        } else {
                            SqlValue::Text(pk_str)
                        };
                        let pk_key = self.build_primary_key_from_value(&table, &pk_value);
                        if let Some(value) = self.transaction.get(&pk_key.to_storage_bytes()) {
                            if self.row_matches_condition(
                                &schema,
                                value.as_ref(),
                                additional_filter.as_ref(),
                            )? {
                                row_maps.push(
                                    self.storage_format
                                        .deserialize_row_full(value.as_ref(), &schema)?,
                                );
                            }
                        }
                    }
                }

                // Convert to streaming iterator
                let row_values: Vec<Vec<SqlValue>> = row_maps
                    .into_iter()
                    .map(|row_map| {
                        query_schema
                            .column_names
                            .iter()
                            .map(|col_name| {
                                row_map.get(col_name).cloned().unwrap_or(SqlValue::Null)
                            })
                            .collect()
                    })
                    .collect();

                // Create a simple iterator that yields the collected rows
                let row_iter = SelectRowIterator::new(
                    Box::new(std::iter::empty()) as ScanIterator,
                    schema.clone(),
                    query_schema.clone(),
                    None,
                    None,
                );

                // Override the iterator's behavior by setting sorted_results
                let mut result_iter = row_iter;
                result_iter.sorted_results = Some(row_values.into_iter());

                Ok(ResultSet::Select {
                    columns: query_schema.column_names.clone(),
                    rows: Box::new(result_iter),
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
                let query_schema = QuerySchema::new_with_expressions(&selected_columns, &schema);

                // Check if this is an aggregate query
                if self.has_aggregate_functions(&selected_columns) {
                    return self.execute_aggregate_query(plan_clone.clone(), query_schema);
                }

                let start_key = PrimaryKey::table_prefix(&table);
                let end_key = PrimaryKey::table_end_marker(&table);
                // Create streaming iterator for table scan
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
            ExecutionPlan::VectorSearch { .. } => {
                // Handle VectorSearch execution plan
                let ExecutionPlan::VectorSearch {
                    table,
                    selected_columns,
                    additional_filter,
                    ..
                } = plan
                else {
                    unreachable!()
                };

                let schema = self.get_table_schema(&table)?;
                let query_schema = QuerySchema::new_with_expressions(&selected_columns, &schema);

                // Check if this is an aggregate query
                if self.has_aggregate_functions(&selected_columns) {
                    return self.execute_aggregate_query(plan_clone.clone(), query_schema);
                }

                // Fall back to table scan for now
                let start_key = PrimaryKey::table_prefix(&table);
                let end_key = PrimaryKey::table_end_marker(&table);
                let scan_iter = self.transaction.scan(start_key..end_key)?;
                let row_iter = SelectRowIterator::new(
                    scan_iter,
                    schema.clone(),
                    query_schema.clone(),
                    additional_filter,
                    None,
                );

                Ok(ResultSet::Select {
                    columns: query_schema.column_names.clone(),
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
                let pk_col = schema.get_primary_key_column().unwrap_or("<pk>");
                let pk_val = row_data.get(pk_col).cloned().unwrap_or(SqlValue::Null);
                return Err(Error::Other(format!(
                    "Primary key constraint violation on table '{table}': key '{pk_col}' has duplicate value {pk_val:?}"
                )));
            }

            // Serialize and store row
            let serialized = self.storage_format.serialize_row(row_data, &schema)?;
            self.transaction.set(&key.to_storage_bytes(), serialized)?;

            // Create index entries for this row
            self.create_index_entries(table, &schema, row_data)?;

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
            let selected_columns = match &scan_plan {
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

            // Extract column names from expressions
            let mut column_names = Vec::new();
            for expr in &selected_columns {
                match expr {
                    crate::parser::Expression::Column(name) => {
                        column_names.push(name.clone());
                    }
                    _ => {
                        return Err(Error::Other(
                            "Update operations only support column references".to_string(),
                        ));
                    }
                }
            }

            // Get the plan results and materialize immediately to avoid lifetime conflicts
            let materialized_rows = self.execute_plan_materialized(scan_plan)?;

            // Pre-allocate with exact capacity to avoid reallocations
            let mut keys = Vec::with_capacity(materialized_rows.len());
            for row_values in materialized_rows {
                let mut row_data = HashMap::with_capacity(column_names.len());
                for (i, col_name) in column_names.iter().enumerate() {
                    if let Some(value) = row_values.get(i) {
                        row_data.insert(col_name.clone(), value.clone());
                    }
                }
                let pk_column = schema.get_primary_key_column().unwrap();
                let key =
                    self.build_primary_key_from_value(table, row_data.get(pk_column).unwrap());
                keys.push(key);
            }
            keys
        };

        for key in keys_to_update {
            if let Some(value) = self.transaction.get(&key.to_storage_bytes()) {
                if let Ok(old_row_data) = self.storage_format.deserialize_row_full(&value, &schema)
                {
                    self.remove_index_entries(table, &schema, &old_row_data)?;
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
                    let pk_column = schema.get_primary_key_column().unwrap();
                    let new_key =
                        self.build_primary_key_from_value(table, row_data.get(pk_column).unwrap());
                    let new_key_bytes = new_key.to_storage_bytes();
                    let key_bytes = key.to_storage_bytes();
                    if new_key_bytes != key_bytes && self.transaction.get(&new_key_bytes).is_some()
                    {
                        let pk_col = schema.get_primary_key_column().unwrap_or("<pk>");
                        let pk_val = row_data.get(pk_col).cloned().unwrap_or(SqlValue::Null);
                        return Err(Error::Other(format!(
                            "Primary key constraint violation on table '{table}': key '{pk_col}' has duplicate value {pk_val:?}"
                        )));
                    }

                    // Validate other constraints (NOT NULL, etc.) but skip primary key validation
                    // since we already handled it above
                    self.validate_row_data(table, &row_data)?;

                    // Serialize and store the updated row
                    let serialized = self.storage_format.serialize_row(&row_data, &schema)?;

                    // If primary key changed, we need to delete the old row and insert the new one
                    if new_key_bytes != key_bytes {
                        self.transaction.delete(&key_bytes)?;
                        self.transaction.set(&new_key_bytes, serialized)?;
                    } else {
                        self.transaction.set(&key_bytes, serialized)?;
                    }

                    self.create_index_entries(table, &schema, &row_data)?;

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
            if let Some(value) = self.transaction.get(key_bytes) {
                let row_data = self.storage_format.deserialize_row_full(&value, &schema)?;
                self.remove_index_entries(table, &schema, &row_data)?;
            }
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
        // Pre-allocate with reasonable capacity to avoid reallocations
        let mut keys = Vec::with_capacity(100);

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
                    if let Some(limit) = limit {
                        if count >= *limit {
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
            ExecutionPlan::VectorSearch {
                table,
                additional_filter,
                ..
            } => {
                let start_key = PrimaryKey::table_prefix(table);
                let end_key = PrimaryKey::table_end_marker(table);
                let scan_iter = self.transaction.scan(start_key..end_key)?;

                for (key, value_rc) in scan_iter {
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

    /// Execute a query plan using a previously computed `QuerySchema`.
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
            ExecutionPlan::VectorSearch {
                table,
                index: _index,
                query_vector: _query_vector,
                similarity_function: _similarity_function,
                k,
                selected_columns,
                additional_filter,
            } => {
                let schema = self.get_table_schema(&table)?;
                let query_schema = QuerySchema::new_with_expressions(&selected_columns, &schema);

                // Check if this is an aggregate query
                if self.has_aggregate_functions(&selected_columns) {
                    let plan_for_aggregate = ExecutionPlan::VectorSearch {
                        table,
                        index: _index,
                        query_vector: _query_vector,
                        similarity_function: _similarity_function,
                        k,
                        selected_columns,
                        additional_filter: additional_filter.clone(),
                    };
                    return self.execute_aggregate_query(plan_for_aggregate, query_schema);
                }

                // For now, fall back to table scan with vector similarity computation
                // TODO: Implement proper vector index usage
                let start_key = PrimaryKey::table_prefix(&table);
                let end_key = PrimaryKey::table_end_marker(&table);
                let scan_iter = self.transaction.scan(start_key..end_key)?;

                // For now, use a table scan with vector similarity computation
                // TODO: Implement proper vector index usage
                let row_iter = SelectRowIterator::new(
                    scan_iter,
                    schema.clone(),
                    query_schema.clone(),
                    additional_filter,
                    Some(k as u64),
                );

                Ok(ResultSet::Select {
                    columns: query_schema.column_names.clone(),
                    rows: Box::new(row_iter),
                })
            }
            _ => self.execute_plan(plan),
        }
    }

    fn create_index_entries(
        &mut self,
        table: &str,
        schema: &TableSchema,
        row_data: &HashMap<String, SqlValue>,
    ) -> Result<()> {
        for index in &schema.indexes {
            if !matches!(index.index_type, IndexType::BTree) {
                // Vector and other specialized indexes maintain their own structures elsewhere.
                continue;
            }
            if let Some(column_value) = row_data.get(&index.column_name) {
                let pk_column = schema.get_primary_key_column().unwrap();
                let pk_value = row_data.get(pk_column).unwrap();
                let index_key =
                    crate::catalog::encode_index_key(table, &index.name, column_value, pk_value);

                if index.unique {
                    let (range_start, range_end) =
                        crate::catalog::index_prefix_range(table, &index.name, column_value);
                    for (existing_key, _) in self
                        .transaction
                        .scan(range_start.clone()..range_end.clone())?
                    {
                        if existing_key != index_key {
                            return Err(Error::Other(format!(
                                "Unique constraint violation on index '{name}' (column '{col}'): duplicate value {val:?}",
                                name = index.name,
                                col = index.column_name,
                                val = column_value
                            )));
                        }
                    }
                }

                self.transaction.set(&index_key, b"1".to_vec())?;
            }
        }
        Ok(())
    }

    fn remove_index_entries(
        &mut self,
        table: &str,
        schema: &TableSchema,
        row_data: &HashMap<String, SqlValue>,
    ) -> Result<()> {
        for index in &schema.indexes {
            if !matches!(index.index_type, IndexType::BTree) {
                continue;
            }
            if let Some(column_value) = row_data.get(&index.column_name) {
                let pk_column = schema.get_primary_key_column().unwrap();
                if let Some(pk_value) = row_data.get(pk_column) {
                    let index_key = crate::catalog::encode_index_key(
                        table,
                        &index.name,
                        column_value,
                        pk_value,
                    );
                    self.transaction.delete(&index_key)?;
                }
            }
        }
        Ok(())
    }

    /// Populate an index with existing data from the table
    fn populate_index_with_existing_data(
        &mut self,
        table_name: &str,
        index: &crate::catalog::IndexInfo,
    ) -> Result<()> {
        if !matches!(index.index_type, IndexType::BTree) {
            // Specialized indexes maintain their own structures; nothing to do for the BTree store.
            return Ok(());
        }

        let schema = self.get_table_schema(table_name)?;

        // Scan all existing rows in the table
        let start_key = PrimaryKey::table_prefix(table_name);
        let end_key = PrimaryKey::table_end_marker(table_name);
        let scan_iter = self.transaction.scan(start_key..end_key)?;

        // Collect all rows first to avoid borrow checker issues
        let mut rows_to_index = Vec::new();
        for (_, value_rc) in scan_iter {
            // Deserialize the row data
            let row_data = self
                .storage_format
                .deserialize_row_full(&value_rc, &schema)?;
            rows_to_index.push(row_data);
        }

        // Now create index entries for all rows
        for row_data in rows_to_index {
            if let Some(column_value) = row_data.get(&index.column_name) {
                let pk_column = schema.get_primary_key_column().unwrap();
                let pk_value = row_data.get(pk_column).unwrap();
                let index_key = crate::catalog::encode_index_key(
                    table_name,
                    &index.name,
                    column_value,
                    pk_value,
                );

                if index.unique {
                    let (range_start, range_end) =
                        crate::catalog::index_prefix_range(table_name, &index.name, column_value);
                    for (existing_key, _) in self
                        .transaction
                        .scan(range_start.clone()..range_end.clone())?
                    {
                        if existing_key != index_key {
                            return Err(Error::Other(format!(
                                "Unique constraint violation on index '{name}' (column '{col}'): duplicate value {val:?}",
                                name = index.name,
                                col = index.column_name,
                                val = column_value
                            )));
                        }
                    }
                }

                self.transaction.set(&index_key, b"1".to_vec())?;
            }
        }

        Ok(())
    }
}
