use crate::executor::TableSchema;
use crate::parser::SqlValue;
use crate::Result;
use std::collections::HashMap;

/// Storage configuration for TegDB
///
/// TegDB now uses only the native binary row format for optimal performance.
/// This provides SQLite-inspired binary records with:
/// - Direct column access without full deserialization
/// - Compact variable-length encoding
/// - Efficient condition evaluation
#[derive(Clone, Debug)]
pub struct StorageFormat;

impl Default for StorageFormat {
    fn default() -> Self {
        StorageFormat
    }
}

impl StorageFormat {
    /// Create a new storage format (always native)
    pub fn new() -> Self {
        StorageFormat
    }

    /// Create native storage format (for API compatibility)
    pub fn native() -> Self {
        StorageFormat
    }

    /// Serialize a row using the native binary format
    pub fn serialize_row(
        &self,
        row_data: &HashMap<String, SqlValue>,
        schema: &TableSchema,
    ) -> Result<Vec<u8>> {
        crate::native_row_format::NativeRowFormat::serialize(row_data, schema)
    }

    /// Deserialize a complete row using the native binary format
    pub fn deserialize_row(
        &self,
        data: &[u8],
        schema: &TableSchema,
    ) -> Result<HashMap<String, SqlValue>> {
        crate::native_row_format::NativeRowFormat::deserialize_full(data, schema)
    }

    /// Deserialize only specific columns (major optimization with native format)
    pub fn deserialize_columns(
        &self,
        data: &[u8],
        schema: &TableSchema,
        column_names: &[String],
    ) -> Result<Vec<SqlValue>> {
        crate::native_row_format::NativeRowFormat::deserialize_columns(data, schema, column_names)
    }

    /// Check if row matches condition without full deserialization
    pub fn matches_condition(
        &self,
        data: &[u8],
        schema: &TableSchema,
        condition: &crate::parser::Condition,
    ) -> Result<bool> {
        crate::native_row_format::NativeRowFormat::matches_condition_fast(data, schema, condition)
    }
}

/// Utility function to evaluate a condition on a row HashMap
pub fn evaluate_condition_on_row(
    condition: &crate::parser::Condition,
    row_data: &HashMap<String, SqlValue>,
) -> bool {
    match condition {
        crate::parser::Condition::Comparison {
            left,
            operator,
            right,
        } => {
            if let Some(left_value) = row_data.get(left) {
                crate::sql_utils::compare_values(left_value, operator, right)
            } else {
                false
            }
        }
        crate::parser::Condition::And(left, right) => {
            evaluate_condition_on_row(left, row_data) && evaluate_condition_on_row(right, row_data)
        }
        crate::parser::Condition::Or(left, right) => {
            evaluate_condition_on_row(left, row_data) || evaluate_condition_on_row(right, row_data)
        }
    }
}
