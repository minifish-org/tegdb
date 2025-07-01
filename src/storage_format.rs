use crate::parser::SqlValue;
use crate::executor::TableSchema;
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
        schema: &TableSchema
    ) -> Result<Vec<u8>> {
        crate::native_row_format::NativeRowFormat::serialize(row_data, schema)
    }
    
    /// Deserialize a complete row using the native binary format
    pub fn deserialize_row(
        &self,
        data: &[u8],
        schema: &TableSchema
    ) -> Result<HashMap<String, SqlValue>> {
        crate::native_row_format::NativeRowFormat::deserialize_full(data, schema)
    }
    
    /// Deserialize only specific columns (major optimization with native format)
    pub fn deserialize_columns(
        &self,
        data: &[u8],
        schema: &TableSchema,
        column_names: &[String]
    ) -> Result<Vec<SqlValue>> {
        crate::native_row_format::NativeRowFormat::deserialize_columns(data, schema, column_names)
    }
    
    /// Check if row matches condition without full deserialization
    pub fn matches_condition(
        &self,
        data: &[u8],
        schema: &TableSchema,
        condition: &crate::parser::Condition
    ) -> Result<bool> {
        crate::native_row_format::NativeRowFormat::matches_condition_fast(data, schema, condition)
    }
}

/// Utility function to evaluate a condition on a row HashMap
pub fn evaluate_condition_on_row(
    condition: &crate::parser::Condition,
    row_data: &HashMap<String, SqlValue>
) -> bool {
    match condition {
        crate::parser::Condition::Comparison { left, operator, right } => {
            if let Some(left_value) = row_data.get(left) {
                compare_values(left_value, operator, right)
            } else {
                false
            }
        }
        crate::parser::Condition::And(left, right) => {
            evaluate_condition_on_row(left, row_data) && 
            evaluate_condition_on_row(right, row_data)
        }
        crate::parser::Condition::Or(left, right) => {
            evaluate_condition_on_row(left, row_data) || 
            evaluate_condition_on_row(right, row_data)
        }
    }
}

/// Compare two SQL values based on operator
fn compare_values(
    left: &SqlValue,
    operator: &crate::parser::ComparisonOperator,
    right: &SqlValue
) -> bool {
    use crate::parser::ComparisonOperator::*;

    match (left, right) {
        // Optimized numeric comparisons
        (SqlValue::Integer(l), SqlValue::Integer(r)) => compare_numeric(l, r, operator),
        (SqlValue::Real(l), SqlValue::Real(r)) => compare_numeric(l, r, operator),
        
        // Handle mixed-type comparisons more carefully to avoid precision loss
        (SqlValue::Integer(l), SqlValue::Real(r)) => {
            // Try to compare as integers if the float is a whole number
            if r.fract() == 0.0 && *r >= i64::MIN as f64 && *r <= i64::MAX as f64 {
                compare_numeric(l, &(*r as i64), operator)
            } else {
                // Fallback to float comparison, acknowledging potential precision loss for large integers
                compare_numeric(&(*l as f64), r, operator)
            }
        }
        (SqlValue::Real(l), SqlValue::Integer(r)) => {
            // Symmetric case
            if l.fract() == 0.0 && *l >= i64::MIN as f64 && *l <= i64::MAX as f64 {
                compare_numeric(&(*l as i64), r, operator)
            } else {
                compare_numeric(l, &(*r as f64), operator)
            }
        }

        // Text comparisons
        (SqlValue::Text(l), SqlValue::Text(r)) => match operator {
            Equal => l == r,
            NotEqual => l != r,
            LessThan => l < r,
            LessThanOrEqual => l <= r,
            GreaterThan => l > r,
            GreaterThanOrEqual => l >= r,
            // A simple version of LIKE. For full SQL compatibility, this would need wildcard handling.
            Like => l.contains(r),
        },

        // Null comparisons
        (SqlValue::Null, SqlValue::Null) => match operator {
            Equal => true,
            NotEqual => false,
            // Comparisons other than IS NULL or IS NOT NULL with NULL are undefined/false.
            _ => false,
        },

        // All other type combinations are not comparable.
        _ => false,
    }
}

/// Generic comparison for numeric types to reduce code duplication.
fn compare_numeric<T: PartialEq + PartialOrd>(
    l: &T,
    r: &T,
    operator: &crate::parser::ComparisonOperator
) -> bool {
    use crate::parser::ComparisonOperator::*;
    match operator {
        Equal => l == r,
        NotEqual => l != r,
        LessThan => l < r,
        LessThanOrEqual => l <= r,
        GreaterThan => l > r,
        GreaterThanOrEqual => l >= r,
        Like => false, // LIKE is not for numeric types
    }
}
