use crate::parser::SqlValue;
use crate::query_processor::TableSchema;
use crate::Result;
use std::collections::HashMap;

/// Ultra-optimized storage format for TegDB with embedded metadata
///
/// This format is designed for maximum performance with:
/// - Embedded metadata in ColumnInfo (no separate computation)
/// - Zero-copy column access
/// - Direct offset-based operations
/// - Minimal allocations
#[derive(Clone, Debug)]
pub struct StorageFormat;

/// Type codes for the ultra-fast format
#[derive(Debug, Clone, Copy, PartialEq)]
#[repr(u8)]
pub enum TypeCode {
    Integer = 1,   // 8-byte i64
    Real = 2,      // 8-byte f64
    TextFixed = 3, // Fixed-length text (padded with nulls)
}

// Remove LazyRow struct, its methods, and create_lazy_row function

impl Default for StorageFormat {
    fn default() -> Self {
        Self::new()
    }
}

impl StorageFormat {
    /// Create a new storage format
    pub fn new() -> Self {
        StorageFormat
    }

    /// Ultra-fast row serialization using embedded metadata
    pub fn serialize_row(
        &self,
        row_data: &HashMap<String, SqlValue>,
        schema: &TableSchema,
    ) -> Result<Vec<u8>> {
        // Compute record size from columns
        let record_size = schema.columns.iter().map(|col| col.storage_size).sum();
        let mut buffer = vec![0u8; record_size]; // Pre-allocate exact size

        for column in &schema.columns {
            let value = row_data.get(&column.name).ok_or_else(|| {
                crate::Error::Other(format!(
                    "Missing required value for column '{}'",
                    column.name
                ))
            })?;

            Self::serialize_value_at_offset(
                value,
                &mut buffer,
                column.storage_offset,
                column.storage_size,
                column.storage_type_code,
            )?;
        }

        Ok(buffer)
    }

    /// Serialize a value at a specific offset (zero-copy)
    fn serialize_value_at_offset(
        value: &SqlValue,
        buffer: &mut [u8],
        offset: usize,
        size: usize,
        type_code: u8,
    ) -> Result<()> {
        match (value, type_code) {
            (SqlValue::Integer(i), 1) => {
                // TypeCode::Integer
                buffer[offset..offset + 8].copy_from_slice(&i.to_le_bytes());
            }
            (SqlValue::Real(r), 2) => {
                // TypeCode::Real
                buffer[offset..offset + 8].copy_from_slice(&r.to_le_bytes());
            }
            (SqlValue::Text(s), 3) => {
                // TypeCode::TextFixed
                let bytes = s.as_bytes();
                let copy_len = bytes.len().min(size);
                buffer[offset..offset + copy_len].copy_from_slice(&bytes[..copy_len]);
                // Pad with nulls if needed
                if copy_len < size {
                    buffer[offset + copy_len..offset + size].fill(0);
                }
            }
            _ => {
                return Err(crate::Error::Other(
                    "Type mismatch during serialization".to_string(),
                ))
            }
        }
        Ok(())
    }

    /// Get a single column value by name (zero-copy) using embedded metadata
    pub fn get_column_value(
        &self,
        data: &[u8],
        schema: &TableSchema,
        column_name: &str,
    ) -> Result<SqlValue> {
        if let Some((index, _)) = schema
            .columns
            .iter()
            .enumerate()
            .find(|(_, col)| col.name == column_name)
        {
            let column_info = &schema.columns[index];
            Self::deserialize_value_at_offset(
                data,
                column_info.storage_offset,
                column_info.storage_size,
                column_info.storage_type_code,
            )
        } else {
            Err(crate::Error::Other(format!(
                "Column '{column_name}' not found"
            )))
        }
    }

    /// Get a single column value by index (zero-copy) using pre-computed metadata
    pub fn get_column_by_index(
        &self,
        data: &[u8],
        schema: &TableSchema,
        column_index: usize,
    ) -> Result<SqlValue> {
        if column_index >= schema.columns.len() {
            return Err(crate::Error::Other(
                "Column index out of bounds".to_string(),
            ));
        }
        let column_info = &schema.columns[column_index];
        Self::deserialize_value_at_offset(
            data,
            column_info.storage_offset,
            column_info.storage_size,
            column_info.storage_type_code,
        )
    }

    /// Get multiple columns by name (zero-copy) using pre-computed metadata
    pub fn get_columns(
        &self,
        data: &[u8],
        schema: &TableSchema,
        column_names: &[&str],
    ) -> Result<Vec<SqlValue>> {
        let mut result = Vec::with_capacity(column_names.len());
        for &column_name in column_names {
            result.push(self.get_column_value(data, schema, column_name)?);
        }
        Ok(result)
    }

    /// Evaluate a condition (zero-copy where possible)
    pub fn matches_condition(
        &self,
        data: &[u8],
        schema: &TableSchema,
        condition: &crate::parser::Condition,
    ) -> Result<bool> {
        match condition {
            crate::parser::Condition::Comparison {
                left,
                operator,
                right,
            } => {
                if let Ok(left_val) = self.get_column_value(data, schema, left) {
                    match (left_val, right) {
                        (SqlValue::Integer(l), SqlValue::Integer(r)) => match operator {
                            crate::parser::ComparisonOperator::Equal => Ok(l == *r),
                            crate::parser::ComparisonOperator::NotEqual => Ok(l != *r),
                            crate::parser::ComparisonOperator::LessThan => Ok(l < *r),
                            crate::parser::ComparisonOperator::LessThanOrEqual => Ok(l <= *r),
                            crate::parser::ComparisonOperator::GreaterThan => Ok(l > *r),
                            crate::parser::ComparisonOperator::GreaterThanOrEqual => Ok(l >= *r),
                            _ => Ok(false),
                        },
                        (SqlValue::Real(l), SqlValue::Real(r)) => match operator {
                            crate::parser::ComparisonOperator::Equal => {
                                Ok((l - *r).abs() < f64::EPSILON)
                            }
                            crate::parser::ComparisonOperator::NotEqual => {
                                Ok((l - *r).abs() >= f64::EPSILON)
                            }
                            crate::parser::ComparisonOperator::LessThan => Ok(l < *r),
                            crate::parser::ComparisonOperator::LessThanOrEqual => Ok(l <= *r),
                            crate::parser::ComparisonOperator::GreaterThan => Ok(l > *r),
                            crate::parser::ComparisonOperator::GreaterThanOrEqual => Ok(l >= *r),
                            _ => Ok(false),
                        },
                        (SqlValue::Text(l), SqlValue::Text(r)) => match operator {
                            crate::parser::ComparisonOperator::Equal => Ok(l == *r),
                            crate::parser::ComparisonOperator::NotEqual => Ok(l != *r),
                            crate::parser::ComparisonOperator::LessThan => Ok(l < *r),
                            crate::parser::ComparisonOperator::LessThanOrEqual => Ok(l <= *r),
                            crate::parser::ComparisonOperator::GreaterThan => Ok(l > *r),
                            crate::parser::ComparisonOperator::GreaterThanOrEqual => Ok(l >= *r),
                            _ => Ok(false),
                        },
                        _ => Ok(false),
                    }
                } else {
                    let row_data = self.deserialize_row_full(data, schema)?;
                    Ok(evaluate_condition_on_row(condition, &row_data))
                }
            }
            crate::parser::Condition::Between { column, low, high } => {
                if let Ok(val) = self.get_column_value(data, schema, column) {
                    let ge = crate::sql_utils::compare_values(
                        &val,
                        &crate::parser::ComparisonOperator::GreaterThanOrEqual,
                        low,
                    );
                    let le = crate::sql_utils::compare_values(
                        &val,
                        &crate::parser::ComparisonOperator::LessThanOrEqual,
                        high,
                    );
                    Ok(ge && le)
                } else {
                    let row_data = self.deserialize_row_full(data, schema)?;
                    Ok(evaluate_condition_on_row(condition, &row_data))
                }
            }
            _ => {
                let row_data = self.deserialize_row_full(data, schema)?;
                Ok(evaluate_condition_on_row(condition, &row_data))
            }
        }
    }

    /// Full row deserialization (only when needed) using pre-computed metadata
    pub fn deserialize_row_full(
        &self,
        data: &[u8],
        schema: &TableSchema,
    ) -> Result<HashMap<String, SqlValue>> {
        let mut result = HashMap::with_capacity(schema.columns.len());
        for column in &schema.columns {
            let value = Self::deserialize_value_at_offset(
                data,
                column.storage_offset,
                column.storage_size,
                column.storage_type_code,
            )?;
            result.insert(column.name.clone(), value);
        }
        Ok(result)
    }

    /// Ultra-fast deserialization at specific offset (zero-copy)
    fn deserialize_value_at_offset(
        data: &[u8],
        offset: usize,
        size: usize,
        type_code: u8,
    ) -> Result<SqlValue> {
        match type_code {
            1 => {
                // TypeCode::Integer - zero-copy, just reinterpret bytes
                let bytes = &data[offset..offset + 8];
                let value = i64::from_le_bytes(bytes.try_into().unwrap());
                Ok(SqlValue::Integer(value))
            }
            2 => {
                // TypeCode::Real - zero-copy, just reinterpret bytes
                let bytes = &data[offset..offset + 8];
                let value = f64::from_le_bytes(bytes.try_into().unwrap());
                Ok(SqlValue::Real(value))
            }
            3 => {
                // TypeCode::TextFixed - minimize allocations by finding null terminator efficiently
                let bytes = &data[offset..offset + size];
                // Find the first null byte to determine actual string length
                let actual_len = bytes.iter().position(|&b| b == 0).unwrap_or(size);
                let text_bytes = &bytes[..actual_len];
                // Use from_utf8_lossy to avoid allocation for valid UTF-8, only allocate for invalid sequences
                let text = String::from_utf8_lossy(text_bytes).into_owned();
                Ok(SqlValue::Text(text))
            }
            _ => Err(crate::Error::Other("Invalid type code".to_string())),
        }
    }

    /// Get record size from schema
    pub fn get_record_size(&self, schema: &TableSchema) -> Result<usize> {
        Ok(schema.columns.iter().map(|col| col.storage_size).sum())
    }

    // Remove backward compatibility methods if unused

    /// Get a single column value by name (zero-copy) - with cached metadata
    pub fn get_column_value_with_metadata(
        &self,
        data: &[u8],
        schema: &TableSchema,
        column_name: &str,
    ) -> Result<SqlValue> {
        if let Some((index, _)) = schema
            .columns
            .iter()
            .enumerate()
            .find(|(_, col)| col.name == column_name)
        {
            let column_info = &schema.columns[index];
            Self::deserialize_value_at_offset(
                data,
                column_info.storage_offset,
                column_info.storage_size,
                column_info.storage_type_code,
            )
        } else {
            Err(crate::Error::Other(format!(
                "Column '{column_name}' not found"
            )))
        }
    }

    /// Get a single column value by index (zero-copy) - with cached metadata
    pub fn get_column_by_index_with_metadata(
        &self,
        data: &[u8],
        schema: &TableSchema,
        column_index: usize,
    ) -> Result<SqlValue> {
        if column_index >= schema.columns.len() {
            return Err(crate::Error::Other(
                "Column index out of bounds".to_string(),
            ));
        }

        let column_info = &schema.columns[column_index];
        Self::deserialize_value_at_offset(
            data,
            column_info.storage_offset,
            column_info.storage_size,
            column_info.storage_type_code,
        )
    }

    /// Get multiple columns by index (zero-copy) - with cached metadata
    pub fn get_columns_by_indices_with_metadata(
        &self,
        data: &[u8],
        schema: &TableSchema,
        column_indices: &[usize],
    ) -> Result<Vec<SqlValue>> {
        // Pre-allocate result vector with exact capacity to avoid reallocations
        let mut result = Vec::with_capacity(column_indices.len());

        for &index in column_indices {
            if index >= schema.columns.len() {
                return Err(crate::Error::Other(
                    "Column index out of bounds".to_string(),
                ));
            }

            let column_info = &schema.columns[index];
            let value = Self::deserialize_value_at_offset(
                data,
                column_info.storage_offset,
                column_info.storage_size,
                column_info.storage_type_code,
            )?;
            result.push(value);
        }
        Ok(result)
    }

    /// Evaluate a condition (zero-copy where possible) - with cached metadata
    pub fn matches_condition_with_metadata(
        &self,
        data: &[u8],
        schema: &TableSchema,
        condition: &crate::parser::Condition,
    ) -> Result<bool> {
        match condition {
            crate::parser::Condition::Comparison {
                left,
                operator,
                right,
            } => {
                if let Ok(left_val) = self.get_column_value_with_metadata(data, schema, left) {
                    match (left_val, right) {
                        (SqlValue::Integer(l), SqlValue::Integer(r)) => match operator {
                            crate::parser::ComparisonOperator::Equal => Ok(l == *r),
                            crate::parser::ComparisonOperator::NotEqual => Ok(l != *r),
                            crate::parser::ComparisonOperator::LessThan => Ok(l < *r),
                            crate::parser::ComparisonOperator::LessThanOrEqual => Ok(l <= *r),
                            crate::parser::ComparisonOperator::GreaterThan => Ok(l > *r),
                            crate::parser::ComparisonOperator::GreaterThanOrEqual => Ok(l >= *r),
                            _ => Ok(false),
                        },
                        (SqlValue::Real(l), SqlValue::Real(r)) => match operator {
                            crate::parser::ComparisonOperator::Equal => {
                                Ok((l - *r).abs() < f64::EPSILON)
                            }
                            crate::parser::ComparisonOperator::NotEqual => {
                                Ok((l - *r).abs() >= f64::EPSILON)
                            }
                            crate::parser::ComparisonOperator::LessThan => Ok(l < *r),
                            crate::parser::ComparisonOperator::LessThanOrEqual => Ok(l <= *r),
                            crate::parser::ComparisonOperator::GreaterThan => Ok(l > *r),
                            crate::parser::ComparisonOperator::GreaterThanOrEqual => Ok(l >= *r),
                            _ => Ok(false),
                        },
                        (SqlValue::Text(l), SqlValue::Text(r)) => match operator {
                            crate::parser::ComparisonOperator::Equal => Ok(l == *r),
                            crate::parser::ComparisonOperator::NotEqual => Ok(l != *r),
                            crate::parser::ComparisonOperator::LessThan => Ok(l < *r),
                            crate::parser::ComparisonOperator::LessThanOrEqual => Ok(l <= *r),
                            crate::parser::ComparisonOperator::GreaterThan => Ok(l > *r),
                            crate::parser::ComparisonOperator::GreaterThanOrEqual => Ok(l >= *r),
                            _ => Ok(false),
                        },
                        _ => Ok(false),
                    }
                } else {
                    // Fallback to full deserialization for complex cases
                    let row_data = self.deserialize_row_full_with_metadata(data, schema)?;
                    Ok(evaluate_condition_on_row(condition, &row_data))
                }
            }
            crate::parser::Condition::Between { column, low, high } => {
                if let Ok(val) = self.get_column_value_with_metadata(data, schema, column) {
                    let ge = crate::sql_utils::compare_values(
                        &val,
                        &crate::parser::ComparisonOperator::GreaterThanOrEqual,
                        low,
                    );
                    let le = crate::sql_utils::compare_values(
                        &val,
                        &crate::parser::ComparisonOperator::LessThanOrEqual,
                        high,
                    );
                    Ok(ge && le)
                } else {
                    let row_data = self.deserialize_row_full_with_metadata(data, schema)?;
                    Ok(evaluate_condition_on_row(condition, &row_data))
                }
            }
            _ => {
                // For complex conditions, fall back to full deserialization
                let row_data = self.deserialize_row_full_with_metadata(data, schema)?;
                Ok(evaluate_condition_on_row(condition, &row_data))
            }
        }
    }

    /// Deserialize full row using embedded metadata
    pub fn deserialize_row_full_with_metadata(
        &self,
        data: &[u8],
        schema: &TableSchema,
    ) -> Result<HashMap<String, SqlValue>> {
        let mut result = HashMap::with_capacity(schema.columns.len());
        for column in &schema.columns {
            let value = Self::deserialize_value_at_offset(
                data,
                column.storage_offset,
                column.storage_size,
                column.storage_type_code,
            )?;
            result.insert(column.name.clone(), value);
        }
        Ok(result)
    }
}

/// Evaluate a condition on row data (fallback for complex conditions)
fn evaluate_condition_on_row(
    condition: &crate::parser::Condition,
    row_data: &HashMap<String, SqlValue>,
) -> bool {
    match condition {
        crate::parser::Condition::Comparison {
            left,
            operator,
            right,
        } => {
            let left_val = row_data.get(left).unwrap_or(&SqlValue::Integer(0));
            match (left_val, right) {
                (SqlValue::Integer(l), SqlValue::Integer(r)) => match operator {
                    crate::parser::ComparisonOperator::Equal => l == r,
                    crate::parser::ComparisonOperator::NotEqual => l != r,
                    crate::parser::ComparisonOperator::LessThan => l < r,
                    crate::parser::ComparisonOperator::LessThanOrEqual => l <= r,
                    crate::parser::ComparisonOperator::GreaterThan => l > r,
                    crate::parser::ComparisonOperator::GreaterThanOrEqual => l >= r,
                    _ => false,
                },
                (SqlValue::Real(l), SqlValue::Real(r)) => match operator {
                    crate::parser::ComparisonOperator::Equal => (l - r).abs() < f64::EPSILON,
                    crate::parser::ComparisonOperator::NotEqual => (l - r).abs() >= f64::EPSILON,
                    crate::parser::ComparisonOperator::LessThan => l < r,
                    crate::parser::ComparisonOperator::LessThanOrEqual => l <= r,
                    crate::parser::ComparisonOperator::GreaterThan => l > r,
                    crate::parser::ComparisonOperator::GreaterThanOrEqual => l >= r,
                    _ => false,
                },
                (SqlValue::Text(l), SqlValue::Text(r)) => match operator {
                    crate::parser::ComparisonOperator::Equal => l == r,
                    crate::parser::ComparisonOperator::NotEqual => l != r,
                    crate::parser::ComparisonOperator::LessThan => l < r,
                    crate::parser::ComparisonOperator::LessThanOrEqual => l <= r,
                    crate::parser::ComparisonOperator::GreaterThan => l > r,
                    crate::parser::ComparisonOperator::GreaterThanOrEqual => l >= r,
                    _ => false,
                },
                _ => false,
            }
        }
        crate::parser::Condition::Between { column, low, high } => {
            let val = row_data.get(column).unwrap_or(&SqlValue::Null);
            crate::sql_utils::compare_values(
                val,
                &crate::parser::ComparisonOperator::GreaterThanOrEqual,
                low,
            ) && crate::sql_utils::compare_values(
                val,
                &crate::parser::ComparisonOperator::LessThanOrEqual,
                high,
            )
        }
        crate::parser::Condition::And(left, right) => {
            evaluate_condition_on_row(left, row_data) && evaluate_condition_on_row(right, row_data)
        }
        crate::parser::Condition::Or(left, right) => {
            evaluate_condition_on_row(left, row_data) || evaluate_condition_on_row(right, row_data)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::parser::{DataType, SqlValue};
    use crate::query_processor::{ColumnInfo, TableSchema};
    use std::collections::HashMap;

    fn create_test_schema() -> TableSchema {
        let mut schema = TableSchema {
            name: "test_table".to_string(),
            columns: vec![
                ColumnInfo {
                    name: "id".to_string(),
                    data_type: DataType::Integer,
                    constraints: vec![],
                    storage_offset: 0,
                    storage_size: 8,
                    storage_type_code: TypeCode::Integer as u8,
                },
                ColumnInfo {
                    name: "name".to_string(),
                    data_type: DataType::Text(Some(10)),
                    constraints: vec![],
                    storage_offset: 8,
                    storage_size: 10,
                    storage_type_code: TypeCode::TextFixed as u8,
                },
                ColumnInfo {
                    name: "score".to_string(),
                    data_type: DataType::Real,
                    constraints: vec![],
                    storage_offset: 18,
                    storage_size: 8,
                    storage_type_code: TypeCode::Real as u8,
                },
            ],
        };
        let _ = crate::catalog::Catalog::compute_table_metadata(&mut schema);
        schema
    }

    #[test]
    fn test_serialize_deserialize_round_trip() {
        let schema = create_test_schema();
        let storage = StorageFormat::new();
        let mut row_data = HashMap::new();
        row_data.insert("id".to_string(), SqlValue::Integer(123));
        row_data.insert("name".to_string(), SqlValue::Text("Alice".to_string()));
        row_data.insert("score".to_string(), SqlValue::Real(95.5));

        let serialized = storage.serialize_row(&row_data, &schema).unwrap();
        let deserialized = storage.deserialize_row_full(&serialized, &schema).unwrap();

        assert_eq!(deserialized.get("id"), Some(&SqlValue::Integer(123)));
        assert_eq!(
            deserialized.get("name"),
            Some(&SqlValue::Text("Alice".to_string()))
        );
        assert_eq!(deserialized.get("score"), Some(&SqlValue::Real(95.5)));
    }

    #[test]
    fn test_partial_column_deserialization() {
        let schema = create_test_schema();
        let storage = StorageFormat::new();
        let mut row_data = HashMap::new();
        row_data.insert("id".to_string(), SqlValue::Integer(456));
        row_data.insert("name".to_string(), SqlValue::Text("Bob".to_string()));
        row_data.insert("score".to_string(), SqlValue::Real(87.2));

        let serialized = storage.serialize_row(&row_data, &schema).unwrap();

        // Only deserialize name and score
        let columns = ["name".to_string(), "score".to_string()];
        let columns_ref: Vec<&str> = columns.iter().map(|s| s.as_str()).collect();
        let values = storage
            .get_columns(&serialized, &schema, &columns_ref)
            .unwrap();

        assert_eq!(values.len(), 2);
        assert_eq!(values[0], SqlValue::Text("Bob".to_string()));
        assert_eq!(values[1], SqlValue::Real(87.2));
    }
}
