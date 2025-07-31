use crate::parser::SqlValue;
use crate::query_processor::TableSchema;
use crate::sql_utils::evaluate_condition;
use std::collections::HashMap;
use crate::Result;

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
    Vector = 4,    // Vector of f64 values
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
            (SqlValue::Vector(v), 4) => {
                // TypeCode::Vector
                let expected_len = size / 8;
                if v.len() != expected_len {
                    return Err(crate::Error::Other(
                        format!("Vector length {} does not match schema dimension {}", v.len(), expected_len)
                    ));
                }
                // Handle zero-dimension vectors (no data to copy)
                if expected_len == 0 {
                    return Ok(());
                }
                for (i, &val) in v.iter().enumerate() {
                    let val_offset = offset + i * 8;
                    buffer[val_offset..val_offset + 8].copy_from_slice(&val.to_le_bytes());
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
                // For simple column references, try to get the value directly
                if let crate::parser::Expression::Column(column_name) = left {
                    if let Ok(left_val) = self.get_column_value(data, schema, column_name) {
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
                                crate::parser::ComparisonOperator::Like => Ok(crate::sql_utils::compare_values(&SqlValue::Text(l.clone()), &crate::parser::ComparisonOperator::Like, &SqlValue::Text(r.clone()))),
                            },
                            _ => Ok(false),
                        }
                    } else {
                        // Fallback to full deserialization for complex cases
                        let row_data = self.deserialize_row_full(data, schema)?;
                        Ok(evaluate_condition(condition, &row_data))
                    }
                } else {
                    // For complex expressions (like function calls), fall back to full deserialization
                    let row_data = self.deserialize_row_full(data, schema)?;
                    Ok(evaluate_condition(condition, &row_data))
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
                    Ok(evaluate_condition(condition, &row_data))
                }
            }
            _ => {
                // For complex conditions, fall back to full deserialization
                let row_data = self.deserialize_row_full(data, schema)?;
                Ok(evaluate_condition(condition, &row_data))
            }
        }
    }

    /// Deserialize full row using embedded metadata
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

    /// Deserialize a value at a specific offset with bounds checking
    fn deserialize_value_at_offset(
        data: &[u8],
        offset: usize,
        size: usize,
        type_code: u8,
    ) -> Result<SqlValue> {
        // Handle zero-size values (like zero-dimension vectors)
        if size == 0 {
            match type_code {
                4 => return Ok(SqlValue::Vector(vec![])), // Zero-dimension vector
                _ => return Err(crate::Error::Other("Zero size not supported for this type".to_string())),
            }
        }
        
        // Add robust bounds checking
        if offset >= data.len() {
            return Err(crate::Error::Other("Offset out of bounds".to_string()));
        }
        if offset + size > data.len() {
            return Err(crate::Error::Other("Range end index out of range".to_string()));
        }
        if data.is_empty() {
            return Err(crate::Error::Other("Data buffer is empty".to_string()));
        }

        match type_code {
            1 => {
                // TypeCode::Integer
                if size < 8 {
                    return Err(crate::Error::Other("Invalid size for integer".to_string()));
                }
                let bytes = &data[offset..offset + 8];
                let value = i64::from_le_bytes([
                    bytes[0], bytes[1], bytes[2], bytes[3],
                    bytes[4], bytes[5], bytes[6], bytes[7],
                ]);
                Ok(SqlValue::Integer(value))
            }
            2 => {
                // TypeCode::Real
                if size < 8 {
                    return Err(crate::Error::Other("Invalid size for real".to_string()));
                }
                let bytes = &data[offset..offset + 8];
                let value = f64::from_le_bytes([
                    bytes[0], bytes[1], bytes[2], bytes[3],
                    bytes[4], bytes[5], bytes[6], bytes[7],
                ]);
                Ok(SqlValue::Real(value))
            }
            3 => {
                // TypeCode::TextFixed
                let bytes = &data[offset..offset + size];
                // Find the first null byte or use the full size
                let text_len = bytes.iter().position(|&b| b == 0).unwrap_or(size);
                let text_bytes = &bytes[..text_len];
                let text = String::from_utf8_lossy(text_bytes).to_string();
                Ok(SqlValue::Text(text))
            }
            4 => {
                // TypeCode::Vector
                let vector_size = size / 8;
                if size % 8 != 0 {
                    return Err(crate::Error::Other("Invalid vector size".to_string()));
                }
                // Handle zero-dimension vectors
                if vector_size == 0 {
                    return Ok(SqlValue::Vector(vec![]));
                }
                let mut vector = Vec::with_capacity(vector_size);
                for i in 0..vector_size {
                    let val_offset = offset + i * 8;
                    if val_offset + 8 > data.len() {
                        return Err(crate::Error::Other("Vector element out of bounds".to_string()));
                    }
                    let bytes = &data[val_offset..val_offset + 8];
                    let value = f64::from_le_bytes([
                        bytes[0], bytes[1], bytes[2], bytes[3],
                        bytes[4], bytes[5], bytes[6], bytes[7],
                    ]);
                    vector.push(value);
                }
                Ok(SqlValue::Vector(vector))
            }
            _ => Err(crate::Error::Other(format!("Unknown type code: {}", type_code))),
        }
    }

    /// Get record size from schema
    pub fn get_record_size(&self, schema: &TableSchema) -> Result<usize> {
        Ok(schema.columns.iter().map(|col| col.storage_size).sum())
    }

    /// Get a single column value by name (zero-copy) using embedded metadata
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

    /// Get a single column value by index (zero-copy) using pre-computed metadata
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

    /// Get multiple columns by indices (zero-copy) using pre-computed metadata
    pub fn get_columns_by_indices_with_metadata(
        &self,
        data: &[u8],
        schema: &TableSchema,
        column_indices: &[usize],
    ) -> Result<Vec<SqlValue>> {
        let mut result = Vec::with_capacity(column_indices.len());
        for &column_index in column_indices {
            result.push(self.get_column_by_index_with_metadata(data, schema, column_index)?);
        }
        Ok(result)
    }

    /// Evaluate a condition (zero-copy where possible)
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
                // For simple column references, try to get the value directly
                if let crate::parser::Expression::Column(column_name) = left {
                    if let Ok(left_val) = self.get_column_value_with_metadata(data, schema, column_name) {
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
                                crate::parser::ComparisonOperator::Like => Ok(crate::sql_utils::compare_values(&SqlValue::Text(l.clone()), &crate::parser::ComparisonOperator::Like, &SqlValue::Text(r.clone()))),
                            },
                            _ => Ok(false),
                        }
                    } else {
                        // Fallback to full deserialization for complex cases
                        let row_data = self.deserialize_row_full_with_metadata(data, schema)?;
                        Ok(evaluate_condition(condition, &row_data))
                    }
                } else {
                    // For complex expressions (like function calls), fall back to full deserialization
                    let row_data = self.deserialize_row_full_with_metadata(data, schema)?;
                    Ok(evaluate_condition(condition, &row_data))
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
                    Ok(evaluate_condition(condition, &row_data))
                }
            }
            _ => {
                // For complex conditions, fall back to full deserialization
                let row_data = self.deserialize_row_full_with_metadata(data, schema)?;
                Ok(evaluate_condition(condition, &row_data))
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::parser::{DataType, ColumnConstraint};

    fn create_test_schema() -> TableSchema {
        TableSchema {
            name: "test_table".to_string(),
            columns: vec![
                crate::query_processor::ColumnInfo {
                    name: "id".to_string(),
                    data_type: DataType::Integer,
                    constraints: vec![ColumnConstraint::PrimaryKey],
                    storage_offset: 0,
                    storage_size: 8,
                    storage_type_code: 1,
                },
                crate::query_processor::ColumnInfo {
                    name: "name".to_string(),
                    data_type: DataType::Text(Some(32)),
                    constraints: vec![],
                    storage_offset: 8,
                    storage_size: 32,
                    storage_type_code: 3,
                },
                crate::query_processor::ColumnInfo {
                    name: "score".to_string(),
                    data_type: DataType::Real,
                    constraints: vec![],
                    storage_offset: 40,
                    storage_size: 8,
                    storage_type_code: 2,
                },
            ],
            indexes: vec![],
        }
    }

    #[test]
    fn test_serialize_deserialize_round_trip() {
        let storage = StorageFormat::new();
        let schema = create_test_schema();
        
        let mut row_data = HashMap::new();
        row_data.insert("id".to_string(), SqlValue::Integer(1));
        row_data.insert("name".to_string(), SqlValue::Text("test".to_string()));
        row_data.insert("score".to_string(), SqlValue::Real(3.14));
        
        let serialized = storage.serialize_row(&row_data, &schema).unwrap();
        let deserialized = storage.deserialize_row_full(&serialized, &schema).unwrap();
        
        assert_eq!(row_data, deserialized);
    }

    #[test]
    fn test_partial_column_deserialization() {
        let storage = StorageFormat::new();
        let schema = create_test_schema();
        
        let mut row_data = HashMap::new();
        row_data.insert("id".to_string(), SqlValue::Integer(1));
        row_data.insert("name".to_string(), SqlValue::Text("test".to_string()));
        row_data.insert("score".to_string(), SqlValue::Real(3.14));
        
        let serialized = storage.serialize_row(&row_data, &schema).unwrap();
        
        let id_value = storage.get_column_value(&serialized, &schema, "id").unwrap();
        assert_eq!(id_value, SqlValue::Integer(1));
        
        let name_value = storage.get_column_value(&serialized, &schema, "name").unwrap();
        assert_eq!(name_value, SqlValue::Text("test".to_string()));
        
        let score_value = storage.get_column_value(&serialized, &schema, "score").unwrap();
        assert_eq!(score_value, SqlValue::Real(3.14));
    }
}
