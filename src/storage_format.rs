use crate::executor::TableSchema;
use crate::parser::{DataType, SqlValue};
use crate::Result;
use std::collections::HashMap;

/// Ultra-optimized storage format for TegDB
///
/// This format is designed for maximum performance with fixed-length columns.
/// All text and blob columns must have a specified length in the schema.
/// This enables:
/// - Direct offset-based column access
/// - Zero-copy deserialization
/// - Predictable record sizes
/// - Maximum cache efficiency
#[derive(Clone, Debug)]
pub struct StorageFormat;

/// Type codes for the new fixed-length format
#[derive(Debug, Clone, Copy, PartialEq)]
#[repr(u8)]
pub enum TypeCode {
    Null = 0,
    Integer = 1,    // 8-byte i64
    Real = 2,       // 8-byte f64
    TextFixed = 3,  // Fixed-length text (padded with nulls)
}

/// Column metadata for fast access
#[derive(Debug, Clone)]
pub struct ColumnMetadata {
    pub offset: usize,     // Byte offset in record
    pub size: usize,       // Size in bytes
    pub type_code: u8,     // Type code
    pub is_nullable: bool, // Whether column can be null
}

/// Table metadata for ultra-fast operations
#[derive(Debug, Clone)]
pub struct TableMetadata {
    pub record_size: usize,           // Total size of each record
    pub column_metadata: Vec<ColumnMetadata>,
    pub column_map: HashMap<String, usize>, // Column name to index mapping
}



impl Default for StorageFormat {
    fn default() -> Self {
        StorageFormat
    }
}

impl StorageFormat {
    /// Create a new storage format
    pub fn new() -> Self {
        StorageFormat
    }

    /// Create native storage format (for API compatibility)
    pub fn native() -> Self {
        StorageFormat
    }

    /// Compute table metadata for ultra-fast operations
    pub fn compute_table_metadata(schema: &TableSchema) -> Result<TableMetadata> {
        let mut column_metadata = Vec::with_capacity(schema.columns.len());
        let mut column_map = HashMap::new();
        let mut current_offset = 0;

        for (index, column) in schema.columns.iter().enumerate() {
            let (size, type_code) = Self::get_column_size_and_type(&column.data_type)?;
            
            column_metadata.push(ColumnMetadata {
                offset: current_offset,
                size,
                type_code,
                is_nullable: !column.constraints.contains(&crate::parser::ColumnConstraint::NotNull),
            });
            
            column_map.insert(column.name.clone(), index);
            current_offset += size;
        }

        Ok(TableMetadata {
            record_size: current_offset,
            column_metadata,
            column_map,
        })
    }

    /// Get column size and type code for a data type
    fn get_column_size_and_type(data_type: &DataType) -> Result<(usize, u8)> {
        match data_type {
            DataType::Integer => Ok((8, TypeCode::Integer as u8)),
            DataType::Real => Ok((8, TypeCode::Real as u8)),
            DataType::Text(Some(length)) => Ok((*length, TypeCode::TextFixed as u8)),
            DataType::Text(None) => Err(crate::Error::Other("Text columns must specify a length (e.g., TEXT(10))".to_string())),
        }
    }

    /// Serialize a row using the ultra-optimized fixed-length format
    pub fn serialize_row(
        &self,
        row_data: &HashMap<String, SqlValue>,
        schema: &TableSchema,
    ) -> Result<Vec<u8>> {
        let metadata = Self::compute_table_metadata(schema)?;
        let mut buffer = vec![0u8; metadata.record_size]; // Pre-allocate exact size

        for (index, column) in schema.columns.iter().enumerate() {
            let value = row_data.get(&column.name).unwrap_or(&SqlValue::Null);
            let column_meta = &metadata.column_metadata[index];
            
            Self::serialize_value_at_offset(
                value,
                &mut buffer,
                column_meta.offset,
                column_meta.size,
                column_meta.type_code,
            )?;
        }

        Ok(buffer)
    }

    /// Serialize a value at a specific offset
    fn serialize_value_at_offset(
        value: &SqlValue,
        buffer: &mut [u8],
        offset: usize,
        size: usize,
        type_code: u8,
    ) -> Result<()> {
        match (value, type_code) {
            (SqlValue::Null, _) => {
                // For null values, we could use a null bitmap or just leave as zeros
                // For now, we'll use zeros to indicate null
                buffer[offset..offset + size].fill(0);
            }
            (SqlValue::Integer(i), 1) => { // TypeCode::Integer
                buffer[offset..offset + 8].copy_from_slice(&i.to_le_bytes());
            }
            (SqlValue::Real(r), 2) => { // TypeCode::Real
                buffer[offset..offset + 8].copy_from_slice(&r.to_le_bytes());
            }
            (SqlValue::Text(s), 3) => { // TypeCode::TextFixed
                let bytes = s.as_bytes();
                let copy_len = bytes.len().min(size);
                buffer[offset..offset + copy_len].copy_from_slice(&bytes[..copy_len]);
                // Pad with nulls if needed
                if copy_len < size {
                    buffer[offset + copy_len..offset + size].fill(0);
                }
            }

            _ => return Err(crate::Error::Other("Type mismatch during serialization".to_string())),
        }
        Ok(())
    }

    /// Deserialize a row using ultra-fast direct access
    pub fn deserialize_row(
        &self,
        data: &[u8],
        schema: &TableSchema,
    ) -> Result<HashMap<String, SqlValue>> {
        let metadata = Self::compute_table_metadata(schema)?;
        let mut result = HashMap::new();

        for (index, column) in schema.columns.iter().enumerate() {
            let column_meta = &metadata.column_metadata[index];
            let value = Self::deserialize_value_at_offset(
                data,
                column_meta.offset,
                column_meta.size,
                column_meta.type_code,
            )?;
            result.insert(column.name.clone(), value);
        }

        Ok(result)
    }

    /// Deserialize specific columns (for LIMIT optimization)
    pub fn deserialize_columns(
        &self,
        data: &[u8],
        schema: &TableSchema,
        column_names: &[String],
    ) -> Result<Vec<SqlValue>> {
        let metadata = Self::compute_table_metadata(schema)?;
        let mut result = Vec::with_capacity(column_names.len());

        for column_name in column_names {
            if let Some(&index) = metadata.column_map.get(column_name) {
                let column_meta = &metadata.column_metadata[index];
                let value = Self::deserialize_value_at_offset(
                    data,
                    column_meta.offset,
                    column_meta.size,
                    column_meta.type_code,
                )?;
                result.push(value);
            } else {
                return Err(crate::Error::Other(format!("Column '{}' not found", column_name)));
            }
        }

        Ok(result)
    }

    /// Ultra-fast deserialization at specific offset
    fn deserialize_value_at_offset(
        data: &[u8],
        offset: usize,
        size: usize,
        type_code: u8,
    ) -> Result<SqlValue> {
        match type_code {
            1 => { // TypeCode::Integer
                let bytes = &data[offset..offset + 8];
                let value = i64::from_le_bytes(bytes.try_into().unwrap());
                Ok(SqlValue::Integer(value))
            }
            2 => { // TypeCode::Real
                let bytes = &data[offset..offset + 8];
                let value = f64::from_le_bytes(bytes.try_into().unwrap());
                Ok(SqlValue::Real(value))
            }
            3 => { // TypeCode::TextFixed
                let bytes = &data[offset..offset + size];
                // Find the first null byte to determine actual string length
                let actual_len = bytes.iter().position(|&b| b == 0).unwrap_or(size);
                let text_bytes = &bytes[..actual_len];
                let text = String::from_utf8_lossy(text_bytes).to_string();
                Ok(SqlValue::Text(text))
            }

            _ => Err(crate::Error::Other("Invalid type code".to_string())),
        }
    }

    /// Ultra-fast column access by index
    pub fn deserialize_column_by_index(
        &self,
        data: &[u8],
        schema: &TableSchema,
        column_index: usize,
    ) -> Result<SqlValue> {
        let metadata = Self::compute_table_metadata(schema)?;
        if column_index >= metadata.column_metadata.len() {
            return Err(crate::Error::Other("Column index out of bounds".to_string()));
        }

        let column_meta = &metadata.column_metadata[column_index];
        Self::deserialize_value_at_offset(
            data,
            column_meta.offset,
            column_meta.size,
            column_meta.type_code,
        )
    }

    /// Deserialize specific columns by index (for backward compatibility)
    pub fn deserialize_column_indices(
        &self,
        data: &[u8],
        schema: &TableSchema,
        column_indices: &[usize],
    ) -> Result<Vec<SqlValue>> {
        let metadata = Self::compute_table_metadata(schema)?;
        let mut result = Vec::with_capacity(column_indices.len());

        for &column_index in column_indices {
            if column_index >= metadata.column_metadata.len() {
                return Err(crate::Error::Other("Column index out of bounds".to_string()));
            }

            let column_meta = &metadata.column_metadata[column_index];
            let value = Self::deserialize_value_at_offset(
                data,
                column_meta.offset,
                column_meta.size,
                column_meta.type_code,
            )?;
            result.push(value);
        }

        Ok(result)
    }

    /// Get record size for a schema
    pub fn get_record_size(schema: &TableSchema) -> Result<usize> {
        let metadata = Self::compute_table_metadata(schema)?;
        Ok(metadata.record_size)
    }

    /// Check if a value matches a condition (for WHERE clauses)
    pub fn matches_condition(
        &self,
        data: &[u8],
        schema: &TableSchema,
        condition: &crate::parser::Condition,
    ) -> Result<bool> {
        // For now, deserialize the row and use the existing condition evaluation
        // This could be optimized further with direct byte comparisons
        let row_data = self.deserialize_row(data, schema)?;
        Ok(evaluate_condition_on_row(condition, &row_data))
    }
}

/// Evaluate a condition on row data
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
            let left_val = row_data.get(left);
            match (left_val, right) {
                (Some(SqlValue::Integer(l)), SqlValue::Integer(r)) => match operator {
                    crate::parser::ComparisonOperator::Equal => l == r,
                    crate::parser::ComparisonOperator::NotEqual => l != r,
                    crate::parser::ComparisonOperator::LessThan => l < r,
                    crate::parser::ComparisonOperator::LessThanOrEqual => l <= r,
                    crate::parser::ComparisonOperator::GreaterThan => l > r,
                    crate::parser::ComparisonOperator::GreaterThanOrEqual => l >= r,
                    _ => false,
                },
                (Some(SqlValue::Real(l)), SqlValue::Real(r)) => match operator {
                    crate::parser::ComparisonOperator::Equal => (l - r).abs() < f64::EPSILON,
                    crate::parser::ComparisonOperator::NotEqual => (l - r).abs() >= f64::EPSILON,
                    crate::parser::ComparisonOperator::LessThan => l < r,
                    crate::parser::ComparisonOperator::LessThanOrEqual => l <= r,
                    crate::parser::ComparisonOperator::GreaterThan => l > r,
                    crate::parser::ComparisonOperator::GreaterThanOrEqual => l >= r,
                    _ => false,
                },
                (Some(SqlValue::Text(l)), SqlValue::Text(r)) => match operator {
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
    use crate::executor::{ColumnInfo, TableSchema};
    use crate::parser::{DataType, SqlValue};
    use std::collections::HashMap;

    fn create_test_schema() -> TableSchema {
        TableSchema {
            name: "test_table".to_string(),
            columns: vec![
                ColumnInfo {
                    name: "id".to_string(),
                    data_type: DataType::Integer,
                    constraints: vec![],
                },
                ColumnInfo {
                    name: "name".to_string(),
                    data_type: DataType::Text(Some(10)),
                    constraints: vec![],
                },
                ColumnInfo {
                    name: "score".to_string(),
                    data_type: DataType::Real,
                    constraints: vec![],
                },
            ],
        }
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
        let deserialized = storage.deserialize_row(&serialized, &schema).unwrap();

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
        let columns = vec!["name".to_string(), "score".to_string()];
        let values = storage
            .deserialize_columns(&serialized, &schema, &columns)
            .unwrap();

        assert_eq!(values.len(), 2);
        assert_eq!(values[0], SqlValue::Text("Bob".to_string()));
        assert_eq!(values[1], SqlValue::Real(87.2));
    }
}
