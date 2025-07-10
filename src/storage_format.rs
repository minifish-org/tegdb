use crate::parser::SqlValue;
use crate::query::TableSchema;
use crate::Result;
use std::collections::HashMap;

/// Storage configuration for TegDB
///
/// TegDB uses a SQLite-inspired native binary row format for optimal performance.
/// This provides:
/// - Direct column access without full deserialization
/// - Compact variable-length encoding
/// - Type information embedded in the record
/// - Skip unused columns during scanning
#[derive(Clone, Debug)]
pub struct StorageFormat;

/// Compact type codes for column types (similar to SQLite's serial types)
#[derive(Debug, Clone, Copy, PartialEq)]
#[repr(u8)]
pub enum TypeCode {
    Null = 0,
    Integer1 = 1, // 1-byte integer
    Integer2 = 2, // 2-byte integer
    Integer4 = 3, // 4-byte integer
    Integer8 = 4, // 8-byte integer
    Real = 5,     // 8-byte float
    Text0 = 12,   // Empty text (type codes 12+ are text with length = (code-12)/2)
    Blob0 = 13,   // Empty blob (type codes 13+ with odd numbers are blobs)
}

/// Column access information for efficient lookups
#[derive(Debug, Clone)]
pub struct ColumnInfo {
    pub offset: usize, // Byte offset in the record
    pub type_code: u8, // Type code from header
    pub size: usize,   // Size in bytes
}

/// Parsed record header for efficient column access
#[derive(Debug, Clone)]
pub struct RecordHeader {
    pub record_size: usize,
    pub header_size: usize,
    pub columns: Vec<ColumnInfo>,
}

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
        let mut buffer = Vec::with_capacity(256);
        let mut header_buffer = Vec::with_capacity(64);
        let mut data_buffer = Vec::with_capacity(192);

        // Build data and collect type information
        let mut type_codes = Vec::new();

        for column in &schema.columns {
            let value = row_data.get(&column.name).unwrap_or(&SqlValue::Null);
            let type_code = Self::serialize_value(value, &mut data_buffer)?;
            type_codes.push(type_code);
        }

        // Build header: header_size + type_codes
        let header_size_bytes = Self::encode_varint(type_codes.len() + 1); // +1 for header_size itself
        header_buffer.extend_from_slice(&header_size_bytes);
        header_buffer.extend_from_slice(&type_codes);

        // Calculate total record size
        let record_size = header_buffer.len() + data_buffer.len();
        let record_size_bytes = Self::encode_varint(record_size);

        // Assemble final record: record_size + header + data
        buffer.extend_from_slice(&record_size_bytes);
        buffer.extend_from_slice(&header_buffer);
        buffer.extend_from_slice(&data_buffer);

        Ok(buffer)
    }

    /// Deserialize a complete row using the native binary format
    pub fn deserialize_row(
        &self,
        data: &[u8],
        schema: &TableSchema,
    ) -> Result<HashMap<String, SqlValue>> {
        let header = Self::parse_header(data, schema)?;
        let mut row = HashMap::with_capacity(schema.columns.len());

        for (i, column_info) in header.columns.iter().enumerate() {
            let value = Self::deserialize_column_at_offset(data, column_info)?;
            row.insert(schema.columns[i].name.clone(), value);
        }

        Ok(row)
    }

    /// Deserialize only specific columns (major optimization!)
    pub fn deserialize_columns(
        &self,
        data: &[u8],
        schema: &TableSchema,
        column_names: &[String],
    ) -> Result<Vec<SqlValue>> {
        let header = Self::parse_header(data, schema)?;
        let mut result = Vec::with_capacity(column_names.len());

        // Create a map from column name to index for faster lookups
        let column_index_map: HashMap<_, _> = schema
            .columns
            .iter()
            .enumerate()
            .map(|(i, col)| (col.name.as_str(), i))
            .collect();

        for col_name in column_names {
            if let Some(&column_index) = column_index_map.get(col_name.as_str()) {
                if let Some(column_info) = header.columns.get(column_index) {
                    let value = Self::deserialize_column_at_offset(data, column_info)?;
                    result.push(value);
                } else {
                    result.push(SqlValue::Null);
                }
            } else {
                result.push(SqlValue::Null);
            }
        }

        Ok(result)
    }

    /// Check if row matches condition without full deserialization
    pub fn matches_condition(
        &self,
        data: &[u8],
        schema: &TableSchema,
        condition: &crate::parser::Condition,
    ) -> Result<bool> {
        // For now, we'll implement a simple version that deserializes only referenced columns
        let referenced_columns = Self::extract_referenced_columns(condition);
        let values = self.deserialize_columns(data, schema, &referenced_columns)?;

        // Create a temporary map for condition evaluation
        let mut row_map = HashMap::new();
        for (i, col_name) in referenced_columns.iter().enumerate() {
            if let Some(value) = values.get(i) {
                row_map.insert(col_name.clone(), value.clone());
            }
        }

        Ok(Self::evaluate_condition_on_map(condition, &row_map))
    }

    /// Parse record header for efficient column access
    fn parse_header(data: &[u8], schema: &TableSchema) -> Result<RecordHeader> {
        let mut cursor = 0;

        // Read record size
        let (record_size, consumed) = Self::decode_varint(&data[cursor..])?;
        cursor += consumed;

        // Read header size
        let (header_size, consumed) = Self::decode_varint(&data[cursor..])?;
        cursor += consumed;

        // Read type codes
        let mut columns = Vec::with_capacity(schema.columns.len());
        let mut data_offset = cursor + header_size - 1; // Start of data section

        for _ in &schema.columns {
            if cursor >= data.len() {
                return Err(crate::Error::Other("Truncated record header".to_string()));
            }

            let type_code = data[cursor];
            cursor += 1;

            let column_size = Self::get_column_size(type_code);

            columns.push(ColumnInfo {
                offset: data_offset,
                type_code,
                size: column_size,
            });

            data_offset += column_size;
        }

        Ok(RecordHeader {
            record_size,
            header_size,
            columns,
        })
    }

    /// Deserialize a single column at a specific offset
    fn deserialize_column_at_offset(data: &[u8], column_info: &ColumnInfo) -> Result<SqlValue> {
        let start = column_info.offset;
        let end = start + column_info.size;

        if end > data.len() {
            return Err(crate::Error::Other(
                "Column data extends beyond record".to_string(),
            ));
        }

        let column_data = &data[start..end];

        match column_info.type_code {
            0 => Ok(SqlValue::Null),
            1 => Ok(SqlValue::Integer(column_data[0] as i64)),
            2 => Ok(SqlValue::Integer(
                i16::from_le_bytes([column_data[0], column_data[1]]) as i64,
            )),
            3 => Ok(SqlValue::Integer(i32::from_le_bytes([
                column_data[0],
                column_data[1],
                column_data[2],
                column_data[3],
            ]) as i64)),
            4 => Ok(SqlValue::Integer(i64::from_le_bytes([
                column_data[0],
                column_data[1],
                column_data[2],
                column_data[3],
                column_data[4],
                column_data[5],
                column_data[6],
                column_data[7],
            ]))),
            5 => Ok(SqlValue::Real(f64::from_le_bytes([
                column_data[0],
                column_data[1],
                column_data[2],
                column_data[3],
                column_data[4],
                column_data[5],
                column_data[6],
                column_data[7],
            ]))),
            code if code >= 12 && code % 2 == 0 => {
                // Text data
                let text = String::from_utf8_lossy(column_data).to_string();
                Ok(SqlValue::Text(text))
            }
            code if code >= 13 && code % 2 == 1 => {
                // Blob data (treat as text for now)
                let text = String::from_utf8_lossy(column_data).to_string();
                Ok(SqlValue::Text(text))
            }
            _ => Err(crate::Error::Other(format!(
                "Unknown type code: {}",
                column_info.type_code
            ))),
        }
    }

    /// Serialize a single value and return its type code
    fn serialize_value(value: &SqlValue, buffer: &mut Vec<u8>) -> Result<u8> {
        match value {
            SqlValue::Null => Ok(0),
            SqlValue::Integer(i) => {
                // Choose the most compact representation
                if *i >= 0 && *i <= 255 {
                    buffer.push(*i as u8);
                    Ok(1)
                } else if *i >= i16::MIN as i64 && *i <= i16::MAX as i64 {
                    buffer.extend_from_slice(&(*i as i16).to_le_bytes());
                    Ok(2)
                } else if *i >= i32::MIN as i64 && *i <= i32::MAX as i64 {
                    buffer.extend_from_slice(&(*i as i32).to_le_bytes());
                    Ok(3)
                } else {
                    buffer.extend_from_slice(&i.to_le_bytes());
                    Ok(4)
                }
            }
            SqlValue::Real(r) => {
                buffer.extend_from_slice(&r.to_le_bytes());
                Ok(5)
            }
            SqlValue::Text(t) => {
                let bytes = t.as_bytes();
                buffer.extend_from_slice(bytes);
                // Type code for text: 12 + 2 * length
                let type_code = std::cmp::min(255, 12 + 2 * bytes.len()) as u8;
                Ok(type_code)
            }
        }
    }

    /// Get the size in bytes for a given type code
    fn get_column_size(type_code: u8) -> usize {
        match type_code {
            0 => 0,                                                          // NULL
            1 => 1,                                                          // 1-byte integer
            2 => 2,                                                          // 2-byte integer
            3 => 4,                                                          // 4-byte integer
            4 => 8,                                                          // 8-byte integer
            5 => 8,                                                          // 8-byte real
            code if code >= 12 && code % 2 == 0 => (code - 12) as usize / 2, // Text
            code if code >= 13 && code % 2 == 1 => (code - 13) as usize / 2, // Blob
            _ => 0,
        }
    }

    /// Encode a variable-length integer (similar to SQLite)
    fn encode_varint(mut value: usize) -> Vec<u8> {
        let mut result = Vec::new();

        while value >= 128 {
            result.push((value & 0x7F) as u8 | 0x80);
            value >>= 7;
        }
        result.push(value as u8);

        result
    }

    /// Decode a variable-length integer
    fn decode_varint(data: &[u8]) -> Result<(usize, usize)> {
        let mut result = 0;
        let mut shift = 0;
        let mut bytes_consumed = 0;

        for &byte in data {
            bytes_consumed += 1;
            result |= ((byte & 0x7F) as usize) << shift;

            if byte & 0x80 == 0 {
                return Ok((result, bytes_consumed));
            }

            shift += 7;
            if shift >= 64 {
                return Err(crate::Error::Other("Varint too long".to_string()));
            }
        }

        Err(crate::Error::Other("Incomplete varint".to_string()))
    }

    /// Extract column names referenced in a condition
    fn extract_referenced_columns(condition: &crate::parser::Condition) -> Vec<String> {
        let mut columns = Vec::new();
        Self::collect_referenced_columns(condition, &mut columns);
        columns.sort();
        columns.dedup();
        columns
    }

    /// Recursively collect column names from a condition
    fn collect_referenced_columns(condition: &crate::parser::Condition, columns: &mut Vec<String>) {
        match condition {
            crate::parser::Condition::Comparison { left, .. } => {
                columns.push(left.clone());
            }
            crate::parser::Condition::And(left, right)
            | crate::parser::Condition::Or(left, right) => {
                Self::collect_referenced_columns(left, columns);
                Self::collect_referenced_columns(right, columns);
            }
        }
    }

    /// Evaluate a condition on a column map
    fn evaluate_condition_on_map(
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
                Self::evaluate_condition_on_map(left, row_data)
                    && Self::evaluate_condition_on_map(right, row_data)
            }
            crate::parser::Condition::Or(left, right) => {
                Self::evaluate_condition_on_map(left, row_data)
                    || Self::evaluate_condition_on_map(right, row_data)
            }
        }
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::parser::{DataType, SqlValue};
    use crate::query::{ColumnInfo, TableSchema};
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
                    data_type: DataType::Text,
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
