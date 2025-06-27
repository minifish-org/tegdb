use crate::parser::SqlValue;
use std::collections::HashMap;

/// Optimized binary row serializer with better performance characteristics
pub struct OptimizedRowSerializer;

impl OptimizedRowSerializer {
    /// Serialize a row with improved efficiency
    pub fn serialize_optimized(row: &HashMap<String, SqlValue>) -> Vec<u8> {
        // Pre-calculate total size to avoid reallocations
        let estimated_size = Self::estimate_serialized_size(row);
        let mut buffer = Vec::with_capacity(estimated_size);
        
        // Write number of columns (use smaller encoding for typical case)
        let num_cols = row.len();
        if num_cols < 255 {
            buffer.push(num_cols as u8);
        } else {
            buffer.push(255);
            buffer.extend_from_slice(&(num_cols as u32).to_le_bytes());
        }
        
        // Write data more efficiently
        for (key, value) in row {
            Self::serialize_key_value_optimized(key, value, &mut buffer);
        }
        
        buffer
    }
    
    /// Deserialize only specific columns (major optimization!)
    pub fn deserialize_columns_only(
        data: &[u8], 
        requested_columns: &[String]
    ) -> crate::Result<HashMap<String, SqlValue>> {
        let mut cursor = 0;
        let mut result = HashMap::with_capacity(requested_columns.len());
        
        if data.is_empty() {
            return Ok(result);
        }
        
        // Read number of columns
        let num_cols = if data[cursor] < 255 {
            let count = data[cursor] as usize;
            cursor += 1;
            count
        } else {
            cursor += 1;
            if cursor + 4 > data.len() {
                return Err(crate::Error::Other("Invalid column count".to_string()));
            }
            let count = u32::from_le_bytes([
                data[cursor], data[cursor+1], data[cursor+2], data[cursor+3]
            ]) as usize;
            cursor += 4;
            count
        };
        
        // Create a set for faster lookup
        let requested_set: std::collections::HashSet<&String> = requested_columns.iter().collect();
        
        // Skip columns we don't need (major performance improvement!)
        for _ in 0..num_cols {
            if cursor >= data.len() {
                break;
            }
            
            // Read key length and key
            let (key, key_consumed) = Self::read_key(&data[cursor..])?;
            cursor += key_consumed;
            
            // Check if we need this column
            if requested_set.contains(&key) {
                // Deserialize value
                let (value, value_consumed) = Self::read_value(&data[cursor..])?;
                cursor += value_consumed;
                result.insert(key, value);
            } else {
                // Skip value efficiently
                let value_consumed = Self::skip_value(&data[cursor..])?;
                cursor += value_consumed;
            }
        }
        
        Ok(result)
    }
    
    /// Estimate serialized size to pre-allocate buffer
    fn estimate_serialized_size(row: &HashMap<String, SqlValue>) -> usize {
        let mut size = 5; // Conservative estimate for header
        
        for (key, value) in row {
            size += 4 + key.len(); // Key length + key
            size += match value {
                SqlValue::Null => 1,
                SqlValue::Integer(_) => 9, // type + 8 bytes
                SqlValue::Real(_) => 9,    // type + 8 bytes
                SqlValue::Text(t) => 5 + t.len(), // type + length + data
            };
        }
        
        size
    }
    
    /// Serialize key-value pair with optimizations
    fn serialize_key_value_optimized(key: &str, value: &SqlValue, buffer: &mut Vec<u8>) {
        // Write key length and key (use compact encoding for short keys)
        let key_bytes = key.as_bytes();
        if key_bytes.len() < 255 {
            buffer.push(key_bytes.len() as u8);
        } else {
            buffer.push(255);
            buffer.extend_from_slice(&(key_bytes.len() as u32).to_le_bytes());
        }
        buffer.extend_from_slice(key_bytes);
        
        // Write value with optimized type encoding
        match value {
            SqlValue::Integer(i) => {
                buffer.push(1);
                buffer.extend_from_slice(&i.to_le_bytes());
            },
            SqlValue::Real(r) => {
                buffer.push(2);
                buffer.extend_from_slice(&r.to_le_bytes());
            },
            SqlValue::Text(t) => {
                buffer.push(3);
                let text_bytes = t.as_bytes();
                if text_bytes.len() < 255 {
                    buffer.push(text_bytes.len() as u8);
                } else {
                    buffer.push(255);
                    buffer.extend_from_slice(&(text_bytes.len() as u32).to_le_bytes());
                }
                buffer.extend_from_slice(text_bytes);
            },
            SqlValue::Null => {
                buffer.push(0);
            },
        }
    }
    
    /// Read key from buffer
    fn read_key(data: &[u8]) -> crate::Result<(String, usize)> {
        if data.is_empty() {
            return Err(crate::Error::Other("Empty data reading key".to_string()));
        }
        
        let mut cursor = 0;
        let key_len = if data[cursor] < 255 {
            let len = data[cursor] as usize;
            cursor += 1;
            len
        } else {
            cursor += 1;
            if cursor + 4 > data.len() {
                return Err(crate::Error::Other("Invalid key length".to_string()));
            }
            let len = u32::from_le_bytes([
                data[cursor], data[cursor+1], data[cursor+2], data[cursor+3]
            ]) as usize;
            cursor += 4;
            len
        };
        
        if cursor + key_len > data.len() {
            return Err(crate::Error::Other("Key data truncated".to_string()));
        }
        
        let key = String::from_utf8_lossy(&data[cursor..cursor+key_len]).to_string();
        cursor += key_len;
        
        Ok((key, cursor))
    }
    
    /// Read value from buffer
    fn read_value(data: &[u8]) -> crate::Result<(SqlValue, usize)> {
        if data.is_empty() {
            return Err(crate::Error::Other("Empty data reading value".to_string()));
        }
        
        let mut cursor = 0;
        let value_type = data[cursor];
        cursor += 1;
        
        let value = match value_type {
            1 => { // Integer
                if cursor + 8 > data.len() {
                    return Err(crate::Error::Other("Integer data truncated".to_string()));
                }
                let val = i64::from_le_bytes([
                    data[cursor], data[cursor+1], data[cursor+2], data[cursor+3],
                    data[cursor+4], data[cursor+5], data[cursor+6], data[cursor+7]
                ]);
                cursor += 8;
                SqlValue::Integer(val)
            },
            2 => { // Real
                if cursor + 8 > data.len() {
                    return Err(crate::Error::Other("Real data truncated".to_string()));
                }
                let val = f64::from_le_bytes([
                    data[cursor], data[cursor+1], data[cursor+2], data[cursor+3],
                    data[cursor+4], data[cursor+5], data[cursor+6], data[cursor+7]
                ]);
                cursor += 8;
                SqlValue::Real(val)
            },
            3 => { // Text
                let text_len = if data.len() > cursor && data[cursor] < 255 {
                    let len = data[cursor] as usize;
                    cursor += 1;
                    len
                } else {
                    cursor += 1;
                    if cursor + 4 > data.len() {
                        return Err(crate::Error::Other("Text length truncated".to_string()));
                    }
                    let len = u32::from_le_bytes([
                        data[cursor], data[cursor+1], data[cursor+2], data[cursor+3]
                    ]) as usize;
                    cursor += 4;
                    len
                };
                
                if cursor + text_len > data.len() {
                    return Err(crate::Error::Other("Text data truncated".to_string()));
                }
                
                let text = String::from_utf8_lossy(&data[cursor..cursor+text_len]).to_string();
                cursor += text_len;
                SqlValue::Text(text)
            },
            0 => SqlValue::Null,
            _ => return Err(crate::Error::Other(format!("Unknown value type: {}", value_type))),
        };
        
        Ok((value, cursor))
    }
    
    /// Skip value without deserializing it (for performance)
    fn skip_value(data: &[u8]) -> crate::Result<usize> {
        if data.is_empty() {
            return Err(crate::Error::Other("Empty data skipping value".to_string()));
        }
        
        let mut cursor = 0;
        let value_type = data[cursor];
        cursor += 1;
        
        match value_type {
            1 | 2 => cursor += 8,  // Integer or Real - fixed 8 bytes
            3 => { // Text - variable length
                let text_len = if data.len() > cursor && data[cursor] < 255 {
                    let len = data[cursor] as usize;
                    cursor += 1;
                    len
                } else {
                    cursor += 1;
                    if cursor + 4 > data.len() {
                        return Err(crate::Error::Other("Text length truncated while skipping".to_string()));
                    }
                    let len = u32::from_le_bytes([
                        data[cursor], data[cursor+1], data[cursor+2], data[cursor+3]
                    ]) as usize;
                    cursor += 4;
                    len
                };
                cursor += text_len;
            },
            0 => {}, // Null - no additional data
            _ => return Err(crate::Error::Other(format!("Unknown value type while skipping: {}", value_type))),
        }
        
        Ok(cursor)
    }
}
