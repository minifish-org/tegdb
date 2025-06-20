use crate::parser::SqlValue;
use std::collections::HashMap;

/// Binary row serializer for efficient data storage
pub struct BinaryRowSerializer;

impl BinaryRowSerializer {
    /// Serialize a row to binary format
    pub fn serialize(row: &HashMap<String, SqlValue>) -> Vec<u8> {
        let mut buffer = Vec::with_capacity(256);
        
        // Write number of columns
        buffer.extend_from_slice(&(row.len() as u32).to_le_bytes());
        
        for (key, value) in row {
            // Write key length and key
            buffer.extend_from_slice(&(key.len() as u32).to_le_bytes());
            buffer.extend_from_slice(key.as_bytes());
            
            // Write value with type tag
            match value {
                SqlValue::Integer(i) => {
                    buffer.push(1); // type tag for integer
                    buffer.extend_from_slice(&i.to_le_bytes());
                },
                SqlValue::Real(r) => {
                    buffer.push(2); // type tag for real
                    buffer.extend_from_slice(&r.to_le_bytes());
                },
                SqlValue::Text(t) => {
                    buffer.push(3); // type tag for text
                    buffer.extend_from_slice(&(t.len() as u32).to_le_bytes());
                    buffer.extend_from_slice(t.as_bytes());
                },
                SqlValue::Null => {
                    buffer.push(0); // type tag for null
                },
            }
        }
        
        buffer
    }
    
    /// Deserialize a row from binary format
    pub fn deserialize(data: &[u8]) -> crate::Result<HashMap<String, SqlValue>> {
        let mut cursor = 0;
        let mut row = HashMap::new();
        
        if data.len() < 4 {
            return Err(crate::Error::Other("Invalid data length".to_string()));
        }
        
        // Read number of columns
        let num_cols = u32::from_le_bytes([
            data[cursor], data[cursor+1], data[cursor+2], data[cursor+3]
        ]) as usize;
        cursor += 4;
        
        for _ in 0..num_cols {
            // Read key
            if cursor + 4 > data.len() {
                return Err(crate::Error::Other("Unexpected end of data reading key length".to_string()));
            }
            
            let key_len = u32::from_le_bytes([
                data[cursor], data[cursor+1], data[cursor+2], data[cursor+3]
            ]) as usize;
            cursor += 4;
            
            if cursor + key_len > data.len() {
                return Err(crate::Error::Other("Unexpected end of data reading key".to_string()));
            }
            
            let key = String::from_utf8_lossy(&data[cursor..cursor+key_len]).to_string();
            cursor += key_len;
            
            // Read value
            if cursor >= data.len() {
                return Err(crate::Error::Other("Unexpected end of data reading value type".to_string()));
            }
            
            let value_type = data[cursor];
            cursor += 1;
            
            let value = match value_type {
                1 => { // Integer
                    if cursor + 8 > data.len() {
                        return Err(crate::Error::Other("Unexpected end of data reading integer".to_string()));
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
                        return Err(crate::Error::Other("Unexpected end of data reading real".to_string()));
                    }
                    let val = f64::from_le_bytes([
                        data[cursor], data[cursor+1], data[cursor+2], data[cursor+3],
                        data[cursor+4], data[cursor+5], data[cursor+6], data[cursor+7]
                    ]);
                    cursor += 8;
                    SqlValue::Real(val)
                },
                3 => { // Text
                    if cursor + 4 > data.len() {
                        return Err(crate::Error::Other("Unexpected end of data reading text length".to_string()));
                    }
                    let text_len = u32::from_le_bytes([
                        data[cursor], data[cursor+1], data[cursor+2], data[cursor+3]
                    ]) as usize;
                    cursor += 4;
                    
                    if cursor + text_len > data.len() {
                        return Err(crate::Error::Other("Unexpected end of data reading text".to_string()));
                    }
                    
                    let text = String::from_utf8_lossy(&data[cursor..cursor+text_len]).to_string();
                    cursor += text_len;
                    SqlValue::Text(text)
                },
                0 => SqlValue::Null,
                _ => return Err(crate::Error::Other(format!("Unknown value type: {}", value_type))),
            };
            
            row.insert(key, value);
        }
        
        Ok(row)
    }
}
