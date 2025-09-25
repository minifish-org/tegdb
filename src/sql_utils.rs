//! Shared utilities for SQL operations
//!
//! This module contains common functions used across the codebase for SQL value
//! comparison, condition evaluation, and other SQL-related operations.

use crate::parser::{ColumnConstraint, ComparisonOperator, Condition, DataType, SqlValue};
use crate::query_processor::{ColumnInfo, TableSchema};
use std::collections::HashMap;

/// Compare two SqlValues using the given operator
pub fn compare_values(left: &SqlValue, operator: &ComparisonOperator, right: &SqlValue) -> bool {
    use ComparisonOperator::*;

    match operator {
        Equal => left == right,
        NotEqual => left != right,
        LessThan => match (left, right) {
            (SqlValue::Integer(a), SqlValue::Integer(b)) => a < b,
            (SqlValue::Real(a), SqlValue::Real(b)) => a < b,
            (SqlValue::Integer(a), SqlValue::Real(b)) => (*a as f64) < *b,
            (SqlValue::Real(a), SqlValue::Integer(b)) => *a < (*b as f64),
            (SqlValue::Text(a), SqlValue::Text(b)) => a < b,
            _ => false,
        },
        LessThanOrEqual => match (left, right) {
            (SqlValue::Integer(a), SqlValue::Integer(b)) => a <= b,
            (SqlValue::Real(a), SqlValue::Real(b)) => a <= b,
            (SqlValue::Integer(a), SqlValue::Real(b)) => (*a as f64) <= *b,
            (SqlValue::Real(a), SqlValue::Integer(b)) => *a <= (*b as f64),
            (SqlValue::Text(a), SqlValue::Text(b)) => a <= b,
            _ => false,
        },
        GreaterThan => match (left, right) {
            (SqlValue::Integer(a), SqlValue::Integer(b)) => a > b,
            (SqlValue::Real(a), SqlValue::Real(b)) => a > b,
            (SqlValue::Integer(a), SqlValue::Real(b)) => (*a as f64) > *b,
            (SqlValue::Real(a), SqlValue::Integer(b)) => *a > (*b as f64),
            (SqlValue::Text(a), SqlValue::Text(b)) => a > b,
            _ => false,
        },
        GreaterThanOrEqual => match (left, right) {
            (SqlValue::Integer(a), SqlValue::Integer(b)) => a >= b,
            (SqlValue::Real(a), SqlValue::Real(b)) => a >= b,
            (SqlValue::Integer(a), SqlValue::Real(b)) => (*a as f64) >= *b,
            (SqlValue::Real(a), SqlValue::Integer(b)) => *a >= (*b as f64),
            (SqlValue::Text(a), SqlValue::Text(b)) => a >= b,
            _ => false,
        },
        Like => {
            match (left, right) {
                (SqlValue::Text(text), SqlValue::Text(pattern)) => {
                    // Simple LIKE pattern matching with % wildcards

                    if pattern == "%" {
                        return true; // % matches everything
                    }

                    if let Some(inner_pattern) =
                        pattern.strip_prefix('%').and_then(|s| s.strip_suffix('%'))
                    {
                        // %pattern% - contains pattern
                        text.contains(inner_pattern)
                    } else if let Some(inner_pattern) = pattern.strip_prefix('%') {
                        // %pattern - ends with pattern
                        text.ends_with(inner_pattern)
                    } else if let Some(inner_pattern) = pattern.strip_suffix('%') {
                        // pattern% - starts with pattern
                        text.starts_with(inner_pattern)
                    } else {
                        // exact match
                        text == pattern
                    }
                }
                _ => false,
            }
        }
    }
}

/// Evaluate condition against row data with optimized performance
pub fn evaluate_condition(condition: &Condition, row_data: &HashMap<String, SqlValue>) -> bool {
    match condition {
        Condition::Comparison {
            left,
            operator,
            right,
        } => {
            // Evaluate the left expression
            let row_value = match left.evaluate(row_data) {
                Ok(val) => val,
                Err(_e) => SqlValue::Null, // If evaluation fails, use null
            };
            compare_values(&row_value, operator, right)
        }
        Condition::Between { column, low, high } => {
            let row_value = row_data.get(column).unwrap_or(&SqlValue::Null);
            compare_values(row_value, &ComparisonOperator::GreaterThanOrEqual, low)
                && compare_values(row_value, &ComparisonOperator::LessThanOrEqual, high)
        }
        Condition::And(left, right) => {
            // Short-circuit evaluation for AND
            evaluate_condition(left, row_data) && evaluate_condition(right, row_data)
        }
        Condition::Or(left, right) => {
            // Short-circuit evaluation for OR
            evaluate_condition(left, row_data) || evaluate_condition(right, row_data)
        }
    }
}

/// Optimized schema parsing utility to eliminate duplication
/// Parses schema data from the format: "col1:DataType:constraints|col2:DataType:constraints|..."
pub fn parse_schema_data(table_name: &str, schema_data: &str) -> Option<TableSchema> {
    let mut columns = Vec::new();
    let parts: Vec<&str> = schema_data.split('|').collect();
    columns.reserve(parts.len());

    for column_part in parts {
        if column_part.is_empty() {
            continue;
        }

        let components: Vec<&str> = column_part.splitn(3, ':').collect();
        if components.len() >= 2 {
            let column_name = components[0].to_string();
            let data_type_str = components[1];
            let constraints_str = if components.len() > 2 {
                components[2]
            } else {
                ""
            };

            let data_type = match data_type_str {
                "Integer" | "INTEGER" => DataType::Integer,
                "Real" | "REAL" => DataType::Real,
                text_type if text_type.starts_with("Text(") => {
                    // Parse Text(Some(n)) format
                    if let Some(length_str) = text_type
                        .strip_prefix("Text(Some(")
                        .and_then(|s| s.strip_suffix("))"))
                    {
                        if let Ok(length) = length_str.parse::<usize>() {
                            DataType::Text(Some(length))
                        } else {
                            DataType::Text(None)
                        }
                    } else {
                        DataType::Text(None)
                    }
                }
                "Text" | "TEXT" => DataType::Text(None), // Default to variable length
                vector_type if vector_type.starts_with("Vector(") => {
                    // Parse Vector(Some(n)) format
                    if let Some(dimension_str) = vector_type
                        .strip_prefix("Vector(Some(")
                        .and_then(|s| s.strip_suffix("))"))
                    {
                        if let Ok(dimension) = dimension_str.parse::<usize>() {
                            DataType::Vector(Some(dimension))
                        } else {
                            DataType::Vector(None)
                        }
                    } else {
                        DataType::Vector(None)
                    }
                }
                "Vector" => DataType::Vector(None), // Default to variable dimension
                _ => continue,                      // Skip unknown types
            };

            let constraints = if constraints_str.is_empty() {
                Vec::new()
            } else {
                constraints_str
                    .split(',')
                    .filter_map(|c| match c {
                        x if x == crate::catalog::CONSTRAINT_PRIMARY_KEY_STR => {
                            Some(ColumnConstraint::PrimaryKey)
                        }
                        x if x == crate::catalog::CONSTRAINT_NOT_NULL_STR => {
                            Some(ColumnConstraint::NotNull)
                        }
                        x if x == crate::catalog::CONSTRAINT_UNIQUE_STR => {
                            Some(ColumnConstraint::Unique)
                        }
                        _ => None,
                    })
                    .collect()
            };

            columns.push(ColumnInfo {
                name: column_name,
                data_type,
                constraints,
                storage_offset: 0,
                storage_size: 0,
                storage_type_code: 0,
            });
        }
    }

    if columns.is_empty() {
        None
    } else {
        let mut schema = TableSchema {
            name: table_name.to_string(),
            columns,
            indexes: vec![], // Initialize indexes as empty
        };
        let _ = crate::catalog::Catalog::compute_table_metadata(&mut schema);
        Some(schema)
    }
}

/// Optimized schema deserialization from binary data
/// Handles the binary format used in storage
pub fn deserialize_schema_from_bytes(data: &[u8]) -> crate::Result<TableSchema> {
    let mut columns = Vec::new();
    let mut start = 0;
    let mut column_count = 0;

    // Pre-count columns to avoid reallocations
    for &byte in data {
        if byte == crate::catalog::FIELD_SEPARATOR {
            column_count += 1;
        }
    }
    columns.reserve(column_count + 1);

    for (i, &byte) in data.iter().enumerate() {
        if byte == crate::catalog::FIELD_SEPARATOR {
            if i > start {
                let column_part = &data[start..i];
                parse_column_part_from_bytes(column_part, &mut columns);
            }
            start = i + 1;
        }
    }

    if start < data.len() {
        let column_part = &data[start..];
        parse_column_part_from_bytes(column_part, &mut columns);
    }

    let mut schema = TableSchema {
        name: "unknown".to_string(), // Will be set by caller
        columns,
        indexes: vec![], // Initialize indexes as empty
    };
    let _ = crate::catalog::Catalog::compute_table_metadata(&mut schema);
    Ok(schema)
}

/// Helper to parse a single column entry from binary data
fn parse_column_part_from_bytes(column_part: &[u8], columns: &mut Vec<ColumnInfo>) {
    let mut parts = column_part.splitn(3, |&b| b == crate::catalog::STORAGE_SEPARATOR);
    if let (Some(name_bytes), Some(type_bytes)) = (parts.next(), parts.next()) {
        let name = String::from_utf8_lossy(name_bytes).to_string();
        let type_str = String::from_utf8_lossy(type_bytes);

        let data_type = match type_str.as_ref() {
            "Integer" | "INTEGER" => DataType::Integer,
            "Real" | "REAL" => DataType::Real,
            text_type if text_type.starts_with("Text(") => {
                // Parse Text(Some(n)) format
                if let Some(length_str) = text_type
                    .strip_prefix("Text(Some(")
                    .and_then(|s| s.strip_suffix("))"))
                {
                    if let Ok(length) = length_str.parse::<usize>() {
                        DataType::Text(Some(length))
                    } else {
                        DataType::Text(None)
                    }
                } else {
                    DataType::Text(None)
                }
            }
            "Text" | "TEXT" => DataType::Text(None), // Default to variable length
            vector_type if vector_type.starts_with("Vector(") => {
                // Parse Vector(Some(n)) format
                if let Some(dimension_str) = vector_type
                    .strip_prefix("Vector(Some(")
                    .and_then(|s| s.strip_suffix("))"))
                {
                    if let Ok(dimension) = dimension_str.parse::<usize>() {
                        DataType::Vector(Some(dimension))
                    } else {
                        DataType::Vector(None)
                    }
                } else {
                    DataType::Vector(None)
                }
            }
            "Vector" => DataType::Vector(None), // Default to variable dimension
            _ => DataType::Text(None),          // Default fallback
        };

        let constraints = if let Some(constraints_bytes) = parts.next() {
            let primary = crate::catalog::CONSTRAINT_PRIMARY_KEY_STR.as_bytes();
            let not_null = crate::catalog::CONSTRAINT_NOT_NULL_STR.as_bytes();
            let unique = crate::catalog::CONSTRAINT_UNIQUE_STR.as_bytes();
            constraints_bytes
                .split(|&b| b == crate::catalog::CONSTRAINT_SEP)
                .filter_map(|c| {
                    if c == primary {
                        Some(ColumnConstraint::PrimaryKey)
                    } else if c == not_null {
                        Some(ColumnConstraint::NotNull)
                    } else if c == unique {
                        Some(ColumnConstraint::Unique)
                    } else {
                        None
                    }
                })
                .collect()
        } else {
            Vec::new()
        };

        columns.push(ColumnInfo {
            name,
            data_type,
            constraints,
            storage_offset: 0,
            storage_size: 0,
            storage_type_code: 0,
        });
    }
}
