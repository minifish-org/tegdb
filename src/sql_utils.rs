//! Shared utilities for SQL operations
//!
//! This module contains common functions used across the codebase for SQL value
//! comparison, condition evaluation, and other SQL-related operations.

use crate::parser::{ComparisonOperator, Condition, SqlValue};
use std::collections::HashMap;

/// Compare two SqlValues using the given operator
pub fn compare_values(
    left: &SqlValue,
    operator: &ComparisonOperator,
    right: &SqlValue,
) -> bool {
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
            // Simple LIKE implementation - just checking if right is substring of left
            match (left, right) {
                (SqlValue::Text(a), SqlValue::Text(b)) => {
                    // Simple pattern matching - convert SQL LIKE to contains for now
                    let pattern = b.replace('%', "");
                    a.contains(&pattern)
                }
                _ => false,
            }
        }
    }
}

/// Evaluate condition against row data
pub fn evaluate_condition(
    condition: &Condition,
    row_data: &HashMap<String, SqlValue>,
) -> bool {
    match condition {
        Condition::Comparison {
            left,
            operator,
            right,
        } => {
            let row_value = row_data.get(left).unwrap_or(&SqlValue::Null);
            compare_values(row_value, operator, right)
        }
        Condition::And(left, right) => {
            evaluate_condition(left, row_data) && evaluate_condition(right, row_data)
        }
        Condition::Or(left, right) => {
            evaluate_condition(left, row_data) || evaluate_condition(right, row_data)
        }
    }
}
