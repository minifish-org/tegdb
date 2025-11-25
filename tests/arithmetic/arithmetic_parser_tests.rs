//! Test parsing of arithmetic expressions in UPDATE statements

#[path = "../helpers/test_helpers.rs"]
mod test_helpers;

#[cfg(feature = "dev")]
use tegdb::parser::*;

#[cfg(feature = "dev")]
#[test]
fn test_parse_arithmetic_expressions() {
    // Test simple addition
    let sql = "UPDATE users SET age = age + 5";
    let result = parse_sql(sql);
    assert!(result.is_ok());
    let statement = result.unwrap();
    match statement {
        Statement::Update(update) => {
            assert_eq!(update.table, "users");
            assert_eq!(update.assignments.len(), 1);
            assert_eq!(update.assignments[0].column, "age");

            // Check that we have a binary operation
            match &update.assignments[0].value {
                Expression::BinaryOp {
                    left,
                    operator,
                    right,
                } => {
                    assert_eq!(**left, Expression::Column("age".to_string()));
                    assert_eq!(*operator, ArithmeticOperator::Add);
                    assert_eq!(**right, Expression::Value(SqlValue::Integer(5)));
                }
                _ => panic!("Expected BinaryOp expression"),
            }
        }
        _ => panic!("Expected UPDATE statement"),
    }
}

#[cfg(feature = "dev")]
#[test]
fn test_parse_all_arithmetic_operators() {
    let test_cases = vec![
        ("UPDATE t SET x = x + 1", ArithmeticOperator::Add),
        ("UPDATE t SET x = x - 1", ArithmeticOperator::Subtract),
        ("UPDATE t SET x = x * 2", ArithmeticOperator::Multiply),
        ("UPDATE t SET x = x / 4", ArithmeticOperator::Divide),
    ];

    for (sql, expected_op) in test_cases {
        let result = parse_sql(sql);
        assert!(result.is_ok(), "Failed to parse: {sql}");
        let statement = result.unwrap();

        match statement {
            Statement::Update(update) => match &update.assignments[0].value {
                Expression::BinaryOp { operator, .. } => {
                    assert_eq!(*operator, expected_op, "Wrong operator for: {sql}");
                }
                _ => panic!("Expected BinaryOp for: {sql}"),
            },
            _ => panic!("Expected UPDATE statement for: {sql}"),
        }
    }
}

#[cfg(feature = "dev")]
#[test]
fn test_parse_complex_arithmetic_expressions() {
    // Test operator precedence: multiplication before addition
    let sql = "UPDATE users SET total = price + quantity * rate";
    let result = parse_sql(sql);
    assert!(result.is_ok());
    let statement = result.unwrap();

    match statement {
        Statement::Update(update) => {
            match &update.assignments[0].value {
                Expression::BinaryOp {
                    left,
                    operator,
                    right,
                } => {
                    // Should be: price + (quantity * rate)
                    assert_eq!(**left, Expression::Column("price".to_string()));
                    assert_eq!(*operator, ArithmeticOperator::Add);

                    // Right side should be a multiplication
                    match &**right {
                        Expression::BinaryOp {
                            left: mult_left,
                            operator: mult_op,
                            right: mult_right,
                        } => {
                            assert_eq!(**mult_left, Expression::Column("quantity".to_string()));
                            assert_eq!(*mult_op, ArithmeticOperator::Multiply);
                            assert_eq!(**mult_right, Expression::Column("rate".to_string()));
                        }
                        _ => panic!("Expected multiplication on right side"),
                    }
                }
                _ => panic!("Expected BinaryOp expression"),
            }
        }
        _ => panic!("Expected UPDATE statement"),
    }
}

#[cfg(feature = "dev")]
#[test]
fn test_parse_parenthesized_expressions() {
    // Test parentheses override precedence: (price + quantity) * rate
    let sql = "UPDATE users SET total = (price + quantity) * rate";
    let result = parse_sql(sql);
    assert!(result.is_ok());
    let statement = result.unwrap();

    match statement {
        Statement::Update(update) => {
            match &update.assignments[0].value {
                Expression::BinaryOp {
                    left,
                    operator,
                    right,
                } => {
                    // Should be: (price + quantity) * rate
                    assert_eq!(*operator, ArithmeticOperator::Multiply);
                    assert_eq!(**right, Expression::Column("rate".to_string()));

                    // Left side should be an addition
                    match &**left {
                        Expression::BinaryOp {
                            left: add_left,
                            operator: add_op,
                            right: add_right,
                        } => {
                            assert_eq!(**add_left, Expression::Column("price".to_string()));
                            assert_eq!(*add_op, ArithmeticOperator::Add);
                            assert_eq!(**add_right, Expression::Column("quantity".to_string()));
                        }
                        _ => panic!("Expected addition on left side"),
                    }
                }
                _ => panic!("Expected BinaryOp expression"),
            }
        }
        _ => panic!("Expected UPDATE statement"),
    }
}

#[cfg(feature = "dev")]
#[test]
fn test_parse_mixed_types_arithmetic() {
    // Test mixing integers and reals
    let sql = "UPDATE products SET price = base_price + 10.5";
    let result = parse_sql(sql);
    assert!(result.is_ok());
    let statement = result.unwrap();

    match statement {
        Statement::Update(update) => match &update.assignments[0].value {
            Expression::BinaryOp {
                left,
                operator,
                right,
            } => {
                assert_eq!(**left, Expression::Column("base_price".to_string()));
                assert_eq!(*operator, ArithmeticOperator::Add);
                assert_eq!(**right, Expression::Value(SqlValue::Real(10.5)));
            }
            _ => panic!("Expected BinaryOp expression"),
        },
        _ => panic!("Expected UPDATE statement"),
    }
}

#[cfg(feature = "dev")]
#[test]
fn test_parse_multiple_assignments_with_expressions() {
    let sql =
        "UPDATE products SET price = price * 1.1, discount = price - 10, total = quantity * price";
    let result = parse_sql(sql);
    assert!(result.is_ok());
    let statement = result.unwrap();

    match statement {
        Statement::Update(update) => {
            assert_eq!(update.assignments.len(), 3);

            // First assignment: price = price * 1.1
            match &update.assignments[0].value {
                Expression::BinaryOp {
                    left,
                    operator,
                    right,
                } => {
                    assert_eq!(**left, Expression::Column("price".to_string()));
                    assert_eq!(*operator, ArithmeticOperator::Multiply);
                    assert_eq!(**right, Expression::Value(SqlValue::Real(1.1)));
                }
                _ => panic!("Expected BinaryOp for first assignment"),
            }

            // Second assignment: discount = price - 10
            match &update.assignments[1].value {
                Expression::BinaryOp {
                    left,
                    operator,
                    right,
                } => {
                    assert_eq!(**left, Expression::Column("price".to_string()));
                    assert_eq!(*operator, ArithmeticOperator::Subtract);
                    assert_eq!(**right, Expression::Value(SqlValue::Integer(10)));
                }
                _ => panic!("Expected BinaryOp for second assignment"),
            }

            // Third assignment: total = quantity * price
            match &update.assignments[2].value {
                Expression::BinaryOp {
                    left,
                    operator,
                    right,
                } => {
                    assert_eq!(**left, Expression::Column("quantity".to_string()));
                    assert_eq!(*operator, ArithmeticOperator::Multiply);
                    assert_eq!(**right, Expression::Column("price".to_string()));
                }
                _ => panic!("Expected BinaryOp for third assignment"),
            }
        }
        _ => panic!("Expected UPDATE statement"),
    }
}

#[cfg(feature = "dev")]
#[test]
fn test_parse_literal_values_still_work() {
    // Ensure simple literal assignments still work
    let sql = "UPDATE users SET name = 'John', age = 25, score = 95.5";
    let result = parse_sql(sql);
    assert!(result.is_ok());
    let statement = result.unwrap();

    match statement {
        Statement::Update(update) => {
            assert_eq!(update.assignments.len(), 3);

            // All should be simple Value expressions
            assert_eq!(
                update.assignments[0].value,
                Expression::Value(SqlValue::Text("John".to_string()))
            );
            assert_eq!(
                update.assignments[1].value,
                Expression::Value(SqlValue::Integer(25))
            );
            assert_eq!(
                update.assignments[2].value,
                Expression::Value(SqlValue::Real(95.5))
            );
        }
        _ => panic!("Expected UPDATE statement"),
    }
}
