//! SQL parser implementation using nom
//!
//! This module provides a SQL parser for basic database operations including
//! SELECT, INSERT, UPDATE, DELETE, and CREATE TABLE statements.

use nom::{
    branch::alt,
    bytes::complete::{tag, tag_no_case, take_while1},
    character::complete::{alpha1, alphanumeric1, char, digit1, multispace0, multispace1},
    combinator::{map, opt, recognize},
    multi::{many0, separated_list0, separated_list1},
    sequence::{delimited, pair, preceded},
    IResult, Parser,
};
use std::collections::HashMap;

fn parse_identifier_optimized(input: &str) -> IResult<&str, String> {
    let (input, first_char) = alpha1.parse(input)?;
    let (input, rest) = many0(alt((alphanumeric1, tag("_")))).parse(input)?;
    let identifier = format!("{}{}", first_char, rest.join(""));
    Ok((input, identifier))
}

fn parse_column_list_optimized(input: &str) -> IResult<&str, Vec<Expression>> {
    let (input, _) = multispace0.parse(input)?;
    // Handle the special case of "*" (all columns)
    if let Ok((input, _)) = tag::<&str, &str, nom::error::Error<&str>>("*").parse(input) {
        return Ok((input, vec![Expression::Column("*".to_string())]));
    }
    // Parse comma-separated list of expressions
    separated_list1(
        delimited(multispace0, char(','), multispace0),
        parse_expression,
    )
    .parse(input)
}

fn parse_u64_safe(s: &str) -> Result<u64, String> {
    s.parse::<u64>().map_err(|e| format!("Invalid u64: {e}"))
}

fn parse_f64_safe(s: &str) -> Result<f64, String> {
    s.parse::<f64>().map_err(|e| format!("Invalid f64: {e}"))
}

#[derive(Debug, Clone, PartialEq)]
pub enum Statement {
    Select(SelectStatement),
    Insert(InsertStatement),
    Update(UpdateStatement),
    Delete(DeleteStatement),
    CreateTable(CreateTableStatement),
    DropTable(DropTableStatement),
    CreateIndex(CreateIndexStatement),
    DropIndex(DropIndexStatement),
    Begin,
    Commit,
    Rollback,
}

#[derive(Debug, Clone, PartialEq)]
pub struct SelectStatement {
    pub columns: Vec<Expression>,
    pub table: String,
    pub where_clause: Option<WhereClause>,
    pub order_by: Option<OrderByClause>,
    pub limit: Option<u64>, // None means no limit, Some(n) means limit n
}

#[derive(Debug, Clone, PartialEq)]
pub struct InsertStatement {
    pub table: String,
    pub columns: Vec<String>,
    pub values: Vec<Vec<SqlValue>>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct UpdateStatement {
    pub table: String,
    pub assignments: Vec<Assignment>,
    pub where_clause: Option<WhereClause>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct DeleteStatement {
    pub table: String,
    pub where_clause: Option<WhereClause>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct CreateTableStatement {
    pub table: String,
    pub columns: Vec<ColumnDefinition>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct DropTableStatement {
    pub table: String,
    pub if_exists: bool,
}

#[derive(Debug, Clone, PartialEq)]
pub struct CreateIndexStatement {
    pub index_name: String,
    pub table_name: String,
    pub column_name: String,
    pub unique: bool,
}

#[derive(Debug, Clone, PartialEq)]
pub struct DropIndexStatement {
    pub index_name: String,
    pub if_exists: bool,
}

#[derive(Debug, Clone, PartialEq)]
pub struct ColumnDefinition {
    pub name: String,
    pub data_type: DataType,
    pub constraints: Vec<ColumnConstraint>,
}

#[derive(Debug, Clone, PartialEq)]
pub enum DataType {
    Integer,
    Text(Option<usize>), // None = variable length, Some(n) = fixed length n
    Real,
    Vector(Option<usize>), // None = variable length, Some(n) = fixed dimension n
}

#[derive(Debug, Clone, PartialEq)]
pub enum ColumnConstraint {
    PrimaryKey,
    NotNull,
    Unique,
}

#[derive(Debug, Clone, PartialEq)]
pub struct WhereClause {
    pub condition: Condition,
}

#[derive(Debug, Clone, PartialEq)]
pub struct OrderByClause {
    pub items: Vec<OrderByItem>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct OrderByItem {
    pub expression: Expression,
    pub direction: OrderDirection,
}

#[derive(Debug, Clone, PartialEq, Copy)]
pub enum OrderDirection {
    Asc,
    Desc,
}

#[derive(Debug, Clone, PartialEq)]
pub enum Condition {
    /// Comparison: left operator right (e.g., id = 1)
    Comparison {
        left: String,
        operator: ComparisonOperator,
        right: SqlValue,
    },
    /// BETWEEN: column BETWEEN low AND high
    Between {
        column: String,
        low: SqlValue,
        high: SqlValue,
    },
    And(Box<Condition>, Box<Condition>),
    Or(Box<Condition>, Box<Condition>),
}

#[derive(Debug, Clone, PartialEq, Copy)]
pub enum ComparisonOperator {
    Equal,
    NotEqual,
    LessThan,
    LessThanOrEqual,
    GreaterThan,
    GreaterThanOrEqual,
    Like,
}

#[derive(Debug, Clone, PartialEq)]
pub struct Assignment {
    pub column: String,
    pub value: Expression,
}

#[derive(Debug, Clone, PartialEq)]
pub enum SqlValue {
    Integer(i64),
    Real(f64),
    Text(String),
    Vector(Vec<f64>), // Vector of f64 values for AI embeddings
    Null,
    Parameter(usize), // Parameter placeholder with index
}

#[derive(Debug, Clone, PartialEq)]
pub enum Expression {
    Value(SqlValue),
    Column(String),
    BinaryOp {
        left: Box<Expression>,
        operator: ArithmeticOperator,
        right: Box<Expression>,
    },
    FunctionCall {
        name: String,
        args: Vec<Expression>,
    },
}

impl Expression {
    pub fn evaluate(&self, context: &HashMap<String, SqlValue>) -> Result<SqlValue, String> {
        match self {
            Expression::Value(value) => Ok(value.clone()),
            Expression::Column(column) => context
                .get(column)
                .cloned()
                .ok_or_else(|| format!("Column '{column}' not found in context")),
            Expression::BinaryOp {
                left,
                operator,
                right,
            } => {
                let left_val = left.evaluate(context)?;
                let right_val = right.evaluate(context)?;

                match (left_val.clone(), right_val.clone()) {
                    (SqlValue::Integer(a), SqlValue::Integer(b)) => {
                        let result = match operator {
                            ArithmeticOperator::Add => a + b,
                            ArithmeticOperator::Subtract => a - b,
                            ArithmeticOperator::Multiply => a * b,
                            ArithmeticOperator::Divide => {
                                if b == 0 {
                                    return Err("Division by zero".to_string());
                                }
                                a / b
                            }
                            ArithmeticOperator::Modulo => {
                                if b == 0 {
                                    return Err("Modulo by zero".to_string());
                                }
                                a % b
                            }
                        };
                        Ok(SqlValue::Integer(result))
                    }
                    (SqlValue::Real(a), SqlValue::Real(b)) => {
                        let result = match operator {
                            ArithmeticOperator::Add => a + b,
                            ArithmeticOperator::Subtract => a - b,
                            ArithmeticOperator::Multiply => a * b,
                            ArithmeticOperator::Divide => {
                                if b == 0.0 {
                                    return Err("Division by zero".to_string());
                                }
                                a / b
                            }
                            ArithmeticOperator::Modulo => {
                                if b == 0.0 {
                                    return Err("Modulo by zero".to_string());
                                }
                                a % b
                            }
                        };
                        Ok(SqlValue::Real(result))
                    }
                    (SqlValue::Integer(a), SqlValue::Real(b)) => {
                        let a_f64 = a as f64;
                        let result = match operator {
                            ArithmeticOperator::Add => a_f64 + b,
                            ArithmeticOperator::Subtract => a_f64 - b,
                            ArithmeticOperator::Multiply => a_f64 * b,
                            ArithmeticOperator::Divide => {
                                if b == 0.0 {
                                    return Err("Division by zero".to_string());
                                }
                                a_f64 / b
                            }
                            ArithmeticOperator::Modulo => {
                                if b == 0.0 {
                                    return Err("Modulo by zero".to_string());
                                }
                                a_f64 % b
                            }
                        };
                        Ok(SqlValue::Real(result))
                    }
                    (SqlValue::Real(a), SqlValue::Integer(b)) => {
                        let b_f64 = b as f64;
                        let result = match operator {
                            ArithmeticOperator::Add => a + b_f64,
                            ArithmeticOperator::Subtract => a - b_f64,
                            ArithmeticOperator::Multiply => a * b_f64,
                            ArithmeticOperator::Divide => {
                                if b_f64 == 0.0 {
                                    return Err("Division by zero".to_string());
                                }
                                a / b_f64
                            }
                            ArithmeticOperator::Modulo => {
                                if b_f64 == 0.0 {
                                    return Err("Modulo by zero".to_string());
                                }
                                a % b_f64
                            }
                        };
                        Ok(SqlValue::Real(result))
                    }
                    (SqlValue::Text(a), SqlValue::Text(b)) => match operator {
                        ArithmeticOperator::Add => Ok(SqlValue::Text(format!("{a}{b}"))),
                        _ => Err("Only addition (+) is supported for text values".to_string()),
                    },
                    _ => Err(format!(
                        "Unsupported operation: {left_val:?} {operator:?} {right_val:?}"
                    )),
                }
            }
            Expression::FunctionCall { name, args } => {
                // Stub: function evaluation will be implemented later
                Err(format!("Function '{name}' evaluation not implemented"))
            }
        }
    }
}

#[derive(Debug, Clone, PartialEq, Copy)]
pub enum ArithmeticOperator {
    Add,
    Subtract,
    Multiply,
    Divide,
    Modulo,
}

/// Parse SQL and assign unique parameter indices
pub fn parse_sql(input: &str) -> Result<Statement, String> {
    let (remaining, statement) = parse_statement
        .parse(input)
        .map_err(|e| format!("Parse error: {e:?}"))?;

    // Allow trailing whitespace and optional semicolon(s)
    let remaining = remaining.trim_start();
    let remaining = if remaining.starts_with(';') {
        &remaining[1..]
    } else {
        remaining
    };
    if !remaining.trim().is_empty() {
        return Err(format!("Unexpected input after statement: '{remaining}'"));
    }

    Ok(statement)
}

/// Parse a single SQL statement
fn parse_statement(input: &str) -> IResult<&str, Statement> {
    let (input, _) = multispace0.parse(input)?;

    alt((
        parse_create_table,
        parse_insert,
        parse_select,
        parse_update,
        parse_delete,
        parse_drop_table,
        parse_create_index,
        parse_drop_index,
        parse_begin_transaction,
        parse_commit,
        parse_rollback,
    ))
    .parse(input)
}

// Parse parameter placeholder (?1, ?2, etc.) - now requires index
fn parse_parameter_placeholder(input: &str) -> IResult<&str, usize> {
    let (input, _) = char('?').parse(input)?;
    // Require a number after '?'
    let (input, num_str) = digit1::<&str, nom::error::Error<&str>>.parse(input)?;
    let num = num_str.parse::<usize>().map_err(|_e| {
        nom::Err::Error(nom::error::Error::new(input, nom::error::ErrorKind::Digit))
    })?;
    Ok((input, num - 1)) // Convert to 0-based index
}

// Parse SQL values
fn parse_sql_value(input: &str) -> IResult<&str, SqlValue> {
    alt((
        map(tag_no_case("NULL"), |_| SqlValue::Null),
        map(parse_string_literal, SqlValue::Text),
        map(parse_vector_literal, SqlValue::Vector),
        map(parse_real, SqlValue::Real),
        map(parse_integer, SqlValue::Integer),
        map(parse_parameter_placeholder, SqlValue::Parameter),
    ))
    .parse(input)
}

// Parse CREATE TABLE statement
fn parse_create_table(input: &str) -> IResult<&str, Statement> {
    let (input, _) = delimited(multispace0, tag_no_case("CREATE"), multispace1).parse(input)?;
    let (input, _) = tag_no_case("TABLE").parse(input)?;
    let (input, _) = multispace1.parse(input)?;
    let (input, table) = parse_identifier_optimized.parse(input)?;
    let (input, _) = multispace0.parse(input)?;
    let (input, _) = char('(').parse(input)?;
    let (input, columns) = separated_list0(
        delimited(multispace0, char(','), multispace0),
        delimited(multispace0, parse_column_definition, multispace0),
    )
    .parse(input)?;
    let (input, _) = char(')').parse(input)?;

    Ok((
        input,
        Statement::CreateTable(CreateTableStatement { table, columns }),
    ))
}

// Parse INSERT statement
fn parse_insert(input: &str) -> IResult<&str, Statement> {
    let (input, _) = tag_no_case("INSERT").parse(input)?;
    let (input, _) = multispace1.parse(input)?;
    let (input, _) = tag_no_case("INTO").parse(input)?;
    let (input, _) = multispace1.parse(input)?;
    let (input, table) = parse_identifier_optimized.parse(input)?;
    let (input, _) = multispace0.parse(input)?;
    let (input, _) = char('(').parse(input)?;
    let (input, columns_expr) = parse_column_list_optimized.parse(input)?;
    let columns: Vec<String> = columns_expr.into_iter().map(|expr| {
        if let Expression::Column(name) = expr {
            name
        } else {
            panic!("Only column names are allowed in INSERT column list")
        }
    }).collect();
    let (input, _) = char(')').parse(input)?;
    let (input, _) = multispace0.parse(input)?;
    let (input, _) = tag_no_case("VALUES").parse(input)?;
    let (input, _) = multispace0.parse(input)?;

    // Parse one or more value tuples
    let (input, values) = separated_list1(
        delimited(multispace0, char(','), multispace0),
        delimited(
            char('('),
            separated_list0(
                delimited(multispace0, char(','), multispace0),
                parse_sql_value,
            ),
            char(')'),
        ),
    )
    .parse(input)?;

    Ok((
        input,
        Statement::Insert(InsertStatement {
            table,
            columns,
            values,
        }),
    ))
}

// Parse SELECT statement
fn parse_select(input: &str) -> IResult<&str, Statement> {
    let (input, _) = tag_no_case("SELECT").parse(input)?;
    let (input, _) = multispace1.parse(input)?;
    let (input, columns) = parse_column_list_optimized.parse(input)?;
    let (input, _) = multispace0.parse(input)?;
    let (input, _) = tag_no_case("FROM").parse(input)?;
    let (input, _) = multispace1.parse(input)?;
    let (input, table) = parse_identifier_optimized.parse(input)?;
    let (input, _) = multispace0.parse(input)?;
    let (input, where_clause) = opt(parse_where_clause).parse(input)?;
    let (input, _) = multispace0.parse(input)?;
    let (input, order_by) = opt(parse_order_by_clause).parse(input)?;
    let (input, _) = multispace0.parse(input)?;
    let (input, limit) = opt(parse_limit_with_parameter).parse(input)?;

    Ok((
        input,
        Statement::Select(SelectStatement {
            columns,
            table,
            where_clause,
            order_by,
            limit: limit.flatten(),
        }),
    ))
}

// Parse UPDATE statement
fn parse_update(input: &str) -> IResult<&str, Statement> {
    let (input, _) = tag_no_case("UPDATE").parse(input)?;
    let (input, _) = multispace1.parse(input)?;
    let (input, table) = parse_identifier_optimized.parse(input)?;
    let (input, _) = multispace0.parse(input)?;
    let (input, _) = tag_no_case("SET").parse(input)?;
    let mut input = input; // Create a mutable input for the loop
    let mut assignments = Vec::new();
    loop {
        // Manual check for WHERE keyword (case-insensitive)
        if input.trim_start().to_ascii_uppercase().starts_with("WHERE")
            || input.trim_start().is_empty()
        {
            break;
        }
        let (next_input, assignment) = parse_assignment(input)?;
        assignments.push(assignment);
        input = next_input;
        // Try to parse comma, but allow trailing comma
        let (next_input, _) = multispace0.parse(input)?;
        if let Ok((after_comma, _)) = char::<&str, nom::error::Error<&str>>(',').parse(next_input) {
            input = after_comma;
        } else {
            input = next_input;
            // If no comma found, we're done with assignments
            break;
        }
    }
    let (input, _) = multispace0.parse(input)?;
    // Try to parse WHERE clause if present (with leading whitespace)
    let (input, where_clause) = if let Ok((input, clause)) = parse_where_clause(input) {
        (input, Some(clause))
    } else {
        (input, None)
    };
    let (input, _) = multispace0.parse(input)?;
    let (input, _) = opt(char(';')).parse(input)?;
    let (input, _) = multispace0.parse(input)?;

    Ok((
        input,
        Statement::Update(UpdateStatement {
            table,
            assignments,
            where_clause,
        }),
    ))
}

// Parse DELETE statement
fn parse_delete(input: &str) -> IResult<&str, Statement> {
    let (input, _) = tag_no_case("DELETE").parse(input)?;
    let (input, _) = multispace1.parse(input)?;
    let (input, _) = tag_no_case("FROM").parse(input)?;
    let (input, _) = multispace1.parse(input)?;
    let (input, table) = parse_identifier_optimized.parse(input)?;
    let (input, _) = multispace0.parse(input)?;
    let (input, where_clause) = opt(parse_where_clause).parse(input)?;

    Ok((
        input,
        Statement::Delete(DeleteStatement {
            table,
            where_clause,
        }),
    ))
}

// Parse DROP TABLE statement
fn parse_drop_table(input: &str) -> IResult<&str, Statement> {
    let (input, _) = tag_no_case("DROP").parse(input)?;
    let (input, _) = multispace1.parse(input)?;
    let (input, _) = tag_no_case("TABLE").parse(input)?;
    let (input, _) = multispace1.parse(input)?;
    let (input, if_exists) = opt(tag_no_case("IF EXISTS")).parse(input)?;
    let (input, _) = multispace0.parse(input)?;
    let (input, table) = parse_identifier_optimized.parse(input)?;

    Ok((
        input,
        Statement::DropTable(DropTableStatement {
            table,
            if_exists: if_exists.is_some(),
        }),
    ))
}

// Parse CREATE INDEX statement
fn parse_create_index(input: &str) -> IResult<&str, Statement> {
    let (input, _) = delimited(multispace0, tag_no_case("CREATE"), multispace1).parse(input)?;
    let (input, _) = tag_no_case("INDEX").parse(input)?;
    let (input, _) = multispace1.parse(input)?;
    let (input, index_name) = parse_identifier_optimized.parse(input)?;
    let (input, _) = multispace0.parse(input)?;
    let (input, _) = tag_no_case("ON").parse(input)?;
    let (input, _) = multispace1.parse(input)?;
    let (input, table_name) = parse_identifier_optimized.parse(input)?;
    let (input, _) = multispace0.parse(input)?;
    let (input, _) = char('(').parse(input)?;
    let (input, column_name) = parse_identifier_optimized.parse(input)?;
    let (input, _) = char(')').parse(input)?;
    let (input, _) = multispace0.parse(input)?;
    let (input, unique) = opt(tag_no_case("UNIQUE")).parse(input)?;
    let (input, _) = multispace0.parse(input)?;

    Ok((
        input,
        Statement::CreateIndex(CreateIndexStatement {
            index_name,
            table_name,
            column_name,
            unique: unique.is_some(),
        }),
    ))
}

// Parse DROP INDEX statement
fn parse_drop_index(input: &str) -> IResult<&str, Statement> {
    let (input, _) = delimited(multispace0, tag_no_case("DROP"), multispace1).parse(input)?;
    let (input, _) = tag_no_case("INDEX").parse(input)?;
    let (input, _) = multispace1.parse(input)?;
    let (input, index_name) = parse_identifier_optimized.parse(input)?;
    let (input, _) = multispace0.parse(input)?;
    let (input, if_exists) = opt(tag_no_case("IF EXISTS")).parse(input)?;

    Ok((
        input,
        Statement::DropIndex(DropIndexStatement {
            index_name,
            if_exists: if_exists.is_some(),
        }),
    ))
}

// Parse BEGIN TRANSACTION
fn parse_begin_transaction(input: &str) -> IResult<&str, Statement> {
    alt((
        // BEGIN TRANSACTION
        map(
            pair(
                tag_no_case("BEGIN"),
                opt(delimited(
                    multispace1,
                    tag_no_case("TRANSACTION"),
                    multispace0,
                )),
            ),
            |_| Statement::Begin,
        ),
        // START TRANSACTION
        map(
            pair(
                tag_no_case("START"),
                delimited(multispace1, tag_no_case("TRANSACTION"), multispace0),
            ),
            |_| Statement::Begin,
        ),
    ))
    .parse(input)
}

// Parse COMMIT
fn parse_commit(input: &str) -> IResult<&str, Statement> {
    let (input, _) = tag_no_case("COMMIT").parse(input)?;
    Ok((input, Statement::Commit))
}

// Parse ROLLBACK
fn parse_rollback(input: &str) -> IResult<&str, Statement> {
    let (input, _) = tag_no_case("ROLLBACK").parse(input)?;
    Ok((input, Statement::Rollback))
}

// Parse WHERE clause
fn parse_where_clause(input: &str) -> IResult<&str, WhereClause> {
    let (input, _) = multispace0.parse(input)?;
    let (input, _) = tag_no_case("WHERE").parse(input)?;
    let (input, _) = multispace1.parse(input)?;
    let (input, condition) = parse_condition.parse(input)?;
    Ok((input, WhereClause { condition }))
}

// Parse condition
fn parse_condition(input: &str) -> IResult<&str, Condition> {
    parse_or_condition.parse(input)
}

// Parse OR condition
fn parse_or_condition(input: &str) -> IResult<&str, Condition> {
    let (input, left) = parse_and_condition.parse(input)?;
    let (input, rights) = many0((
        delimited(multispace0, tag_no_case("OR"), multispace0),
        parse_and_condition,
    ))
    .parse(input)?;

    Ok((
        input,
        rights.into_iter().fold(left, |acc, (_, right)| {
            Condition::Or(Box::new(acc), Box::new(right))
        }),
    ))
}

// Parse AND condition
fn parse_and_condition(input: &str) -> IResult<&str, Condition> {
    let (input, left) = parse_primary_condition.parse(input)?;
    let (input, rights) = many0((
        delimited(multispace0, tag_no_case("AND"), multispace0),
        parse_primary_condition,
    ))
    .parse(input)?;

    Ok((
        input,
        rights.into_iter().fold(left, |acc, (_, right)| {
            Condition::And(Box::new(acc), Box::new(right))
        }),
    ))
}

// Parse primary condition
fn parse_primary_condition(input: &str) -> IResult<&str, Condition> {
    // Try BETWEEN first, then fallback to parenthesis or comparison
    alt((
        parse_between,
        delimited(
            char('('),
            delimited(multispace0, parse_condition, multispace0),
            char(')'),
        ),
        parse_comparison,
    ))
    .parse(input)
}

// Parse BETWEEN condition: <column> BETWEEN <low> AND <high>
fn parse_between(input: &str) -> IResult<&str, Condition> {
    let (input, left_expr) = parse_expression.parse(input)?;
    let (input, _) = multispace0.parse(input)?;
    let (input, _) = tag_no_case("BETWEEN").parse(input)?;
    let (input, _) = multispace0.parse(input)?;
    let (input, low) = parse_sql_value.parse(input)?;
    let (input, _) = multispace0.parse(input)?;
    let (input, _) = tag_no_case("AND").parse(input)?;
    let (input, _) = multispace0.parse(input)?;
    let (input, high) = parse_sql_value.parse(input)?;
    // Only allow column name on left
    let column = match left_expr {
        Expression::Column(name) => name,
        _ => {
            return Err(nom::Err::Error(nom::error::Error::new(
                input,
                nom::error::ErrorKind::Tag,
            )))
        }
    };
    Ok((input, Condition::Between { column, low, high }))
}

// Parse comparison
fn parse_comparison(input: &str) -> IResult<&str, Condition> {
    let (input, left_expr) = parse_expression.parse(input)?;
    let (input, _) = multispace0.parse(input)?;
    let (input, operator) = parse_comparison_operator.parse(input)?;
    let (input, _) = multispace0.parse(input)?;
    let (input, right) = parse_sql_value.parse(input)?;

    // Convert the left expression to a string representation for the condition
    let left = match left_expr {
        Expression::Column(name) => name,
        Expression::Value(value) => format!("{value:?}"),
        Expression::BinaryOp {
            left,
            operator: op,
            right,
        } => {
            format!("{left:?} {op:?} {right:?}")
        }
        Expression::FunctionCall { name, args } => {
            format!("{name}({args:?})")
        }
    };

    Ok((
        input,
        Condition::Comparison {
            left,
            operator,
            right,
        },
    ))
}

// Parse comparison operator
fn parse_comparison_operator(input: &str) -> IResult<&str, ComparisonOperator> {
    alt((
        map(tag(">="), |_| ComparisonOperator::GreaterThanOrEqual),
        map(tag("<="), |_| ComparisonOperator::LessThanOrEqual),
        map(tag("!="), |_| ComparisonOperator::NotEqual),
        map(tag("<>"), |_| ComparisonOperator::NotEqual),
        map(tag("="), |_| ComparisonOperator::Equal),
        map(tag("<"), |_| ComparisonOperator::LessThan),
        map(tag(">"), |_| ComparisonOperator::GreaterThan),
        map(tag_no_case("LIKE"), |_| ComparisonOperator::Like),
    ))
    .parse(input)
}

// Parse ORDER BY clause
fn parse_order_by_clause(input: &str) -> IResult<&str, OrderByClause> {
    let (input, _) = multispace0.parse(input)?;
    let (input, _) = tag_no_case("ORDER").parse(input)?;
    let (input, _) = multispace1.parse(input)?;
    let (input, _) = tag_no_case("BY").parse(input)?;
    let (input, _) = multispace1.parse(input)?;
    let (input, items) = separated_list0(
        delimited(multispace0, char(','), multispace0),
        parse_order_by_item,
    )
    .parse(input)?;

    Ok((input, OrderByClause { items }))
}

// Parse ORDER BY item
fn parse_order_by_item(input: &str) -> IResult<&str, OrderByItem> {
    let (input, expression) = parse_expression.parse(input)?;
    let (input, _) = multispace0.parse(input)?;
    let (input, direction) = opt(parse_order_direction).parse(input)?;
    let direction = direction.unwrap_or(OrderDirection::Asc);

    Ok((input, OrderByItem { expression, direction }))
}

// Parse ORDER BY direction (ASC or DESC)
fn parse_order_direction(input: &str) -> IResult<&str, OrderDirection> {
    alt((
        map(tag_no_case("ASC"), |_| OrderDirection::Asc),
        map(tag_no_case("DESC"), |_| OrderDirection::Desc),
    ))
    .parse(input)
}

// Parse LIMIT clause with parameter support
fn parse_limit_with_parameter(input: &str) -> IResult<&str, Option<u64>> {
    let (input, _) = multispace0.parse(input)?;
    let (input, _) = tag_no_case("LIMIT").parse(input)?;
    let (input, _) = multispace1.parse(input)?;

    // Try to parse a parameter placeholder first
    if let Ok((input, _)) = char::<&str, nom::error::Error<&str>>('?').parse(input) {
        // For now, we'll use a placeholder value and handle parameter binding later
        // This allows the parser to accept LIMIT ? but we'll need to handle the actual value during execution
        Ok((input, None)) // None indicates a parameter placeholder
    } else {
        // Fall back to parsing a literal number
        let (input, limit_str) = digit1.parse(input)?;
        let limit = parse_u64_safe(limit_str).map_err(|_e| {
            nom::Err::Error(nom::error::Error::new(input, nom::error::ErrorKind::Tag))
        })?;
        Ok((input, Some(limit)))
    }
}

// Parse assignment
fn parse_assignment(input: &str) -> IResult<&str, Assignment> {
    let (input, _) = multispace0.parse(input)?;
    let (input, column) = parse_identifier_optimized.parse(input)?;
    let (input, _) = multispace0.parse(input)?;
    let (input, _) = char('=').parse(input)?;
    let (input, _) = multispace0.parse(input)?;
    let (input, value) = parse_expression.parse(input)?;

    Ok((input, Assignment { column, value }))
}

// Parse column definition
fn parse_column_definition(input: &str) -> IResult<&str, ColumnDefinition> {
    let (input, name) = parse_identifier_optimized.parse(input)?;
    let (input, _) = multispace1.parse(input)?;
    let (input, data_type) = parse_data_type.parse(input)?;
    let (input, constraints) =
        many0(preceded(multispace1, parse_column_constraint)).parse(input)?;

    Ok((
        input,
        ColumnDefinition {
            name,
            data_type,
            constraints,
        },
    ))
}

// Parse data types with optional length specifications
fn parse_data_type(input: &str) -> IResult<&str, DataType> {
    alt((
        map(tag_no_case("INTEGER"), |_| DataType::Integer),
        map(tag_no_case("INT"), |_| DataType::Integer),
        map(tag_no_case("REAL"), |_| DataType::Real),
        map(tag_no_case("FLOAT"), |_| DataType::Real),
        // Text with optional length: TEXT(10) or TEXT
        map(
            pair(
                alt((tag_no_case("TEXT"), tag_no_case("VARCHAR"))),
                opt(parse_length_specification),
            ),
            |(_, length)| DataType::Text(length),
        ),
        // Vector with optional dimension: VECTOR(384) or VECTOR
        map(
            pair(
                tag_no_case("VECTOR"),
                opt(parse_length_specification),
            ),
            |(_, dimension)| DataType::Vector(dimension),
        ),
    ))
    .parse(input)
}

// Parse length specification: (10)
fn parse_length_specification(input: &str) -> IResult<&str, usize> {
    let (input, _) = multispace0.parse(input)?;
    let (input, _) = char('(').parse(input)?;
    let (input, _) = multispace0.parse(input)?;
    let (input, length_str) = digit1.parse(input)?;
    let (input, _) = multispace0.parse(input)?;
    let (input, _) = char(')').parse(input)?;

    let length = length_str
        .parse::<usize>()
        .map_err(|_e| nom::Err::Error(nom::error::Error::new(input, nom::error::ErrorKind::Tag)))?;

    Ok((input, length))
}

// Parse column constraints
fn parse_column_constraint(input: &str) -> IResult<&str, ColumnConstraint> {
    alt((
        map(
            (tag_no_case("PRIMARY"), multispace1, tag_no_case("KEY")),
            |_| ColumnConstraint::PrimaryKey,
        ),
        map(
            (tag_no_case("NOT"), multispace1, tag_no_case("NULL")),
            |_| ColumnConstraint::NotNull,
        ),
        map(tag_no_case("UNIQUE"), |_| ColumnConstraint::Unique),
    ))
    .parse(input)
}

// Parse string literal - simplified version
fn parse_string_literal(input: &str) -> IResult<&str, String> {
    delimited(
        char('\''),
        map(take_while1(|c| c != '\''), |s: &str| s.to_string()),
        char('\''),
    )
    .parse(input)
}

// Parse integer - optimized version with error handling
fn parse_integer(input: &str) -> IResult<&str, i64> {
    let (input, int_str) = recognize(pair(opt(char('-')), digit1)).parse(input)?;

    // Fast path for small positive integers
    if int_str.len() <= 3 && !int_str.starts_with('-') {
        let mut result = 0i64;
        for byte in int_str.bytes() {
            result = result * 10 + (byte - b'0') as i64;
        }
        Ok((input, result))
    } else {
        let value = int_str.parse::<i64>().map_err(|_e| {
            nom::Err::Error(nom::error::Error::new(input, nom::error::ErrorKind::Tag))
        })?;
        Ok((input, value))
    }
}

// Parse real number with error handling
fn parse_real(input: &str) -> IResult<&str, f64> {
    let (input, real_str) = recognize((opt(char('-')), digit1, char('.'), digit1)).parse(input)?;
    let value = parse_f64_safe(real_str)
        .map_err(|_e| nom::Err::Error(nom::error::Error::new(input, nom::error::ErrorKind::Tag)))?;
    Ok((input, value))
}

// Parse vector literal: [1.0, 2.0, 3.0]
fn parse_vector_literal(input: &str) -> IResult<&str, Vec<f64>> {
    let (input, _) = char('[').parse(input)?;
    let (input, _) = multispace0.parse(input)?;
    let (input, values) = separated_list0(
        delimited(multispace0, char(','), multispace0),
        parse_real,
    )
    .parse(input)?;
    let (input, _) = multispace0.parse(input)?;
    let (input, _) = char(']').parse(input)?;
    Ok((input, values))
}

// Parse expression (supports arithmetic operations)
fn parse_expression(input: &str) -> IResult<&str, Expression> {
    parse_additive_expression.parse(input)
}

// Parse additive expressions (+ and -)
fn parse_additive_expression(input: &str) -> IResult<&str, Expression> {
    let (input, left) = parse_multiplicative_expression.parse(input)?;
    let (input, rights) = many0((
        delimited(multispace0, parse_additive_operator, multispace0),
        parse_multiplicative_expression,
    ))
    .parse(input)?;

    Ok((
        input,
        rights
            .into_iter()
            .fold(left, |acc, (op, right)| Expression::BinaryOp {
                left: Box::new(acc),
                operator: op,
                right: Box::new(right),
            }),
    ))
}

// Parse multiplicative expressions (* and /)
fn parse_multiplicative_expression(input: &str) -> IResult<&str, Expression> {
    let (input, left) = parse_primary_expression.parse(input)?;
    let (input, rights) = many0((
        delimited(multispace0, parse_multiplicative_operator, multispace0),
        parse_primary_expression,
    ))
    .parse(input)?;

    Ok((
        input,
        rights
            .into_iter()
            .fold(left, |acc, (op, right)| Expression::BinaryOp {
                left: Box::new(acc),
                operator: op,
                right: Box::new(right),
            }),
    ))
}

// Parse primary expressions (values, columns, parentheses)
fn parse_primary_expression(input: &str) -> IResult<&str, Expression> {
    alt((
        // Function call: name(args...)
        map(
            pair(
                parse_identifier_optimized,
                delimited(
                    char('('),
                    separated_list0(
                        delimited(multispace0, char(','), multispace0),
                        parse_expression,
                    ),
                    char(')'),
                ),
            ),
            |(name, args)| Expression::FunctionCall { name, args },
        ),
        // Parenthesized expressions
        delimited(
            char('('),
            delimited(multispace0, parse_expression, multispace0),
            char(')'),
        ),
        // Column references
        map(parse_identifier_optimized, Expression::Column),
        // Literal values
        map(parse_sql_value, Expression::Value),
    ))
    .parse(input)
}

// Parse additive operators (+ and -)
fn parse_additive_operator(input: &str) -> IResult<&str, ArithmeticOperator> {
    alt((
        map(char('+'), |_| ArithmeticOperator::Add),
        map(char('-'), |_| ArithmeticOperator::Subtract),
    ))
    .parse(input)
}

// Parse multiplicative operators (* and /)
fn parse_multiplicative_operator(input: &str) -> IResult<&str, ArithmeticOperator> {
    alt((
        map(char('*'), |_| ArithmeticOperator::Multiply),
        map(char('/'), |_| ArithmeticOperator::Divide),
        map(char('%'), |_| ArithmeticOperator::Modulo),
    ))
    .parse(input)
}
