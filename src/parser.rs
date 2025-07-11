//! SQL parser implementation using nom
//!
//! This module provides a SQL parser for basic database operations including
//! SELECT, INSERT, UPDATE, DELETE, and CREATE TABLE statements.

use nom::{
    branch::alt,
    bytes::complete::{tag, tag_no_case, take_while1},
    character::complete::{alpha1, alphanumeric1, char, digit1, multispace0, multispace1},
    combinator::{map, opt, recognize},
    multi::{many0, separated_list1},
    sequence::{delimited, pair, preceded},
    IResult, Parser,
};
use std::collections::HashMap;

// String interning removed for simplicity

// Optimized identifier parsing
fn parse_identifier_optimized(input: &str) -> IResult<&str, String> {
    let (input, identifier) = recognize(pair(
        alt((alpha1, tag("_"))),
        many0(alt((alphanumeric1, tag("_")))),
    ))
    .parse(input)?;

    Ok((input, identifier.to_string()))
}

// Optimized column list parsing
fn parse_column_list_optimized(input: &str) -> IResult<&str, Vec<String>> {
    let (input, _) = multispace0.parse(input)?;
    // Handle the special case of "*" (all columns)
    if let Ok((input, _)) = tag::<&str, &str, nom::error::Error<&str>>("*").parse(input) {
        return Ok((input, vec!["*".to_string()]));
    }
    // Parse comma-separated list of identifiers
    let (input, columns) = separated_list1(
        delimited(multispace0, char(','), multispace0),
        parse_identifier_optimized,
    )
    .parse(input)?;
    Ok((input, columns))
}

// Safe integer parsing with error handling
fn parse_u64_safe(s: &str) -> Result<u64, String> {
    s.parse::<u64>()
        .map_err(|e| format!("Invalid integer: {e}"))
}

// Safe float parsing with error handling
fn parse_f64_safe(s: &str) -> Result<f64, String> {
    s.parse::<f64>().map_err(|e| format!("Invalid float: {e}"))
}

#[derive(Debug, Clone, PartialEq)]
pub enum Statement {
    Select(SelectStatement),
    Insert(InsertStatement),
    Update(UpdateStatement),
    Delete(DeleteStatement),
    CreateTable(CreateTableStatement),
    DropTable(DropTableStatement),
    Begin,
    Commit,
    Rollback,
}

#[derive(Debug, Clone, PartialEq)]
pub struct SelectStatement {
    pub columns: Vec<String>,
    pub table: String,
    pub where_clause: Option<WhereClause>,
    pub limit: Option<u64>,
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
pub struct ColumnDefinition {
    pub name: String,
    pub data_type: DataType,
    pub constraints: Vec<ColumnConstraint>,
}

#[derive(Debug, Clone, PartialEq)]
pub enum DataType {
    Integer,
    Text,
    Real,
    Blob,
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
pub enum Condition {
    Comparison {
        left: String,
        operator: ComparisonOperator,
        right: SqlValue,
    },
    And(Box<Condition>, Box<Condition>),
    Or(Box<Condition>, Box<Condition>),
}

#[derive(Debug, Clone, PartialEq)]
pub enum ComparisonOperator {
    Equal,
    NotEqual,
    LessThan,
    LessThanOrEqual,
    GreaterThan,
    GreaterThanOrEqual,
    Like,
    Modulo,
}

#[derive(Debug, Clone, PartialEq)]
pub struct Assignment {
    pub column: String,
    pub value: Expression,
}

// Remove OrderByClause and OrderDirection

#[derive(Debug, Clone, PartialEq)]
pub enum SqlValue {
    Integer(i64),
    Real(f64),
    Text(String),
    Null,
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
}

// Expression evaluation
impl Expression {
    /// Evaluate an expression given a context (current row data)
    pub fn evaluate(&self, context: &HashMap<String, SqlValue>) -> Result<SqlValue, String> {
        match self {
            Expression::Value(value) => Ok(value.clone()),
            Expression::Column(column_name) => context
                .get(column_name)
                .cloned()
                .ok_or_else(|| format!("Column '{column_name}' not found")),
            Expression::BinaryOp {
                left,
                operator,
                right,
            } => {
                let left_val = left.evaluate(context)?;
                let right_val = right.evaluate(context)?;

                match (left_val, right_val) {
                    (SqlValue::Integer(l), SqlValue::Integer(r)) => match operator {
                        ArithmeticOperator::Add => Ok(SqlValue::Integer(l + r)),
                        ArithmeticOperator::Subtract => Ok(SqlValue::Integer(l - r)),
                        ArithmeticOperator::Multiply => Ok(SqlValue::Integer(l * r)),
                        ArithmeticOperator::Divide => {
                            if r == 0 {
                                Err("Division by zero".to_string())
                            } else {
                                Ok(SqlValue::Integer(l / r))
                            }
                        }
                        ArithmeticOperator::Modulo => {
                            if r == 0 {
                                Err("Modulo by zero".to_string())
                            } else {
                                Ok(SqlValue::Integer(l % r))
                            }
                        }
                    },
                    (SqlValue::Real(l), SqlValue::Real(r)) => match operator {
                        ArithmeticOperator::Add => Ok(SqlValue::Real(l + r)),
                        ArithmeticOperator::Subtract => Ok(SqlValue::Real(l - r)),
                        ArithmeticOperator::Multiply => Ok(SqlValue::Real(l * r)),
                        ArithmeticOperator::Divide => {
                            if r == 0.0 {
                                Err("Division by zero".to_string())
                            } else {
                                Ok(SqlValue::Real(l / r))
                            }
                        }
                        ArithmeticOperator::Modulo => {
                            if r == 0.0 {
                                Err("Modulo by zero".to_string())
                            } else {
                                Ok(SqlValue::Real(l % r))
                            }
                        }
                    },
                    (SqlValue::Integer(l), SqlValue::Real(r)) => {
                        let l = l as f64;
                        match operator {
                            ArithmeticOperator::Add => Ok(SqlValue::Real(l + r)),
                            ArithmeticOperator::Subtract => Ok(SqlValue::Real(l - r)),
                            ArithmeticOperator::Multiply => Ok(SqlValue::Real(l * r)),
                            ArithmeticOperator::Divide => {
                                if r == 0.0 {
                                    Err("Division by zero".to_string())
                                } else {
                                    Ok(SqlValue::Real(l / r))
                                }
                            }
                            ArithmeticOperator::Modulo => {
                                if r == 0.0 {
                                    Err("Modulo by zero".to_string())
                                } else {
                                    Ok(SqlValue::Real(l % r))
                                }
                            }
                        }
                    }
                    (SqlValue::Real(l), SqlValue::Integer(r)) => {
                        let r = r as f64;
                        match operator {
                            ArithmeticOperator::Add => Ok(SqlValue::Real(l + r)),
                            ArithmeticOperator::Subtract => Ok(SqlValue::Real(l - r)),
                            ArithmeticOperator::Multiply => Ok(SqlValue::Real(l * r)),
                            ArithmeticOperator::Divide => {
                                if r == 0.0 {
                                    Err("Division by zero".to_string())
                                } else {
                                    Ok(SqlValue::Real(l / r))
                                }
                            }
                            ArithmeticOperator::Modulo => {
                                if r == 0.0 {
                                    Err("Modulo by zero".to_string())
                                } else {
                                    Ok(SqlValue::Real(l % r))
                                }
                            }
                        }
                    }
                    (SqlValue::Text(l), SqlValue::Text(r)) => match operator {
                        ArithmeticOperator::Add => Ok(SqlValue::Text(format!("{l}{r}"))),
                        _ => Err("Only addition (+) is supported for text values".to_string()),
                    },
                    _ => Err("Unsupported operand types for arithmetic operation".to_string()),
                }
            }
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub enum ArithmeticOperator {
    Add,
    Subtract,
    Multiply,
    Divide,
    Modulo,
}

/// Parse a complete SQL statement
pub fn parse_sql(input: &str) -> Result<Statement, String> {
    let (remaining, statement) = parse_statement
        .parse(input)
        .map_err(|e| format!("Parse error: {e:?}"))?;

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
        parse_begin_transaction,
        parse_commit,
        parse_rollback,
    ))
    .parse(input)
}

/// Parse CREATE TABLE statement
fn parse_create_table(input: &str) -> IResult<&str, Statement> {
    let (input, _) = delimited(multispace0, tag_no_case("CREATE"), multispace1).parse(input)?;

    let (input, _) = tag_no_case("TABLE").parse(input)?;
    let (input, _) = multispace1.parse(input)?;

    // Try to parse optional "IF EXISTS"
    let (input, _if_exists) = opt((
        tag_no_case("IF"),
        multispace1,
        tag_no_case("EXISTS"),
        multispace1,
    ))
    .parse(input)?;

    let (input, table_name) = parse_identifier_optimized.parse(input)?;
    let (input, _) = multispace0.parse(input)?;
    let (input, _) = char('(').parse(input)?;
    let (input, _) = multispace0.parse(input)?;

    let (input, columns) = separated_list1(
        delimited(multispace0, char(','), multispace0),
        parse_column_definition,
    )
    .parse(input)?;

    let (input, _) = multispace0.parse(input)?;
    let (input, _) = char(')').parse(input)?;
    let (input, _) = multispace0.parse(input)?;
    let (input, _) = opt(char(';')).parse(input)?;

    Ok((
        input,
        Statement::CreateTable(CreateTableStatement {
            table: table_name.to_string(),
            columns,
        }),
    ))
}

/// Parse INSERT statement
fn parse_insert(input: &str) -> IResult<&str, Statement> {
    let (input, _) = delimited(multispace0, tag_no_case("INSERT"), multispace1).parse(input)?;
    let (input, _) = tag_no_case("INTO").parse(input)?;
    let (input, _) = multispace1.parse(input)?;
    let (input, table_name) = parse_identifier_optimized.parse(input)?;
    let (input, _) = multispace0.parse(input)?;
    let (input, _) = char('(').parse(input)?;
    let (input, _) = multispace0.parse(input)?;
    let (input, columns) = separated_list1(
        delimited(multispace0, char(','), multispace0),
        parse_identifier_optimized,
    )
    .parse(input)?;
    let (input, _) = multispace0.parse(input)?;
    let (input, _) = char(')').parse(input)?;
    let (input, _) = multispace1.parse(input)?;
    let (input, _) = tag_no_case("VALUES").parse(input)?;
    let (input, _) = multispace0.parse(input)?;
    // Parse one or more value tuples
    let (input, values) = separated_list1(
        delimited(multispace0, char(','), multispace0),
        delimited(
            char('('),
            separated_list1(
                delimited(multispace0, char(','), multispace0),
                parse_sql_value,
            ),
            char(')'),
        ),
    )
    .parse(input)?;
    let (input, _) = multispace0.parse(input)?;
    let (input, _) = opt(char(';')).parse(input)?;
    Ok((
        input,
        Statement::Insert(InsertStatement {
            table: table_name.to_string(),
            columns: columns.into_iter().map(|s| s.to_string()).collect(),
            values,
        }),
    ))
}

/// Parse SELECT statement
fn parse_select(input: &str) -> IResult<&str, Statement> {
    let (input, _) = delimited(multispace0, tag_no_case("SELECT"), multispace1).parse(input)?;

    let (input, columns) = parse_column_list_optimized.parse(input)?;
    let (input, _) = multispace0.parse(input)?;
    let (input, _) = tag_no_case("FROM").parse(input)?;
    let (input, _) = multispace1.parse(input)?;

    let (input, table_name) = parse_identifier_optimized.parse(input)?;

    // Parse optional WHERE clause
    let (input, where_clause) = opt(preceded(
        delimited(multispace0, tag_no_case("WHERE"), multispace1),
        parse_where_clause,
    ))
    .parse(input)?;

    // Remove ORDER BY clause parsing
    // Parse optional LIMIT clause
    let (input, limit) = opt(preceded(
        delimited(multispace0, tag_no_case("LIMIT"), multispace1),
        parse_limit,
    ))
    .parse(input)?;
    // Accept any trailing whitespace and optional semicolon after LIMIT
    let (input, _) = multispace0.parse(input)?;
    let (input, _) = opt(char(';')).parse(input)?;
    let (input, _) = multispace0.parse(input)?;
    // Accept end of input
    if !input.trim().is_empty() {
        return Err(nom::Err::Error(nom::error::Error::new(
            input,
            nom::error::ErrorKind::Eof,
        )));
    }
    Ok((
        input,
        Statement::Select(SelectStatement {
            columns,
            table: table_name.to_string(),
            where_clause,
            limit,
        }),
    ))
}

/// Parse UPDATE statement
fn parse_update(input: &str) -> IResult<&str, Statement> {
    let (input, _) = delimited(multispace0, tag_no_case("UPDATE"), multispace1).parse(input)?;

    let (input, table_name) = parse_identifier_optimized.parse(input)?;
    let (input, _) = multispace1.parse(input)?;
    let (input, _) = tag_no_case("SET").parse(input)?;
    let (input, _) = multispace0.parse(input)?;

    let (input, assignments) = separated_list1(
        delimited(multispace0, char(','), multispace0),
        parse_assignment,
    )
    .parse(input)?;

    // Parse optional WHERE clause
    let (input, where_clause) = opt(preceded(
        delimited(multispace0, tag_no_case("WHERE"), multispace1),
        parse_where_clause,
    ))
    .parse(input)?;

    let (input, _) = multispace0.parse(input)?;
    let (input, _) = opt(char(';')).parse(input)?;

    Ok((
        input,
        Statement::Update(UpdateStatement {
            table: table_name.to_string(),
            assignments,
            where_clause,
        }),
    ))
}

/// Parse DELETE statement
fn parse_delete(input: &str) -> IResult<&str, Statement> {
    let (input, _) = delimited(multispace0, tag_no_case("DELETE"), multispace1).parse(input)?;

    let (input, _) = tag_no_case("FROM").parse(input)?;
    let (input, _) = multispace1.parse(input)?;

    let (input, table_name) = parse_identifier_optimized.parse(input)?;

    // Parse optional WHERE clause
    let (input, where_clause) = opt(preceded(
        delimited(multispace0, tag_no_case("WHERE"), multispace1),
        parse_where_clause,
    ))
    .parse(input)?;

    let (input, _) = multispace0.parse(input)?;
    let (input, _) = opt(char(';')).parse(input)?;

    Ok((
        input,
        Statement::Delete(DeleteStatement {
            table: table_name.to_string(),
            where_clause,
        }),
    ))
}

/// Parse DROP TABLE statement
fn parse_drop_table(input: &str) -> IResult<&str, Statement> {
    let (input, _) = delimited(multispace0, tag_no_case("DROP"), multispace1).parse(input)?;

    let (input, _) = tag_no_case("TABLE").parse(input)?;
    let (input, _) = multispace1.parse(input)?;

    // Try to parse optional "IF EXISTS"
    let (input, if_exists) = opt((
        tag_no_case("IF"),
        multispace1,
        tag_no_case("EXISTS"),
        multispace1,
    ))
    .parse(input)?;

    let (input, table_name) = parse_identifier_optimized.parse(input)?;
    let (input, _) = multispace0.parse(input)?;
    let (input, _) = opt(char(';')).parse(input)?;

    Ok((
        input,
        Statement::DropTable(DropTableStatement {
            table: table_name.to_string(),
            if_exists: if_exists.is_some(),
        }),
    ))
}

/// Parse BEGIN TRANSACTION statement
fn parse_begin_transaction(input: &str) -> IResult<&str, Statement> {
    alt((
        map(
            delimited(multispace0, tag_no_case("BEGIN"), multispace0),
            |_| Statement::Begin,
        ),
        map(
            delimited(
                multispace0,
                (
                    tag_no_case("START"),
                    multispace1,
                    tag_no_case("TRANSACTION"),
                ),
                multispace0,
            ),
            |_| Statement::Begin,
        ),
    ))
    .parse(input)
}

/// Parse COMMIT statement
fn parse_commit(input: &str) -> IResult<&str, Statement> {
    let (input, _) = delimited(multispace0, tag_no_case("COMMIT"), multispace0).parse(input)?;

    let (input, _) = opt(char(';')).parse(input)?;

    Ok((input, Statement::Commit))
}

/// Parse ROLLBACK statement
fn parse_rollback(input: &str) -> IResult<&str, Statement> {
    let (input, _) = delimited(multispace0, tag_no_case("ROLLBACK"), multispace0).parse(input)?;

    let (input, _) = opt(char(';')).parse(input)?;

    Ok((input, Statement::Rollback))
}

// Parse WHERE clause
fn parse_where_clause(input: &str) -> IResult<&str, WhereClause> {
    let (input, condition) = parse_condition.parse(input)?;

    Ok((input, WhereClause { condition }))
}

// Parse condition
fn parse_condition(input: &str) -> IResult<&str, Condition> {
    parse_or_condition.parse(input)
}

fn parse_or_condition(input: &str) -> IResult<&str, Condition> {
    let (input, left) = parse_and_condition.parse(input)?;
    let (input, rights) = many0(preceded(
        delimited(multispace1, tag_no_case("OR"), multispace1),
        parse_and_condition,
    ))
    .parse(input)?;

    Ok((
        input,
        rights.into_iter().fold(left, |acc, right| {
            Condition::Or(Box::new(acc), Box::new(right))
        }),
    ))
}

fn parse_and_condition(input: &str) -> IResult<&str, Condition> {
    let (input, left) = parse_primary_condition.parse(input)?;
    let (input, rights) = many0(preceded(
        delimited(multispace1, tag_no_case("AND"), multispace1),
        parse_primary_condition,
    ))
    .parse(input)?;

    Ok((
        input,
        rights.into_iter().fold(left, |acc, right| {
            Condition::And(Box::new(acc), Box::new(right))
        }),
    ))
}

fn parse_primary_condition(input: &str) -> IResult<&str, Condition> {
    alt((
        // Parenthesized conditions
        delimited(
            char('('),
            delimited(multispace0, parse_condition, multispace0),
            char(')'),
        ),
        // Simple comparisons
        parse_comparison,
    ))
    .parse(input)
}

fn parse_comparison(input: &str) -> IResult<&str, Condition> {
    let (input, left) = parse_expression.parse(input)?;
    let (input, _) = multispace0.parse(input)?;
    let (input, operator) = parse_comparison_operator.parse(input)?;
    let (input, _) = multispace0.parse(input)?;
    let (input, right) = parse_expression.parse(input)?;

    // Convert expressions to string representation for compatibility
    let left_str = match &left {
        Expression::Column(name) => name.clone(),
        Expression::Value(val) => format!("{val:?}"),
        Expression::BinaryOp {
            left,
            operator,
            right,
        } => {
            format!("{left:?} {operator:?} {right:?}")
        }
    };

    // For the right side, try to evaluate it if it's a simple expression
    let right_value = match &right {
        Expression::Value(val) => val.clone(),
        Expression::Column(name) => SqlValue::Text(name.clone()),
        Expression::BinaryOp {
            left,
            operator,
            right,
        } => {
            // For complex expressions, create a string representation
            SqlValue::Text(format!("{left:?} {operator:?} {right:?}"))
        }
    };

    Ok((
        input,
        Condition::Comparison {
            left: left_str,
            operator,
            right: right_value,
        },
    ))
}

// Parse comparison operators - optimized order but multi-char first to avoid partial matches
fn parse_comparison_operator(input: &str) -> IResult<&str, ComparisonOperator> {
    alt((
        // Multi-character operators first to avoid partial matches
        map(tag("<="), |_| ComparisonOperator::LessThanOrEqual),
        map(tag(">="), |_| ComparisonOperator::GreaterThanOrEqual),
        map(tag("!="), |_| ComparisonOperator::NotEqual),
        map(tag("<>"), |_| ComparisonOperator::NotEqual),
        map(tag_no_case("LIKE"), |_| ComparisonOperator::Like),
        map(tag_no_case("MOD"), |_| ComparisonOperator::Modulo),
        // Single-character operators last
        map(tag("="), |_| ComparisonOperator::Equal),
        map(tag("<"), |_| ComparisonOperator::LessThan),
        map(tag(">"), |_| ComparisonOperator::GreaterThan),
        map(tag("%"), |_| ComparisonOperator::Modulo),
    ))
    .parse(input)
}

// Remove parse_order_by and parse_order_by_column

// Parse LIMIT clause
fn parse_limit(input: &str) -> IResult<&str, u64> {
    let (input, _) = multispace0.parse(input)?;
    let (input, limit_str) = digit1.parse(input)?;
    let limit = parse_u64_safe(limit_str)
        .map_err(|_| nom::Err::Error(nom::error::Error::new(input, nom::error::ErrorKind::Tag)))?;
    Ok((input, limit))
}

// Parse assignment (for UPDATE)
fn parse_assignment(input: &str) -> IResult<&str, Assignment> {
    let (input, column) = parse_identifier_optimized.parse(input)?;
    let (input, _) = multispace0.parse(input)?;
    let (input, _) = char('=').parse(input)?;
    let (input, _) = multispace0.parse(input)?;
    let (input, value) = parse_expression.parse(input)?;

    Ok((input, Assignment { column, value }))
}

// Parse column definition (for CREATE TABLE)
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

// Parse data types
fn parse_data_type(input: &str) -> IResult<&str, DataType> {
    alt((
        map(tag_no_case("INTEGER"), |_| DataType::Integer),
        map(tag_no_case("INT"), |_| DataType::Integer),
        map(tag_no_case("TEXT"), |_| DataType::Text),
        map(tag_no_case("VARCHAR"), |_| DataType::Text),
        map(tag_no_case("REAL"), |_| DataType::Real),
        map(tag_no_case("FLOAT"), |_| DataType::Real),
        map(tag_no_case("BLOB"), |_| DataType::Blob),
    ))
    .parse(input)
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

// Parse SQL values
fn parse_sql_value(input: &str) -> IResult<&str, SqlValue> {
    alt((
        map(tag_no_case("NULL"), |_| SqlValue::Null),
        map(parse_string_literal, SqlValue::Text),
        map(parse_real, SqlValue::Real),
        map(parse_integer, SqlValue::Integer),
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
        let value = int_str.parse::<i64>().map_err(|_| {
            nom::Err::Error(nom::error::Error::new(input, nom::error::ErrorKind::Tag))
        })?;
        Ok((input, value))
    }
}

// Parse real number with error handling
fn parse_real(input: &str) -> IResult<&str, f64> {
    let (input, real_str) = recognize((opt(char('-')), digit1, char('.'), digit1)).parse(input)?;
    let value = parse_f64_safe(real_str)
        .map_err(|_| nom::Err::Error(nom::error::Error::new(input, nom::error::ErrorKind::Tag)))?;
    Ok((input, value))
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
