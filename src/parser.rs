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
    sequence::{delimited, pair, preceded, tuple},
    IResult,
};
use std::collections::HashMap;

// String interning for common SQL keywords and identifiers
thread_local! {
    static STRING_CACHE: std::cell::RefCell<HashMap<String, String>> = std::cell::RefCell::new(HashMap::new());
}

fn intern_string(s: &str) -> String {
    STRING_CACHE.with(|cache| {
        let mut cache = cache.borrow_mut();
        if let Some(interned) = cache.get(s) {
            return interned.clone();
        }
        let owned = s.to_string();
        cache.insert(owned.clone(), owned.clone());
        owned
    })
}

// Optimized identifier parsing with string interning
fn parse_identifier_optimized(input: &str) -> IResult<&str, String> {
    let (input, identifier) = recognize(pair(
        alt((alpha1, tag("_"))),
        many0(alt((alphanumeric1, tag("_")))),
    ))(input)?;
    
    // Use string interning for better performance
    Ok((input, intern_string(identifier)))
}

// Optimized column list parsing
fn parse_column_list_optimized(input: &str) -> IResult<&str, Vec<String>> {
    let (input, _) = multispace0(input)?;
    // Handle the special case of "*" (all columns)
    if let Ok((input, _)) = tag::<&str, &str, nom::error::Error<&str>>("*")(input) {
        return Ok((input, vec!["*".to_string()]));
    }
    // Try to parse a single identifier (column name)
    let (input, first_col) = parse_identifier_optimized(input)?;
    // Only consume whitespace before a comma, not after the last column
    let (input, rest_cols) = many0(
        preceded(
            tuple((multispace0, char(','), multispace0)),
            parse_identifier_optimized,
        )
    )(input)?;
    let mut columns = Vec::with_capacity(1 + rest_cols.len());
    columns.push(first_col);
    columns.extend(rest_cols);
    Ok((input, columns))
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
    pub order_by: Option<Vec<OrderByClause>>,
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
}

#[derive(Debug, Clone, PartialEq)]
pub struct Assignment {
    pub column: String,
    pub value: Expression,
}

#[derive(Debug, Clone, PartialEq)]
pub struct OrderByClause {
    pub column: String,
    pub direction: OrderDirection,
}

#[derive(Debug, Clone, PartialEq)]
pub enum OrderDirection {
    Asc,
    Desc,
}

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
}

// Parse main SQL statement with fast paths for common cases
pub fn parse_sql(input: &str) -> IResult<&str, Statement> {
    delimited(
        multispace0,
        alt((
            // Fast path for transaction commands (most common)
            map(parse_begin, |_| Statement::Begin),
            map(parse_commit, |_| Statement::Commit),
            map(parse_rollback, |_| Statement::Rollback),
            // Data manipulation commands
            map(parse_select, Statement::Select),
            map(parse_insert, Statement::Insert),
            map(parse_update, Statement::Update),
            map(parse_delete, Statement::Delete),
            // DDL commands (least common)
            map(parse_create_table, Statement::CreateTable),
            map(parse_drop_table, Statement::DropTable),
        )),
        multispace0,
    )(input)
}

// Parse SELECT statement
fn parse_select(input: &str) -> IResult<&str, SelectStatement> {
    let (input, _) = tag_no_case("SELECT")(input)?;
    let (input, _) = multispace1(input)?;
    let (input, columns) = parse_column_list_optimized(input)?;
    let (input, _) = multispace1(input)?;
    let (input, _) = tag_no_case("FROM")(input)?;
    let (input, _) = multispace1(input)?;
    let (input, table) = parse_identifier_optimized(input)?;
    let (input, where_clause) = opt(preceded(multispace1, parse_where_clause))(input)?;
    let (input, order_by) = opt(preceded(multispace1, parse_order_by))(input)?;
    let (input, limit) = opt(preceded(multispace1, parse_limit))(input)?;

    Ok((
        input,
        SelectStatement {
            columns,
            table,
            where_clause,
            order_by,
            limit,
        },
    ))
}

// Parse INSERT statement
fn parse_insert(input: &str) -> IResult<&str, InsertStatement> {
    let (input, _) = tag_no_case("INSERT")(input)?;
    let (input, _) = multispace1(input)?;
    let (input, _) = tag_no_case("INTO")(input)?;
    let (input, _) = multispace1(input)?;
    let (input, table) = parse_identifier_optimized(input)?;
    let (input, _) = multispace0(input)?;
    let (input, columns) = delimited(
        char('('),
        delimited(
            multispace0,
            separated_list1(
                delimited(multispace0, char(','), multispace0),
                parse_identifier_optimized,
            ),
            multispace0,
        ),
        char(')'),
    )(input)?;
    let (input, _) = multispace1(input)?;
    let (input, _) = tag_no_case("VALUES")(input)?;
    let (input, _) = multispace0(input)?;
    let (input, values) = separated_list1(
        delimited(multispace0, char(','), multispace0),
        delimited(
            char('('),
            delimited(
                multispace0,
                separated_list1(
                    delimited(multispace0, char(','), multispace0),
                    parse_sql_value,
                ),
                multispace0,
            ),
            char(')'),
        ),
    )(input)?;

    Ok((
        input,
        InsertStatement {
            table,
            columns,
            values,
        },
    ))
}

// Parse UPDATE statement
fn parse_update(input: &str) -> IResult<&str, UpdateStatement> {
    let (input, _) = tag_no_case("UPDATE")(input)?;
    let (input, _) = multispace1(input)?;
    let (input, table) = parse_identifier_optimized(input)?;
    let (input, _) = multispace1(input)?;
    let (input, _) = tag_no_case("SET")(input)?;
    let (input, _) = multispace1(input)?;
    let (input, assignments) = separated_list1(
        delimited(multispace0, char(','), multispace0),
        parse_assignment,
    )(input)?;
    let (input, where_clause) = opt(preceded(multispace1, parse_where_clause))(input)?;

    Ok((
        input,
        UpdateStatement {
            table,
            assignments,
            where_clause,
        },
    ))
}

// Parse DELETE statement
fn parse_delete(input: &str) -> IResult<&str, DeleteStatement> {
    let (input, _) = tag_no_case("DELETE")(input)?;
    let (input, _) = multispace1(input)?;
    let (input, _) = tag_no_case("FROM")(input)?;
    let (input, _) = multispace1(input)?;
    let (input, table) = parse_identifier_optimized(input)?;
    let (input, where_clause) = opt(preceded(multispace1, parse_where_clause))(input)?;

    Ok((
        input,
        DeleteStatement {
            table,
            where_clause,
        },
    ))
}

// Parse CREATE TABLE statement
fn parse_create_table(input: &str) -> IResult<&str, CreateTableStatement> {
    let (input, _) = tag_no_case("CREATE")(input)?;
    let (input, _) = multispace1(input)?;
    let (input, _) = tag_no_case("TABLE")(input)?;
    let (input, _) = multispace1(input)?;
    let (input, table) = parse_identifier_optimized(input)?;
    let (input, _) = multispace0(input)?;
    let (input, columns) = delimited(
        char('('),
        delimited(
            multispace0,
            separated_list1(
                delimited(multispace0, char(','), multispace0),
                parse_column_definition,
            ),
            multispace0,
        ),
        char(')'),
    )(input)?;

    Ok((input, CreateTableStatement { table, columns }))
}

// Parse DROP TABLE statement
fn parse_drop_table(input: &str) -> IResult<&str, DropTableStatement> {
    let (input, _) = tag_no_case("DROP")(input)?;
    let (input, _) = multispace1(input)?;
    let (input, _) = tag_no_case("TABLE")(input)?;
    let (input, _) = multispace1(input)?;

    // Try to parse optional "IF EXISTS"
    let (input, if_exists) = opt(tuple((
        tag_no_case("IF"),
        multispace1,
        tag_no_case("EXISTS"),
        multispace1,
    )))(input)?;

    let (input, table) = parse_drop_table_identifier(input)?;

    Ok((
        input,
        DropTableStatement {
            table,
            if_exists: if_exists.is_some(),
        },
    ))
}

// Parse identifier but reject reserved keywords for DROP TABLE context
fn parse_drop_table_identifier(input: &str) -> IResult<&str, String> {
    // First check if the next token is a reserved keyword that would be invalid as a table name
    if let Ok((_, _)) = tag_no_case::<&str, &str, nom::error::Error<&str>>("IF")(input) {
        return Err(nom::Err::Error(nom::error::Error::new(
            input,
            nom::error::ErrorKind::Tag,
        )));
    }
    if let Ok((_, _)) = tag_no_case::<&str, &str, nom::error::Error<&str>>("EXISTS")(input) {
        return Err(nom::Err::Error(nom::error::Error::new(
            input,
            nom::error::ErrorKind::Tag,
        )));
    }

    parse_identifier_optimized(input)
}

// Parse BEGIN statement
fn parse_begin(input: &str) -> IResult<&str, ()> {
    let (input, _) = alt((
        tag_no_case("BEGIN"),
        map(
            tuple((
                tag_no_case("START"),
                multispace1,
                tag_no_case("TRANSACTION"),
            )),
            |_| "",
        ),
    ))(input)?;
    Ok((input, ()))
}

// Parse COMMIT statement
fn parse_commit(input: &str) -> IResult<&str, ()> {
    let (input, _) = tag_no_case("COMMIT")(input)?;
    Ok((input, ()))
}

// Parse ROLLBACK statement
fn parse_rollback(input: &str) -> IResult<&str, ()> {
    let (input, _) = tag_no_case("ROLLBACK")(input)?;
    Ok((input, ()))
}



// Parse WHERE clause
fn parse_where_clause(input: &str) -> IResult<&str, WhereClause> {
    let (input, _) = tag_no_case("WHERE")(input)?;
    let (input, _) = multispace1(input)?;
    let (input, condition) = parse_condition(input)?;

    Ok((input, WhereClause { condition }))
}

// Parse condition
fn parse_condition(input: &str) -> IResult<&str, Condition> {
    parse_or_condition(input)
}

fn parse_or_condition(input: &str) -> IResult<&str, Condition> {
    let (input, left) = parse_and_condition(input)?;
    let (input, rights) = many0(preceded(
        delimited(multispace1, tag_no_case("OR"), multispace1),
        parse_and_condition,
    ))(input)?;

    Ok((
        input,
        rights.into_iter().fold(left, |acc, right| {
            Condition::Or(Box::new(acc), Box::new(right))
        }),
    ))
}

fn parse_and_condition(input: &str) -> IResult<&str, Condition> {
    let (input, left) = parse_comparison(input)?;
    let (input, rights) = many0(preceded(
        delimited(multispace1, tag_no_case("AND"), multispace1),
        parse_comparison,
    ))(input)?;

    Ok((
        input,
        rights.into_iter().fold(left, |acc, right| {
            Condition::And(Box::new(acc), Box::new(right))
        }),
    ))
}

fn parse_comparison(input: &str) -> IResult<&str, Condition> {
    let (input, left) = parse_identifier(input)?;
    let (input, _) = multispace0(input)?;
    let (input, operator) = parse_comparison_operator(input)?;
    let (input, _) = multispace0(input)?;
    let (input, right) = parse_sql_value(input)?;

    Ok((
        input,
        Condition::Comparison {
            left,
            operator,
            right,
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
        // Single-character operators last
        map(tag("="), |_| ComparisonOperator::Equal),
        map(tag("<"), |_| ComparisonOperator::LessThan),
        map(tag(">"), |_| ComparisonOperator::GreaterThan),
    ))(input)
}

// Parse ORDER BY clause
fn parse_order_by(input: &str) -> IResult<&str, Vec<OrderByClause>> {
    let (input, _) = tag_no_case("ORDER")(input)?;
    let (input, _) = multispace1(input)?;
    let (input, _) = tag_no_case("BY")(input)?;
    let (input, _) = multispace1(input)?;
    let (input, columns) = separated_list1(
        delimited(multispace0, char(','), multispace0),
        parse_order_by_column,
    )(input)?;

    Ok((input, columns))
}

fn parse_order_by_column(input: &str) -> IResult<&str, OrderByClause> {
    let (input, column) = parse_identifier(input)?;
    let (input, direction) = opt(preceded(
        multispace1,
        alt((
            map(tag_no_case("ASC"), |_| OrderDirection::Asc),
            map(tag_no_case("DESC"), |_| OrderDirection::Desc),
        )),
    ))(input)?;

    Ok((
        input,
        OrderByClause {
            column,
            direction: direction.unwrap_or(OrderDirection::Asc),
        },
    ))
}

// Parse LIMIT clause
fn parse_limit(input: &str) -> IResult<&str, u64> {
    let (input, _) = tag_no_case("LIMIT")(input)?;
    let (input, _) = multispace1(input)?;
    let (input, limit) = map(digit1, |s: &str| s.parse::<u64>().unwrap())(input)?;

    Ok((input, limit))
}

// Parse assignment (for UPDATE)
fn parse_assignment(input: &str) -> IResult<&str, Assignment> {
    let (input, column) = parse_identifier(input)?;
    let (input, _) = multispace0(input)?;
    let (input, _) = char('=')(input)?;
    let (input, _) = multispace0(input)?;
    let (input, value) = parse_expression(input)?;

    Ok((input, Assignment { column, value }))
}

// Parse column definition (for CREATE TABLE)
fn parse_column_definition(input: &str) -> IResult<&str, ColumnDefinition> {
    let (input, name) = parse_identifier(input)?;
    let (input, _) = multispace1(input)?;
    let (input, data_type) = parse_data_type(input)?;
    let (input, constraints) = many0(preceded(multispace1, parse_column_constraint))(input)?;

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
    ))(input)
}

// Parse column constraints
fn parse_column_constraint(input: &str) -> IResult<&str, ColumnConstraint> {
    alt((
        map(
            tuple((tag_no_case("PRIMARY"), multispace1, tag_no_case("KEY"))),
            |_| ColumnConstraint::PrimaryKey,
        ),
        map(
            tuple((tag_no_case("NOT"), multispace1, tag_no_case("NULL"))),
            |_| ColumnConstraint::NotNull,
        ),
        map(tag_no_case("UNIQUE"), |_| ColumnConstraint::Unique),
    ))(input)
}

// Parse SQL values
fn parse_sql_value(input: &str) -> IResult<&str, SqlValue> {
    alt((
        map(tag_no_case("NULL"), |_| SqlValue::Null),
        map(parse_string_literal, SqlValue::Text),
        map(parse_real, SqlValue::Real),
        map(parse_integer, SqlValue::Integer),
    ))(input)
}

// Parse string literal - optimized version with selective interning
fn parse_string_literal(input: &str) -> IResult<&str, String> {
    delimited(
        char('\''),
        map(take_while1(|c| c != '\''), |s: &str| {
            // Only intern very common, short strings to avoid overhead
            if s.len() <= 16 && s.is_ascii() {
                match s {
                    "active" | "inactive" | "pending" | "admin" | "user" | "guest" => {
                        intern_string(s)
                    }
                    _ => s.to_string(),
                }
            } else {
                s.to_string()
            }
        }),
        char('\''),
    )(input)
}

// Parse integer - optimized version
fn parse_integer(input: &str) -> IResult<&str, i64> {
    map(recognize(pair(opt(char('-')), digit1)), |s: &str| {
        // Fast path for small positive integers
        if s.len() <= 3 && !s.starts_with('-') {
            let mut result = 0i64;
            for byte in s.bytes() {
                result = result * 10 + (byte - b'0') as i64;
            }
            result
        } else {
            s.parse().unwrap()
        }
    })(input)
}

// Parse real number
fn parse_real(input: &str) -> IResult<&str, f64> {
    map(
        recognize(tuple((opt(char('-')), digit1, char('.'), digit1))),
        |s: &str| s.parse().unwrap(),
    )(input)
}

// Parse identifier - optimized version with selective interning
fn parse_identifier(input: &str) -> IResult<&str, String> {
    map(
        recognize(pair(
            alt((alpha1, tag("_"))),
            many0(alt((alphanumeric1, tag("_")))),
        )),
        |s: &str| {
            // Only intern very common, short identifiers to avoid overhead
            if s.len() <= 8 && s.is_ascii() {
                match s {
                    "id" | "name" | "age" | "email" | "user" | "users" | "data" | "table" => {
                        intern_string(s)
                    }
                    _ => s.to_string(),
                }
            } else {
                s.to_string()
            }
        },
    )(input)
}

// Parse expression (supports arithmetic operations)
fn parse_expression(input: &str) -> IResult<&str, Expression> {
    parse_additive_expression(input)
}

// Parse additive expressions (+ and -)
fn parse_additive_expression(input: &str) -> IResult<&str, Expression> {
    let (input, left) = parse_multiplicative_expression(input)?;
    let (input, rights) = many0(tuple((
        delimited(multispace0, parse_additive_operator, multispace0),
        parse_multiplicative_expression,
    )))(input)?;

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
    let (input, left) = parse_primary_expression(input)?;
    let (input, rights) = many0(tuple((
        delimited(multispace0, parse_multiplicative_operator, multispace0),
        parse_primary_expression,
    )))(input)?;

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
        map(parse_identifier, Expression::Column),
        // Literal values
        map(parse_sql_value, Expression::Value),
    ))(input)
}

// Parse additive operators (+ and -)
fn parse_additive_operator(input: &str) -> IResult<&str, ArithmeticOperator> {
    alt((
        map(char('+'), |_| ArithmeticOperator::Add),
        map(char('-'), |_| ArithmeticOperator::Subtract),
    ))(input)
}

// Parse multiplicative operators (* and /)
fn parse_multiplicative_operator(input: &str) -> IResult<&str, ArithmeticOperator> {
    alt((
        map(char('*'), |_| ArithmeticOperator::Multiply),
        map(char('/'), |_| ArithmeticOperator::Divide),
    ))(input)
}
