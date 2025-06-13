//! SQL parser implementation using nom
//! 
//! This module provides a SQL parser for basic database operations including
//! SELECT, INSERT, UPDATE, DELETE, and CREATE TABLE statements.

use nom::{
    branch::alt,
    bytes::complete::{tag, tag_no_case, take_while1},
    character::complete::{alpha1, alphanumeric1, char, multispace0, multispace1, digit1},
    combinator::{map, opt, recognize},
    multi::{many0, separated_list1},
    sequence::{delimited, pair, preceded, tuple},
    IResult,
};

#[derive(Debug, Clone, PartialEq)]
pub enum Statement {
    Select(SelectStatement),
    Insert(InsertStatement),
    Update(UpdateStatement),
    Delete(DeleteStatement),
    CreateTable(CreateTableStatement),
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
    pub value: SqlValue,
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

// Parser implementation
pub fn parse_sql(input: &str) -> IResult<&str, Statement> {
    delimited(
        multispace0,
        alt((
            map(parse_select, Statement::Select),
            map(parse_insert, Statement::Insert),
            map(parse_update, Statement::Update),
            map(parse_delete, Statement::Delete),
            map(parse_create_table, Statement::CreateTable),
            map(parse_begin, |_| Statement::Begin),
            map(parse_commit, |_| Statement::Commit),
            map(parse_rollback, |_| Statement::Rollback),
        )),
        multispace0,
    )(input)
}

// Parse SELECT statement
fn parse_select(input: &str) -> IResult<&str, SelectStatement> {
    let (input, _) = tag_no_case("SELECT")(input)?;
    let (input, _) = multispace1(input)?;
    let (input, columns) = parse_column_list(input)?;
    let (input, _) = multispace1(input)?;
    let (input, _) = tag_no_case("FROM")(input)?;
    let (input, _) = multispace1(input)?;
    let (input, table) = parse_identifier(input)?;
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
    let (input, table) = parse_identifier(input)?;
    let (input, _) = multispace0(input)?;
    let (input, columns) = delimited(
        char('('),
        delimited(
            multispace0,
            separated_list1(
                delimited(multispace0, char(','), multispace0),
                parse_identifier,
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
    let (input, table) = parse_identifier(input)?;
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
    let (input, table) = parse_identifier(input)?;
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
    let (input, table) = parse_identifier(input)?;
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

    Ok((
        input,
        CreateTableStatement { table, columns },
    ))
}

// Parse BEGIN statement
fn parse_begin(input: &str) -> IResult<&str, ()> {
    let (input, _) = alt((
        tag_no_case("BEGIN"),
        tag_no_case("START TRANSACTION"),
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

// Parse column list (for SELECT)
fn parse_column_list(input: &str) -> IResult<&str, Vec<String>> {
    alt((
        map(char('*'), |_| vec!["*".to_string()]),
        separated_list1(
            delimited(multispace0, char(','), multispace0),
            parse_identifier,
        ),
    ))(input)
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

// Parse comparison operators
fn parse_comparison_operator(input: &str) -> IResult<&str, ComparisonOperator> {
    alt((
        map(tag("<="), |_| ComparisonOperator::LessThanOrEqual),
        map(tag(">="), |_| ComparisonOperator::GreaterThanOrEqual),
        map(tag("!="), |_| ComparisonOperator::NotEqual),
        map(tag("<>"), |_| ComparisonOperator::NotEqual),
        map(tag("="), |_| ComparisonOperator::Equal),
        map(tag("<"), |_| ComparisonOperator::LessThan),
        map(tag(">"), |_| ComparisonOperator::GreaterThan),
        map(tag_no_case("LIKE"), |_| ComparisonOperator::Like),
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
    let (input, value) = parse_sql_value(input)?;

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

// Parse string literal
fn parse_string_literal(input: &str) -> IResult<&str, String> {
    delimited(
        char('\''),
        map(
            take_while1(|c| c != '\''),
            |s: &str| s.to_string(),
        ),
        char('\''),
    )(input)
}

// Parse integer
fn parse_integer(input: &str) -> IResult<&str, i64> {
    map(
        recognize(pair(opt(char('-')), digit1)),
        |s: &str| s.parse().unwrap(),
    )(input)
}

// Parse real number
fn parse_real(input: &str) -> IResult<&str, f64> {
    map(
        recognize(tuple((
            opt(char('-')),
            digit1,
            char('.'),
            digit1,
        ))),
        |s: &str| s.parse().unwrap(),
    )(input)
}

// Parse identifier
fn parse_identifier(input: &str) -> IResult<&str, String> {
    map(
        recognize(pair(
            alt((alpha1, tag("_"))),
            many0(alt((alphanumeric1, tag("_")))),
        )),
        |s: &str| s.to_string(),
    )(input)
}

