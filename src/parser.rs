//! SQL parser implementation using nom
//!
//! This module provides a SQL parser for basic database operations including
//! SELECT, INSERT, UPDATE, DELETE, and CREATE TABLE statements.

use nom::{
    branch::alt,
    bytes::complete::{tag, tag_no_case},
    character::complete::{alpha1, alphanumeric1, char, digit1, multispace0, multispace1},
    combinator::{map, map_res, opt, peek, recognize},
    multi::{many0, separated_list0, separated_list1},
    sequence::{delimited, pair, preceded},
    IResult, Parser,
};
use std::collections::HashMap;

/// Enhanced error information for better debugging
#[derive(Debug, Clone)]
pub struct ParseError {
    pub message: String,
    pub line: usize,
    pub column: usize,
    pub context: String,
    pub expected: Vec<String>,
    pub found: Option<String>,
}

impl std::fmt::Display for ParseError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        writeln!(
            f,
            "Parse error at line {}, column {}:",
            self.line, self.column
        )?;
        writeln!(f, "  {}", self.context)?;
        writeln!(f, "  {}", " ".repeat(self.column - 1) + "^")?;
        writeln!(f, "Error: {}", self.message)?;

        if !self.expected.is_empty() {
            writeln!(f, "Expected one of: {}", self.expected.join(", "))?;
        }

        if let Some(found) = &self.found {
            writeln!(f, "Found: '{found}'")?;
        }

        Ok(())
    }
}

/// Calculate line and column from input position
fn calculate_position(input: &str, position: usize) -> (usize, usize) {
    let before_error = &input[..position];
    let line = before_error.matches('\n').count() + 1;
    let column = before_error
        .rfind('\n')
        .map_or(position + 1, |last_newline| position - last_newline);
    (line, column)
}

/// Extract context around the error position
fn extract_context(input: &str, position: usize, context_size: usize) -> String {
    let start = position.saturating_sub(context_size);
    let end = (position + context_size).min(input.len());
    let context = &input[start..end];

    // Replace newlines with spaces for single-line display
    context.replace(['\n', '\r'], " ")
}

/// Convert nom error to our enhanced error format
fn convert_nom_error(input: &str, error: nom::Err<nom::error::Error<&str>>) -> ParseError {
    match error {
        nom::Err::Error(e) | nom::Err::Failure(e) => {
            let (line, column) = calculate_position(input, input.len() - e.input.len());
            let context = extract_context(input, input.len() - e.input.len(), 20);

            // Provide more specific error messages based on error kind
            let (message, expected) = match e.code {
                nom::error::ErrorKind::Tag => (
                    "Unexpected token".to_string(),
                    vec!["keyword".to_string(), "identifier".to_string()],
                ),
                nom::error::ErrorKind::Alpha => (
                    "Expected alphabetic character".to_string(),
                    vec!["letter".to_string()],
                ),
                nom::error::ErrorKind::Digit => {
                    ("Expected digit".to_string(), vec!["number".to_string()])
                }
                nom::error::ErrorKind::AlphaNumeric => (
                    "Expected alphanumeric character".to_string(),
                    vec!["letter or digit".to_string()],
                ),
                nom::error::ErrorKind::Char => (
                    "Expected specific character".to_string(),
                    vec!["character".to_string()],
                ),
                nom::error::ErrorKind::Eof => (
                    "Unexpected end of input".to_string(),
                    vec!["more input".to_string()],
                ),
                _ => (
                    format!("Parse error: {:?}", e.code),
                    vec!["valid SQL syntax".to_string()],
                ),
            };

            ParseError {
                message,
                line,
                column,
                context,
                expected,
                found: e.input.chars().next().map(|c| c.to_string()),
            }
        }
        nom::Err::Incomplete(_) => {
            let (line, column) = calculate_position(input, input.len());
            let context = extract_context(input, input.len(), 20);

            ParseError {
                message: "Incomplete input - more data expected".to_string(),
                line,
                column,
                context,
                expected: vec!["more input".to_string()],
                found: None,
            }
        }
    }
}

/// Debug utility to help identify parser issues
pub fn debug_parse_sql(input: &str) -> Result<Statement, ParseError> {
    // First try normal parsing
    match parse_sql(input) {
        Ok(statement) => Ok(statement),
        Err(error) => {
            // Provide minimal debugging information without log spam
            eprintln!("Parser error: {error}");
            Err(error)
        }
    }
}

/// Parse SQL with detailed error reporting and suggestions
pub fn parse_sql_with_suggestions(input: &str) -> Result<Statement, ParseError> {
    match parse_sql(input) {
        Ok(statement) => Ok(statement),
        Err(mut error) => {
            // Add suggestions based on common mistakes
            let suggestions = generate_suggestions(input, &error);
            if !suggestions.is_empty() {
                error
                    .message
                    .push_str(&format!("\n\nSuggestions:\n{}", suggestions.join("\n")));
            }
            Err(error)
        }
    }
}

/// Generate helpful suggestions for common parser errors
fn generate_suggestions(input: &str, error: &ParseError) -> Vec<String> {
    let mut suggestions = Vec::new();

    // Check for common SQL syntax issues
    if error.message.contains("Unexpected token") {
        if input.to_uppercase().contains("SELCT") {
            suggestions.push("- Did you mean 'SELECT' instead of 'SELCT'?".to_string());
        }
        if input.to_uppercase().contains("FRM") {
            suggestions.push("- Did you mean 'FROM' instead of 'FRM'?".to_string());
        }
        if input.to_uppercase().contains("WERE") {
            suggestions.push("- Did you mean 'WHERE' instead of 'WERE'?".to_string());
        }
        if input.to_uppercase().contains("INSRT") {
            suggestions.push("- Did you mean 'INSERT' instead of 'INSRT'?".to_string());
        }
    }

    // Check for missing keywords
    if error.message.contains("Expected") && error.expected.contains(&"keyword".to_string()) {
        let context = &error.context;
        if context.to_uppercase().contains("SELECT") && !context.to_uppercase().contains("FROM") {
            suggestions.push("- Missing 'FROM' clause after SELECT".to_string());
        }
        if context.to_uppercase().contains("INSERT") && !context.to_uppercase().contains("INTO") {
            suggestions.push("- Missing 'INTO' clause after INSERT".to_string());
        }
        if context.to_uppercase().contains("UPDATE") && !context.to_uppercase().contains("SET") {
            suggestions.push("- Missing 'SET' clause after UPDATE".to_string());
        }
    }

    // Check for missing punctuation
    if error.message.contains("Expected specific character") {
        if error.context.contains("(") && !error.context.contains(")") {
            suggestions.push("- Missing closing parenthesis ')'".to_string());
        }
        if error.context.contains("'") && error.context.matches("'").count() % 2 == 1 {
            suggestions.push("- Missing closing single quote".to_string());
        }
        if error.context.contains("\"") && error.context.matches("\"").count() % 2 == 1 {
            suggestions.push("- Missing closing double quote".to_string());
        }
    }

    suggestions
}

fn parse_identifier_optimized(input: &str) -> IResult<&str, &str> {
    recognize(pair(alpha1, many0(alt((alphanumeric1, tag("_")))))).parse(input)
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

// Parse aggregate function: COUNT(*), SUM(column), etc.
fn parse_aggregate_function(input: &str) -> IResult<&str, Expression> {
    let (input, _) = multispace0.parse(input)?;
    let (input, func_name) = parse_identifier_optimized.parse(input)?;
    let (input, _) = multispace0.parse(input)?;
    let (input, _) = char('(').parse(input)?;
    let (input, _) = multispace0.parse(input)?;

    // Only treat these as aggregate functions
    let is_aggregate = matches!(
        func_name.to_uppercase().as_str(),
        "COUNT" | "SUM" | "AVG" | "MAX" | "MIN"
    );

    if !is_aggregate {
        // Not an aggregate function, let the regular function parser handle it
        return Err(nom::Err::Error(nom::error::Error::new(
            input,
            nom::error::ErrorKind::Tag,
        )));
    }

    // Handle COUNT(*) specially
    if func_name.to_uppercase() == "COUNT" {
        if let Ok((input, _)) = tag::<&str, &str, nom::error::Error<&str>>("*").parse(input) {
            let (input, _) = multispace0.parse(input)?;
            let (input, _) = char(')').parse(input)?;
            return Ok((
                input,
                Expression::AggregateFunction {
                    name: func_name.to_string(),
                    arg: Box::new(Expression::Column("*".to_string())),
                },
            ));
        }
    }

    // Parse the argument as an expression
    let (input, arg) = parse_expression.parse(input)?;
    let (input, _) = multispace0.parse(input)?;
    let (input, _) = char(')').parse(input)?;

    Ok((
        input,
        Expression::AggregateFunction {
            name: func_name.to_string(),
            arg: Box::new(arg),
        },
    ))
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
    pub index_type: Option<IndexType>,
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub enum IndexType {
    BTree, // Default for regular indexes
    HNSW,  // Hierarchical Navigable Small World for vector similarity
    IVF,   // Inverted File Index for vector clustering
    LSH,   // Locality Sensitive Hashing for vector search
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
        left: Expression,
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

#[derive(Debug, Clone, PartialEq, Eq, Hash, Copy)]
pub enum ArithmeticOperator {
    Add,
    Subtract,
    Multiply,
    Divide,
    Modulo,
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
    AggregateFunction {
        name: String,
        arg: Box<Expression>,
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
                match name.to_uppercase().as_str() {
                    "COSINE_SIMILARITY" => {
                        if args.len() != 2 {
                            return Err(
                                "COSINE_SIMILARITY requires exactly 2 arguments".to_string()
                            );
                        }
                        let vec1 = args[0].evaluate(context)?;
                        let vec2 = args[1].evaluate(context)?;

                        match (vec1, vec2) {
                            (SqlValue::Vector(v1), SqlValue::Vector(v2)) => {
                                if v1.len() != v2.len() {
                                    return Err("Vectors must have the same dimension for cosine similarity".to_string());
                                }

                                // Calculate dot product
                                let dot_product: f64 =
                                    v1.iter().zip(v2.iter()).map(|(a, b)| a * b).sum();

                                // Calculate magnitudes
                                let mag1: f64 = v1.iter().map(|x| x * x).sum::<f64>().sqrt();
                                let mag2: f64 = v2.iter().map(|x| x * x).sum::<f64>().sqrt();

                                if mag1 == 0.0 || mag2 == 0.0 {
                                    return Err(
                                        "Cannot calculate cosine similarity with zero vectors"
                                            .to_string(),
                                    );
                                }

                                let similarity = dot_product / (mag1 * mag2);
                                Ok(SqlValue::Real(similarity))
                            }
                            _ => Err("COSINE_SIMILARITY requires vector arguments".to_string()),
                        }
                    }
                    "EUCLIDEAN_DISTANCE" => {
                        if args.len() != 2 {
                            return Err(
                                "EUCLIDEAN_DISTANCE requires exactly 2 arguments".to_string()
                            );
                        }
                        let vec1 = args[0].evaluate(context)?;
                        let vec2 = args[1].evaluate(context)?;

                        match (vec1, vec2) {
                            (SqlValue::Vector(v1), SqlValue::Vector(v2)) => {
                                if v1.len() != v2.len() {
                                    return Err("Vectors must have the same dimension for euclidean distance".to_string());
                                }

                                let distance: f64 = v1
                                    .iter()
                                    .zip(v2.iter())
                                    .map(|(a, b)| (a - b).powi(2))
                                    .sum::<f64>()
                                    .sqrt();

                                Ok(SqlValue::Real(distance))
                            }
                            _ => Err("EUCLIDEAN_DISTANCE requires vector arguments".to_string()),
                        }
                    }
                    "DOT_PRODUCT" => {
                        if args.len() != 2 {
                            return Err("DOT_PRODUCT requires exactly 2 arguments".to_string());
                        }
                        let vec1 = args[0].evaluate(context)?;
                        let vec2 = args[1].evaluate(context)?;

                        match (vec1, vec2) {
                            (SqlValue::Vector(v1), SqlValue::Vector(v2)) => {
                                if v1.len() != v2.len() {
                                    return Err(
                                        "Vectors must have the same dimension for dot product"
                                            .to_string(),
                                    );
                                }

                                let dot_product: f64 =
                                    v1.iter().zip(v2.iter()).map(|(a, b)| a * b).sum();
                                Ok(SqlValue::Real(dot_product))
                            }
                            _ => Err("DOT_PRODUCT requires vector arguments".to_string()),
                        }
                    }
                    "L2_NORMALIZE" => {
                        if args.len() != 1 {
                            return Err("L2_NORMALIZE requires exactly 1 argument".to_string());
                        }
                        let vec = args[0].evaluate(context)?;

                        match vec {
                            SqlValue::Vector(v) => {
                                let magnitude: f64 = v.iter().map(|x| x * x).sum::<f64>().sqrt();

                                if magnitude == 0.0 {
                                    return Err("Cannot normalize zero vector".to_string());
                                }

                                let normalized: Vec<f64> =
                                    v.iter().map(|x| x / magnitude).collect();
                                Ok(SqlValue::Vector(normalized))
                            }
                            _ => Err("L2_NORMALIZE requires vector argument".to_string()),
                        }
                    }
                    "EMBED" => {
                        // EMBED(text) or EMBED(text, model)
                        if args.is_empty() || args.len() > 2 {
                            return Err("EMBED requires 1 or 2 arguments: EMBED(text) or EMBED(text, model)".to_string());
                        }

                        let text = args[0].evaluate(context)?;
                        let model_name = if args.len() == 2 {
                            match args[1].evaluate(context)? {
                                SqlValue::Text(s) => s,
                                _ => return Err("EMBED model argument must be text".to_string()),
                            }
                        } else {
                            "simple".to_string() // Default model
                        };

                        match text {
                            SqlValue::Text(t) => {
                                // Parse model
                                let model = model_name
                                    .parse::<crate::embedding::EmbeddingModel>()
                                    .map_err(|e| format!("EMBED model error: {e}"))?;

                                // Generate embedding
                                let embedding = crate::embedding::embed(&t, model)
                                    .map_err(|e| format!("EMBED error: {e}"))?;

                                Ok(SqlValue::Vector(embedding))
                            }
                            _ => Err("EMBED requires text argument".to_string()),
                        }
                    }
                    "ABS" => {
                        if args.len() != 1 {
                            return Err("ABS requires exactly 1 argument".to_string());
                        }
                        let value = args[0].evaluate(context)?;

                        match value {
                            SqlValue::Integer(i) => Ok(SqlValue::Integer(i.abs())),
                            SqlValue::Real(f) => Ok(SqlValue::Real(f.abs())),
                            _ => Err("ABS requires numeric argument".to_string()),
                        }
                    }
                    _ => Err(format!("Function '{name}' evaluation not implemented")),
                }
            }
            Expression::AggregateFunction { name, arg } => {
                // For now, we'll evaluate the argument but not perform aggregation
                // This will be handled by the query processor during execution
                let _arg_value = arg.evaluate(context)?;
                match name.to_uppercase().as_str() {
                    "COUNT" => Ok(SqlValue::Integer(1)), // Placeholder
                    "SUM" => Ok(SqlValue::Integer(0)),   // Placeholder
                    "AVG" => Ok(SqlValue::Real(0.0)),    // Placeholder
                    "MAX" => Ok(SqlValue::Integer(0)),   // Placeholder
                    "MIN" => Ok(SqlValue::Integer(0)),   // Placeholder
                    _ => Err(format!("Aggregate function '{name}' not implemented")),
                }
            }
        }
    }
}

/// Parse SQL and assign unique parameter indices
pub fn parse_sql(input: &str) -> Result<Statement, ParseError> {
    // Pre-normalize input to be robust to REPL/control inputs
    let mut normalized = input.replace("\r\n", "\n").replace('\r', "\n");
    if let Some(stripped) = normalized.strip_prefix('\u{FEFF}') {
        normalized = stripped.to_string();
    }
    // Strip leading control chars (except space/tab/newline) and stray leading semicolons
    let normalized = normalized
        .trim_start_matches(|c: char| {
            let cu = c as u32;
            (cu < 0x20 && c != ' ' && c != '\t' && c != '\n') || cu == 0x7F
        })
        .trim_start_matches(';')
        .to_string();

    let (remaining, statement) = parse_statement
        .parse(&normalized)
        .map_err(|e| convert_nom_error(input, e))?;

    // Allow trailing whitespace and optional semicolon(s)
    let remaining = remaining.trim_start();
    let remaining = remaining.strip_prefix(';').unwrap_or(remaining);
    if !remaining.trim().is_empty() {
        let (line, column) = calculate_position(input, input.len() - remaining.len());
        let context = extract_context(input, input.len() - remaining.len(), 20);

        return Err(ParseError {
            message: "Unexpected input after statement".to_string(),
            line,
            column,
            context,
            expected: vec!["end of statement".to_string(), "semicolon".to_string()],
            found: Some(remaining.chars().take(10).collect()),
        });
    }

    Ok(statement)
}

/// Parse a single SQL statement
fn parse_statement(input: &str) -> IResult<&str, Statement> {
    let (input, _) = multispace0.parse(input)?;

    // Guard each branch with a non-consuming peek on the leading keyword(s)
    alt((
        preceded(peek(tag_no_case("CREATE")), parse_create_table),
        preceded(peek(tag_no_case("INSERT")), parse_insert),
        preceded(peek(tag_no_case("SELECT")), parse_select),
        preceded(peek(tag_no_case("UPDATE")), parse_update),
        preceded(peek(tag_no_case("DELETE")), parse_delete),
        preceded(
            peek(pair(tag_no_case("DROP"), multispace1).and(tag_no_case("TABLE"))),
            parse_drop_table,
        ),
        preceded(peek(tag_no_case("CREATE")), parse_create_index),
        preceded(
            peek(pair(tag_no_case("DROP"), multispace1).and(tag_no_case("INDEX"))),
            parse_drop_index,
        ),
        preceded(
            peek(alt((tag_no_case("BEGIN"), tag_no_case("START")))),
            parse_begin_transaction,
        ),
        preceded(peek(tag_no_case("COMMIT")), parse_commit),
        preceded(peek(tag_no_case("ROLLBACK")), parse_rollback),
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

// Parse CREATE TABLE statement with improved multi-line support
fn parse_create_table(input: &str) -> IResult<&str, Statement> {
    let (input, _) = delimited(multispace0, tag_no_case("CREATE"), multispace1).parse(input)?;
    let (input, _) = tag_no_case("TABLE").parse(input)?;
    let (input, _) = multispace1.parse(input)?;
    let (input, table) = parse_identifier_optimized.parse(input)?;
    let (input, _) = multispace0.parse(input)?;

    // Parse column definitions with better multi-line support
    let (input, _) = char('(').parse(input)?;
    let (input, columns) = separated_list0(
        delimited(multispace0, char(','), multispace0),
        delimited(multispace0, parse_column_definition, multispace0),
    )
    .parse(input)?;
    let (input, _) = delimited(multispace0, char(')'), multispace0).parse(input)?;

    Ok((
        input,
        Statement::CreateTable(CreateTableStatement {
            table: table.to_string(),
            columns,
        }),
    ))
}

// Parse INSERT statement with improved multi-line support
fn parse_insert(input: &str) -> IResult<&str, Statement> {
    let (input, _) = delimited(multispace0, tag_no_case("INSERT"), multispace1).parse(input)?;
    let (input, _) = tag_no_case("INTO").parse(input)?;
    let (input, _) = multispace1.parse(input)?;
    let (input, table) = parse_identifier_optimized.parse(input)?;
    let (input, _) = multispace0.parse(input)?;

    // Parse optional column list
    let (input, columns) =
        if let Ok((input, _)) = char::<&str, nom::error::Error<&str>>('(').parse(input) {
            let (input, columns_expr) = separated_list0(
                delimited(multispace0, char(','), multispace0),
                delimited(multispace0, parse_identifier_optimized, multispace0),
            )
            .parse(input)?;

            let (input, _) = char(')').parse(input)?;
            let (input, _) = multispace0.parse(input)?;

            let columns: Vec<String> = columns_expr.into_iter().map(|s| s.to_string()).collect();
            (input, columns)
        } else {
            (input, Vec::new()) // No column list specified
        };

    // Parse VALUES keyword with better whitespace handling
    let (input, _) = tag_no_case("VALUES").parse(input)?;
    let (input, _) = multispace0.parse(input)?;

    // Parse one or more value tuples with improved multi-line support
    let (input, values) = separated_list1(
        delimited(multispace0, char(','), multispace0),
        delimited(
            delimited(multispace0, char('('), multispace0),
            separated_list0(
                delimited(multispace0, char(','), multispace0),
                parse_sql_value,
            ),
            delimited(multispace0, char(')'), multispace0),
        ),
    )
    .parse(input)?;

    Ok((
        input,
        Statement::Insert(InsertStatement {
            table: table.to_string(),
            columns,
            values,
        }),
    ))
}

// Parse SELECT statement with improved multi-line support
fn parse_select(input: &str) -> IResult<&str, Statement> {
    let (input, _) = delimited(multispace0, tag_no_case("SELECT"), multispace1).parse(input)?;
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
            table: table.to_string(),
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
            table: table.to_string(),
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
            table: table.to_string(),
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
            table: table.to_string(),
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
    let (input, index_type_opt) = opt(preceded(
        delimited(multispace0, tag_no_case("USING"), multispace1),
        map_res(parse_identifier_optimized, |ty: &str| {
            match ty.to_uppercase().as_str() {
                "BTREE" => Ok(IndexType::BTree),
                "HNSW" => Ok(IndexType::HNSW),
                "IVF" => Ok(IndexType::IVF),
                "LSH" => Ok(IndexType::LSH),
                _ => Err("Unsupported index type"),
            }
        }),
    ))
    .parse(input)?;
    let (input, _) = multispace0.parse(input)?;

    Ok((
        input,
        Statement::CreateIndex(CreateIndexStatement {
            index_name: index_name.to_string(),
            table_name: table_name.to_string(),
            column_name: column_name.to_string(),
            unique: unique.is_some(),
            index_type: index_type_opt,
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
            index_name: index_name.to_string(),
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

    Ok((
        input,
        Condition::Comparison {
            left: left_expr,
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

    Ok((
        input,
        OrderByItem {
            expression,
            direction,
        },
    ))
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

    Ok((
        input,
        Assignment {
            column: column.to_string(),
            value,
        },
    ))
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
            name: name.to_string(),
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
        // TEXT requires fixed length: TEXT(10)
        map(
            pair(tag_no_case("TEXT"), parse_length_specification),
            |(_, length)| DataType::Text(Some(length)),
        ),
        // VECTOR requires fixed dimension: VECTOR(384)
        map(
            pair(tag_no_case("VECTOR"), parse_length_specification),
            |(_, dimension)| DataType::Vector(Some(dimension)),
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

// Parse string literal with escape sequence support
fn parse_string_literal(input: &str) -> IResult<&str, String> {
    let (input, _) = char('\'').parse(input)?;
    let mut result = String::new();
    let mut chars = input.chars();

    while let Some(ch) = chars.next() {
        match ch {
            '\'' => {
                // End of string
                let remaining = chars.as_str();
                return Ok((remaining, result));
            }
            '\\' => {
                // Escape sequence
                if let Some(next_ch) = chars.next() {
                    match next_ch {
                        'n' => result.push('\n'),
                        't' => result.push('\t'),
                        'r' => result.push('\r'),
                        '\\' => result.push('\\'),
                        '\'' => result.push('\''),
                        _ => {
                            // Unknown escape sequence, treat as literal
                            result.push('\\');
                            result.push(next_ch);
                        }
                    }
                } else {
                    // Backslash at end of string
                    result.push('\\');
                    break;
                }
            }
            _ => result.push(ch),
        }
    }

    // If we get here, the string wasn't properly closed
    Err(nom::Err::Error(nom::error::Error::new(
        input,
        nom::error::ErrorKind::Tag,
    )))
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
    let (input, values) =
        separated_list0(delimited(multispace0, char(','), multispace0), parse_real).parse(input)?;
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
        // Aggregate function: COUNT(*), SUM(column), etc.
        parse_aggregate_function,
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
            |(name, args)| Expression::FunctionCall {
                name: name.to_string(),
                args,
            },
        ),
        // Parenthesized expressions
        delimited(
            char('('),
            delimited(multispace0, parse_expression, multispace0),
            char(')'),
        ),
        // Column references
        map(parse_identifier_optimized, |name| {
            Expression::Column(name.to_string())
        }),
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
