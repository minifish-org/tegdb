//! High-level database interface
//!
//! This module provides a SQLite-like interface for TegDB, making it easy for users
//! to interact with the database without dealing with low-level engine details.

use crate::catalog::Catalog;
use crate::parser::Expression;
use crate::parser::{parse_sql, SqlValue, Statement};
use crate::planner::QueryPlanner;
use crate::query_processor::{QueryProcessor, QuerySchema, TableSchema};
use crate::storage_engine::StorageEngine;
use crate::Result;
use std::collections::HashMap;
use std::rc::Rc;

/// Prepared statement for parameterized queries
pub struct PreparedStatement {
    /// The original SQL with placeholders
    sql: String,
    /// The parsed statement with parameter placeholders
    statement: Statement,
    /// Number of parameters expected
    parameter_count: usize,
    query_schema: Option<QuerySchema>,
    /// Optional cached plan template for SELECT PK lookup
    plan_template: Option<crate::planner::ExecutionPlan>,
}

impl PreparedStatement {
    /// Create a new prepared statement
    fn new(
        sql: String,
        statement: Statement,
        query_schema: Option<QuerySchema>,
        plan_template: Option<crate::planner::ExecutionPlan>,
    ) -> Self {
        let parameter_count = Self::count_parameters(&statement);
        Self {
            sql,
            statement,
            parameter_count,
            query_schema,
            plan_template,
        }
    }

    /// Count the number of parameters in a statement
    fn count_parameters(statement: &Statement) -> usize {
        use crate::parser::Statement;
        match statement {
            Statement::Select(select) => Self::count_parameters_in_condition(&select.where_clause),
            Statement::Insert(insert) => insert
                .values
                .iter()
                .map(|row| {
                    row.iter()
                        .filter(|expr| expr_has_param(*expr))
                        .count()
                })
                .sum::<usize>(),
            Statement::Update(update) => {
                let assignment_params = update
                    .assignments
                    .iter()
                    .map(|a| Self::count_parameters_in_expression(&a.value))
                    .sum::<usize>();
                let where_params = Self::count_parameters_in_condition(&update.where_clause);
                assignment_params + where_params
            }
            Statement::Delete(delete) => Self::count_parameters_in_condition(&delete.where_clause),
            _ => 0, // DDL statements don't have parameters
        }
    }

    /// Count parameters in a WHERE condition
    fn count_parameters_in_condition(where_clause: &Option<crate::parser::WhereClause>) -> usize {
        if let Some(where_clause) = where_clause {
            Self::count_parameters_in_condition_recursive(&where_clause.condition)
        } else {
            0
        }
    }

    /// Recursively count parameters in a condition
    fn count_parameters_in_condition_recursive(condition: &crate::parser::Condition) -> usize {
        use crate::parser::Condition;
        match condition {
            Condition::Comparison { right, .. } => {
                if matches!(right, SqlValue::Parameter(_)) {
                    1
                } else {
                    0
                }
            }
            Condition::Between { low, high, .. } => {
                let mut count = 0;
                if matches!(low, SqlValue::Parameter(_)) {
                    count += 1;
                }
                if matches!(high, SqlValue::Parameter(_)) {
                    count += 1;
                }
                count
            }
            Condition::And(left, right) => {
                Self::count_parameters_in_condition_recursive(left)
                    + Self::count_parameters_in_condition_recursive(right)
            }
            Condition::Or(left, right) => {
                Self::count_parameters_in_condition_recursive(left)
                    + Self::count_parameters_in_condition_recursive(right)
            }
        }
    }

    /// Count parameters in an expression
    fn count_parameters_in_expression(expression: &crate::parser::Expression) -> usize {
        use crate::parser::Expression;
        match expression {
            Expression::Value(SqlValue::Parameter(_)) => 1,
            Expression::Value(_) => 0,
            Expression::Column(_) => 0,
            Expression::BinaryOp { left, right, .. } => {
                Self::count_parameters_in_expression(left)
                    + Self::count_parameters_in_expression(right)
            }
            Expression::FunctionCall { args, .. } => {
                args.iter().map(Self::count_parameters_in_expression).sum()
            }
            Expression::AggregateFunction { arg, .. } => Self::count_parameters_in_expression(arg),
        }
    }

    /// Get the number of parameters this statement expects
    pub fn parameter_count(&self) -> usize {
        self.parameter_count
    }

    /// Get the original SQL
    pub fn sql(&self) -> &str {
        &self.sql
    }
}
/// Normalize SQL input before parsing.
///
/// Goals:
/// - Handle mixed/newline styles (CRLF/CR) by converting to `\n`
/// - Strip UTF-8 BOM and leading non-printable control characters
/// - Preserve internal whitespace and content
fn normalize_sql_input(sql: &str) -> String {
    // Normalize newlines first
    let mut out = sql.replace("\r\n", "\n").replace('\r', "\n");

    // Trim UTF-8 BOM if present
    if let Some(stripped) = out.strip_prefix('\u{FEFF}') {
        out = stripped.to_string();
    }

    // Strip leading control characters except space/tab/newline
    let trimmed = out
        .trim_start_matches(|c: char| {
            let cu = c as u32;
            (cu < 0x20 && c != ' ' && c != '\t' && c != '\n') || cu == 0x7F
        })
        .to_string();

    trimmed
}

/// Database connection, similar to sqlite::Connection
///
/// This struct maintains a schema catalog at the database level to avoid
/// repeated schema loading from disk for every query processor creation.
/// Schemas are loaded once when the database is opened and kept in sync
/// with DDL operations (CREATE TABLE, DROP TABLE).
/// Optimized for single-threaded usage without locks.
pub struct Database {
    storage: StorageEngine,
    /// Schema catalog for managing table metadata (no locks needed for single-threaded)
    catalog: Catalog,
}

impl Database {
    /// Create or open a database on the local filesystem.
    ///
    /// Accepts only absolute paths with the `file://` protocol.
    ///
    /// Examples:
    /// - ✅ file:///absolute/path/to/db
    /// - ❌ relative/path (missing protocol)
    /// - ❌ file://relative/path (relative path with protocol)
    pub fn open<P: AsRef<str>>(path: P) -> Result<Self> {
        let path_str = path.as_ref();
        let (protocol, path_part) = crate::protocol_utils::parse_storage_identifier(path_str);

        if protocol != crate::protocol_utils::PROTOCOL_NAME_FILE {
            return Err(crate::Error::Other(format!(
                "Unsupported protocol: {protocol}. Only 'file://' is supported."
            )));
        }

        let path_buf = std::path::Path::new(path_part);
        if !path_buf.is_absolute() {
            return Err(crate::Error::Other(format!(
                "Path must be absolute. Got: '{path_str}'. Use absolute path like 'file:///absolute/path/to/db'"
            )));
        }

        let storage = StorageEngine::new(path_buf.to_path_buf())?;

        let catalog = Catalog::load_from_storage(&storage)?;

        Ok(Self { storage, catalog })
    }

    /// Helper function to create TableSchema from CreateTableStatement
    /// Centralizes schema creation logic to avoid duplication
    fn create_table_schema(create_table: &crate::parser::CreateTableStatement) -> TableSchema {
        Catalog::create_table_schema(create_table)
    }

    /// Helper function to get schemas in Rc format (no conversion needed)
    fn get_schemas_rc(
        schemas: &HashMap<String, Rc<TableSchema>>,
    ) -> HashMap<String, Rc<TableSchema>> {
        schemas
            .iter()
            .map(|(k, v)| (k.clone(), Rc::clone(v)))
            .collect()
    }

    /// Helper function to update schema catalog for DDL operations
    /// Centralizes schema catalog update logic to avoid duplication
    fn update_schema_catalog_for_ddl(catalog: &mut Catalog, statement: &crate::parser::Statement) {
        match statement {
            crate::parser::Statement::CreateTable(create_table) => {
                let schema = Self::create_table_schema(create_table);
                catalog.add_table_schema(schema);
            }
            crate::parser::Statement::DropTable(drop_table) => {
                catalog.remove_table_schema(&drop_table.table);
            }
            crate::parser::Statement::CreateIndex(create_index) => {
                let index = crate::catalog::IndexInfo {
                    name: create_index.index_name.clone(),
                    table_name: create_index.table_name.clone(),
                    column_name: create_index.column_name.clone(),
                    unique: create_index.unique,
                    index_type: create_index
                        .index_type
                        .unwrap_or(crate::parser::IndexType::BTree),
                };
                let _ = catalog.add_index(index);
            }
            crate::parser::Statement::DropIndex(drop_index) => {
                let _ = catalog.remove_index(&drop_index.index_name);
            }
            _ => {} // No schema changes for other statements
        }
    }

    /// Centralized query execution helper for mutable reference
    /// Executes SELECT statements and returns QueryResult
    fn execute_query_with_processor_ref(
        processor: &mut QueryProcessor<'_>,
        sql: &str,
        schemas: &HashMap<String, Rc<TableSchema>>,
    ) -> Result<QueryResult> {
        // Get schemas in Rc format for the planner
        let rc_schemas = Self::get_schemas_rc(schemas);
        Self::execute_query_core(processor, sql, &rc_schemas)
    }

    /// Core query execution logic - the actual implementation
    /// Executes SELECT statements and returns QueryResult
    fn execute_query_core(
        processor: &mut QueryProcessor<'_>,
        sql: &str,
        schemas: &HashMap<String, Rc<TableSchema>>,
    ) -> Result<QueryResult> {
        let normalized = normalize_sql_input(sql);
        let statement =
            parse_sql(&normalized).map_err(|e| crate::Error::ParseError(e.to_string()))?;

        // Only SELECT statements make sense for queries
        match &statement {
            crate::parser::Statement::Select(_) => {
                // Use the planner to generate an optimized execution plan
                let planner = QueryPlanner::new(schemas.clone());
                let plan = planner.plan(statement)?;

                // Execute and immediately collect results
                let result = processor.execute_plan(plan)?;
                match result {
                    crate::query_processor::ResultSet::Select { columns, rows } => {
                        // Collect all rows from the iterator efficiently
                        // The iterator yields rows one by one, avoiding large memmove operations
                        let collected_rows: Result<Vec<Vec<crate::parser::SqlValue>>> =
                            rows.collect();
                        let final_rows = collected_rows?;
                        Ok(QueryResult {
                            columns,
                            rows: final_rows,
                        })
                    }
                    _ => Err(crate::Error::Other(
                        "Expected SELECT result but got something else".to_string(),
                    )),
                }
            }
            _ => {
                // For non-SELECT statements, this doesn't make sense
                Err(crate::Error::Other(
                    "query() should only be used for SELECT statements".to_string(),
                ))
            }
        }
    }

    /// Helper: extract rows_affected from non-SELECT results
    fn extract_rows_affected(result: &crate::query_processor::ResultSet<'_>) -> Result<usize> {
        match result {
            crate::query_processor::ResultSet::Insert { rows_affected }
            | crate::query_processor::ResultSet::Update { rows_affected }
            | crate::query_processor::ResultSet::Delete { rows_affected } => Ok(*rows_affected),
            crate::query_processor::ResultSet::CreateTable
            | crate::query_processor::ResultSet::DropTable
            | crate::query_processor::ResultSet::CreateIndex
            | crate::query_processor::ResultSet::DropIndex
            | crate::query_processor::ResultSet::Begin
            | crate::query_processor::ResultSet::Commit
            | crate::query_processor::ResultSet::Rollback => Ok(0),
            crate::query_processor::ResultSet::Select { .. } => Err(crate::Error::Other(
                "execute()/execute_prepared should not be used for SELECT statements. Use query()/query_prepared instead.".to_string(),
            )),
        }
    }

    /// 核心执行函数：接收一个执行计划，处理事务、执行和 schema 更新。
    /// 封装了 execute 和 execute_prepared 的公共逻辑。
    fn _execute_plan(
        &mut self,
        plan: crate::planner::ExecutionPlan,
        statement: &Statement,
    ) -> Result<usize> {
        let transaction = self.storage.begin_transaction();
        let schemas = Self::get_schemas_rc(self.catalog.get_all_schemas());
        let mut processor = QueryProcessor::new_with_rc_schemas(transaction, schemas);

        // 执行计划
        let result = processor.execute_plan(plan)?;
        let final_result = Self::extract_rows_affected(&result)?;

        // 释放对 result 的借用
        drop(result);

        // 如果是 DDL 操作，更新 catalog
        Self::update_schema_catalog_for_ddl(&mut self.catalog, statement);

        // 提交事务
        processor.transaction_mut().commit()?;

        Ok(final_result)
    }

    /// 核心查询函数：接收一个执行计划，处理事务、执行并返回最终结果。
    /// 封装了 query 和 query_prepared 的公共逻辑。
    fn _query_plan(
        &mut self,
        plan: crate::planner::ExecutionPlan,
        query_schema: Option<&QuerySchema>,
    ) -> Result<QueryResult> {
        let transaction = self.storage.begin_transaction();
        let schemas = Self::get_schemas_rc(self.catalog.get_all_schemas());
        let mut processor = QueryProcessor::new_with_rc_schemas(transaction, schemas);

        // 执行计划
        let result = match query_schema {
            Some(schema) => processor.execute_plan_with_query_schema(plan, schema)?,
            None => processor.execute_plan(plan)?,
        };

        match result {
            crate::query_processor::ResultSet::Select { columns, rows } => {
                // 将流式结果收集起来
                let collected_rows: Result<Vec<Vec<crate::parser::SqlValue>>> = rows.collect();
                Ok(QueryResult {
                    columns,
                    rows: collected_rows?,
                })
            }
            _ => Err(crate::Error::Other(
                "Expected SELECT result but got something else".to_string(),
            )),
        }
    }

    /// Execute SQL statement, return number of affected rows
    pub fn execute(&mut self, sql: &str) -> Result<usize> {
        let normalized = normalize_sql_input(sql);
        #[cfg(feature = "dev")]
        {
            // Debug mode enabled but no longer printing debug info to avoid log spam
        }
        let statement =
            parse_sql(&normalized).map_err(|e| crate::Error::ParseError(e.to_string()))?;

        let schemas = Self::get_schemas_rc(self.catalog.get_all_schemas());
        let planner = QueryPlanner::new(schemas);
        let plan = planner.plan(statement.clone())?;

        // 调用核心执行函数
        self._execute_plan(plan, &statement)
    }

    /// Execute SQL query, return all results materialized in memory
    /// This follows the parse -> plan -> execute_plan pipeline but returns simple QueryResult
    pub fn query(&mut self, sql: &str) -> Result<QueryResult> {
        let normalized = normalize_sql_input(sql);
        let statement =
            parse_sql(&normalized).map_err(|e| crate::Error::ParseError(e.to_string()))?;

        let schemas = Self::get_schemas_rc(self.catalog.get_all_schemas());
        let planner = QueryPlanner::new(schemas);
        let plan = planner.plan(statement)?;

        // 调用核心查询函数
        self._query_plan(plan, None)
    }

    /// Begin a new database transaction
    pub fn begin_transaction(&mut self) -> Result<DatabaseTransaction<'_>> {
        let schemas = Self::get_schemas_rc(self.catalog.get_all_schemas());
        let transaction = self.storage.begin_transaction();
        let processor = QueryProcessor::new_with_rc_schemas(transaction, schemas);

        Ok(DatabaseTransaction {
            processor,
            catalog: &mut self.catalog,
        })
    }

    /// Get a reference to all cached table schemas (no cloning)
    /// Use this when you only need to read schema information
    pub fn get_table_schemas_ref(&self) -> &HashMap<String, Rc<TableSchema>> {
        self.catalog.get_all_schemas()
    }

    /// Get a copy of all cached table schemas
    /// Useful for debugging or introspection
    /// Note: This clones the entire schema HashMap - use sparingly
    pub fn get_table_schemas(&self) -> HashMap<String, TableSchema> {
        self.catalog
            .get_all_schemas()
            .iter()
            .map(|(k, v)| (k.clone(), (**v).clone()))
            .collect()
    }

    /// Prepare a SQL statement for execution
    /// This parses the SQL and creates a prepared statement that can be executed with parameters
    pub fn prepare(&self, sql: &str) -> Result<PreparedStatement> {
        let statement = parse_sql(sql).map_err(|e| crate::Error::ParseError(e.to_string()))?;
        let query_schema = if let Statement::Select(ref select) = statement {
            let schemas = Self::get_schemas_rc(self.catalog.get_all_schemas());
            let columns: Vec<String> = select
                .columns
                .iter()
                .enumerate()
                .map(|(i, expr)| {
                    if let Expression::Column(ref name) = expr {
                        Ok(name.clone())
                    } else {
                        // For function calls and other expressions, use a placeholder name
                        Ok(format!("expr_{i}"))
                    }
                })
                .collect::<Result<Vec<_>>>()?;
            let schema = schemas
                .get(&select.table)
                .ok_or_else(|| crate::Error::TableNotFound(select.table.clone()))?;
            Some(QuerySchema::new(&columns, schema))
        } else {
            None
        };
        // Attempt to cache a plan template for all statement types with parameter placeholders
        let plan_template = {
            let schemas = Self::get_schemas_rc(self.catalog.get_all_schemas());
            let planner = QueryPlanner::new(schemas);
            match planner.plan(statement.clone()) {
                Ok(plan) if plan_has_param(&plan) => Some(plan),
                _ => None,
            }
        };
        Ok(PreparedStatement::new(
            sql.to_string(),
            statement,
            query_schema,
            plan_template,
        ))
    }

    /// Execute a prepared statement with parameters
    /// This is similar to SQLite's prepared statement execution
    pub fn execute_prepared(
        &mut self,
        stmt: &PreparedStatement,
        params: &[SqlValue],
    ) -> Result<usize> {
        if params.len() != stmt.parameter_count() {
            let expected = stmt.parameter_count();
            let received = params.len();
            return Err(crate::Error::Other(format!(
                "Expected {expected} parameters, got {received}"
            )));
        }

        // Use plan template if available and valid
        if let Some(ref plan_template) = stmt.plan_template {
            let instantiated_plan = instantiate_plan_with_params(plan_template, params);
            // 调用核心执行函数
            self._execute_plan(instantiated_plan, &stmt.statement)
        } else {
            // Fallback: bind parameters and plan as before
            let bound_stmt = Self::bind_parameters_to_statement(&stmt.statement, params)?;
            let schemas = Self::get_schemas_rc(self.catalog.get_all_schemas());
            let planner = QueryPlanner::new(schemas);
            let plan = planner.plan(bound_stmt.clone())?;

            // 调用核心执行函数
            self._execute_plan(plan, &bound_stmt)
        }
    }

    /// Execute a prepared statement with simple Rust types - no SqlValue required!
    pub fn execute_prepared_simple<T>(
        &mut self,
        stmt: &PreparedStatement,
        params: &[T],
    ) -> Result<usize>
    where
        T: Into<SqlValue> + Clone,
    {
        let sql_values: Vec<SqlValue> = params.iter().map(|p| p.to_owned().into()).collect();
        self.execute_prepared(stmt, &sql_values)
    }

    /// Ultra-clean API: Execute 4 parameters with mixed types
    pub fn execute_prepared_4<A, B, C, D>(
        &mut self,
        stmt: &PreparedStatement,
        a: A, b: B, c: C, d: D,
    ) -> Result<usize>
    where
        A: Into<SqlValue>, B: Into<SqlValue>, C: Into<SqlValue>, D: Into<SqlValue>,
    {
        let sql_values = vec![a.into(), b.into(), c.into(), d.into()];
        self.execute_prepared(stmt, &sql_values)
    }

    /// Ultra-clean API: Execute 5 parameters with mixed types
    pub fn execute_prepared_5<A, B, C, D, E>(
        &mut self,
        stmt: &PreparedStatement,
        a: A, b: B, c: C, d: D, e: E,
    ) -> Result<usize>
    where
        A: Into<SqlValue>, B: Into<SqlValue>, C: Into<SqlValue>, D: Into<SqlValue>, E: Into<SqlValue>,
    {
        let sql_values = vec![a.into(), b.into(), c.into(), d.into(), e.into()];
        self.execute_prepared(stmt, &sql_values)
    }

    /// Execute a prepared SELECT statement with simple Rust types - no SqlValue required!
    pub fn query_prepared_simple<T>(
        &mut self,
        stmt: &PreparedStatement,
        params: &[T],
    ) -> Result<QueryResult>
    where
        T: Into<SqlValue> + Clone,
    {
        let sql_values: Vec<SqlValue> = params.iter().map(|p| p.to_owned().into()).collect();
        self.query_prepared(stmt, &sql_values)
    }

    /// Execute a prepared SELECT statement with parameters
    /// This is similar to SQLite's prepared statement query execution
    pub fn query_prepared(
        &mut self,
        stmt: &PreparedStatement,
        params: &[SqlValue],
    ) -> Result<QueryResult> {
        if params.len() != stmt.parameter_count() {
            let expected = stmt.parameter_count();
            let received = params.len();
            return Err(crate::Error::Other(format!(
                "Expected {expected} parameters, got {received}"
            )));
        }

        // Use plan template if available and valid
        if let Some(ref plan_template) = stmt.plan_template {
            let instantiated_plan = instantiate_plan_with_params(plan_template, params);
            // 调用核心查询函数
            self._query_plan(instantiated_plan, stmt.query_schema.as_ref())
        } else {
            // Fallback: bind parameters and plan as before
            let bound_stmt = Self::bind_parameters_to_statement(&stmt.statement, params)?;
            let schemas = Self::get_schemas_rc(self.catalog.get_all_schemas());
            let planner = QueryPlanner::new(schemas);
            let plan = planner.plan(bound_stmt)?;

            // 调用核心查询函数
            self._query_plan(plan, stmt.query_schema.as_ref())
        }
    }

    /// Helper: Bind parameters into a Statement AST (recursively)
    fn bind_parameters_to_statement(
        statement: &Statement,
        params: &[SqlValue],
    ) -> Result<Statement> {
        fn bind_value(value: &SqlValue, params: &[SqlValue]) -> Result<SqlValue> {
            match value {
                SqlValue::Parameter(index) => {
                    if *index >= params.len() {
                        let position = index + 1;
                        let available = params.len();
                        return Err(crate::Error::Other(format!(
                            "Parameter index {position} out of bounds (only {available} parameters provided)"
                        )));
                    }
                    Ok(params[*index].clone())
                }
                _ => Ok(value.clone()),
            }
        }
        fn bind_expr(
            expr: &crate::parser::Expression,
            params: &[SqlValue],
        ) -> Result<crate::parser::Expression> {
            use crate::parser::Expression;
            match expr {
                Expression::Value(v) => Ok(Expression::Value(bind_value(v, params)?)),
                Expression::Column(c) => Ok(Expression::Column(c.clone())),
                Expression::BinaryOp {
                    left,
                    operator,
                    right,
                } => Ok(Expression::BinaryOp {
                    left: Box::new(bind_expr(left, params)?),
                    operator: *operator,
                    right: Box::new(bind_expr(right, params)?),
                }),
                Expression::FunctionCall { name, args } => Ok(Expression::FunctionCall {
                    name: name.clone(),
                    args: args
                        .iter()
                        .map(|arg| bind_expr(arg, params))
                        .collect::<Result<Vec<_>>>()?,
                }),
                Expression::AggregateFunction { name, arg } => Ok(Expression::AggregateFunction {
                    name: name.clone(),
                    arg: Box::new(bind_expr(arg, params)?),
                }),
            }
        }
        fn bind_condition(
            cond: &crate::parser::Condition,
            params: &[SqlValue],
        ) -> Result<crate::parser::Condition> {
            use crate::parser::Condition;
            match cond {
                Condition::Comparison {
                    left,
                    operator,
                    right,
                } => Ok(Condition::Comparison {
                    left: left.clone(),
                    operator: *operator,
                    right: bind_value(right, params)?,
                }),
                Condition::Between { column, low, high } => Ok(Condition::Between {
                    column: column.clone(),
                    low: bind_value(low, params)?,
                    high: bind_value(high, params)?,
                }),
                Condition::And(l, r) => Ok(Condition::And(
                    Box::new(bind_condition(l, params)?),
                    Box::new(bind_condition(r, params)?),
                )),
                Condition::Or(l, r) => Ok(Condition::Or(
                    Box::new(bind_condition(l, params)?),
                    Box::new(bind_condition(r, params)?),
                )),
            }
        }
        use crate::parser::Statement;
        match statement {
            Statement::Select(s) => {
                let columns = s
                    .columns
                    .iter()
                    .map(|expr| bind_expr(expr, params))
                    .collect::<Result<Vec<_>>>()?;

                let where_clause = if let Some(wc) = &s.where_clause {
                    Some(crate::parser::WhereClause {
                        condition: bind_condition(&wc.condition, params)?,
                    })
                } else {
                    None
                };

                let order_by = if let Some(order) = &s.order_by {
                    let items = order
                        .items
                        .iter()
                        .map(|item| {
                            Ok(crate::parser::OrderByItem {
                                expression: bind_expr(&item.expression, params)?,
                                direction: item.direction,
                            })
                        })
                        .collect::<Result<Vec<_>>>()?;
                    Some(crate::parser::OrderByClause { items })
                } else {
                    None
                };

                Ok(Statement::Select(crate::parser::SelectStatement {
                    columns,
                    table: s.table.clone(),
                    where_clause,
                    order_by,
                    limit: s.limit,
                }))
            }
            Statement::Insert(s) => {
                // Bind expressions instead of SqlValues
                let mut values = Vec::new();
                for row in &s.values {
                    let mut new_row = Vec::new();
                    for expr in row {
                        // Bind parameters in expressions
                        new_row.push(bind_expr(expr, params)?);
                    }
                    values.push(new_row);
                }
                Ok(Statement::Insert(crate::parser::InsertStatement {
                    table: s.table.clone(),
                    columns: s.columns.clone(),
                    values,
                }))
            }
            Statement::Update(s) => {
                let assignments = s
                    .assignments
                    .iter()
                    .map(|a| {
                        Ok(crate::parser::Assignment {
                            column: a.column.clone(),
                            value: bind_expr(&a.value, params)?,
                        })
                    })
                    .collect::<Result<Vec<_>>>()?;
                let where_clause = if let Some(wc) = &s.where_clause {
                    Some(crate::parser::WhereClause {
                        condition: bind_condition(&wc.condition, params)?,
                    })
                } else {
                    None
                };
                Ok(Statement::Update(crate::parser::UpdateStatement {
                    table: s.table.clone(),
                    assignments,
                    where_clause,
                }))
            }
            Statement::Delete(s) => {
                let where_clause = if let Some(wc) = &s.where_clause {
                    Some(crate::parser::WhereClause {
                        condition: bind_condition(&wc.condition, params)?,
                    })
                } else {
                    None
                };
                Ok(Statement::Delete(crate::parser::DeleteStatement {
                    table: s.table.clone(),
                    where_clause,
                }))
            }
            Statement::CreateTable(s) => Ok(Statement::CreateTable(s.clone())),
            Statement::DropTable(s) => Ok(Statement::DropTable(s.clone())),
            Statement::CreateIndex(s) => Ok(Statement::CreateIndex(s.clone())),
            Statement::DropIndex(s) => Ok(Statement::DropIndex(s.clone())),
            Statement::Begin => Ok(Statement::Begin),
            Statement::Commit => Ok(Statement::Commit),
            Statement::Rollback => Ok(Statement::Rollback),
        }
    }
}

/// Query result containing columns and rows
#[derive(Debug, Clone, PartialEq)]
pub struct QueryResult {
    columns: Vec<String>,
    rows: Vec<Vec<crate::parser::SqlValue>>,
}

impl QueryResult {
    /// Get column names
    pub fn columns(&self) -> &[String] {
        &self.columns
    }

    /// Get all rows
    pub fn rows(&self) -> &[Vec<crate::parser::SqlValue>] {
        &self.rows
    }

    /// Get number of rows
    pub fn len(&self) -> usize {
        self.rows.len()
    }

    /// Check if result is empty
    pub fn is_empty(&self) -> bool {
        self.rows.is_empty()
    }

    /// Collect rows into a Vec (for compatibility)
    pub fn collect_rows(self) -> Result<Vec<Vec<crate::parser::SqlValue>>> {
        Ok(self.rows)
    }

    // ========== CLEAN API METHODS - No SqlValue exposed! ==========

    /// Get the first row as clean Rust types - no SqlValue!
    pub fn first_row_text(&self) -> Option<Vec<String>> {
        self.rows.first().map(|row| {
            row.iter().map(|value| value.as_text().unwrap_or_default()).collect()
        })
    }

    /// Get all rows as clean String vectors - no SqlValue!
    pub fn rows_as_text(&self) -> Vec<Vec<String>> {
        self.rows.iter().map(|row| {
            row.iter().map(|value| value.as_text().unwrap_or_default()).collect()
        }).collect()
    }

    /// Get a specific cell as text - no SqlValue!
    pub fn get_cell_text(&self, row: usize, col: usize) -> Option<String> {
        self.rows.get(row).and_then(|r| r.get(col)).and_then(|v| v.as_text())
    }

    /// Get a specific cell as integer - no SqlValue!
    pub fn get_cell_integer(&self, row: usize, col: usize) -> Option<i64> {
        self.rows.get(row).and_then(|r| r.get(col)).and_then(|v| v.as_integer())
    }

    /// Get a specific cell as real number - no SqlValue!
    pub fn get_cell_real(&self, row: usize, col: usize) -> Option<f64> {
        self.rows.get(row).and_then(|r| r.get(col)).and_then(|v| v.as_real())
    }

    /// Get all values in a column as text - no SqlValue!
    pub fn get_column_text(&self, col_index: usize) -> Vec<String> {
        self.rows.iter()
            .filter_map(|row| row.get(col_index))
            .filter_map(|value| value.as_text())
            .collect()
    }

    /// Convert to a simple HashMap<String, String> for first row - useful for single-row results
    pub fn as_map(&self) -> Option<std::collections::HashMap<String, String>> {
        self.first_row_text().map(|row| {
            self.columns.iter().zip(row.iter())
                .map(|(col, val)| (col.clone(), val.clone()))
                .collect()
        })
    }
}

// Allow iterating over QueryResult as a stream of Result<Vec<SqlValue>>
impl IntoIterator for QueryResult {
    type Item = Result<Vec<crate::parser::SqlValue>>;
    type IntoIter = std::vec::IntoIter<Self::Item>;

    fn into_iter(self) -> Self::IntoIter {
        self.rows
            .into_iter()
            .map(Ok)
            .collect::<Vec<_>>()
            .into_iter()
    }
}

/// Transaction handle for batch operations
pub struct DatabaseTransaction<'a> {
    processor: QueryProcessor<'a>,
    catalog: &'a mut Catalog,
}

impl DatabaseTransaction<'_> {
    /// Execute SQL statement within transaction
    pub fn execute(&mut self, sql: &str) -> Result<usize> {
        let statement = parse_sql(sql).map_err(|e| crate::Error::ParseError(e.to_string()))?;

        // Get schemas from shared catalog and convert to Rc
        let schemas = Database::get_schemas_rc(self.catalog.get_all_schemas());

        // Use the planner pipeline
        let planner = QueryPlanner::new(schemas);
        let plan = planner.plan(statement.clone())?;
        let result = self.processor.execute_plan(plan)?;

        // Update schema cache for DDL operations using centralized helper
        Database::update_schema_catalog_for_ddl(self.catalog, &statement);

        match result {
            crate::query_processor::ResultSet::Insert { rows_affected } => Ok(rows_affected),
            crate::query_processor::ResultSet::Update { rows_affected } => Ok(rows_affected),
            crate::query_processor::ResultSet::Delete { rows_affected } => Ok(rows_affected),
            crate::query_processor::ResultSet::CreateTable => Ok(0),
            _ => Ok(0),
        }
    }

    /// Execute SQL query within transaction, return all results materialized in memory
    /// Following the parse -> plan -> execute_plan pipeline
    pub fn query(&mut self, sql: &str) -> Result<QueryResult> {
        // Get schemas from shared cache (reuse existing schemas in processor)
        let schemas = self.catalog.get_all_schemas().clone();

        // Use centralized query execution helper
        // Note: We need to be careful about borrowing here since we can't move self.executor
        // Instead, we'll use a more direct approach that's still centralized
        Database::execute_query_with_processor_ref(&mut self.processor, sql, &schemas)
    }

    /// Commit the transaction
    pub fn commit(mut self) -> Result<()> {
        self.processor.transaction_mut().commit()
    }

    /// Rollback the transaction
    pub fn rollback(mut self) -> Result<()> {
        self.processor.transaction_mut().rollback()
    }
}

// Helper to check for parameter in a condition
fn contains_param_in_condition(cond: &crate::parser::Condition) -> bool {
    use crate::parser::{Condition, SqlValue};
    match cond {
        Condition::Comparison { right, .. } => matches!(right, SqlValue::Parameter(_)),
        Condition::Between { low, high, .. } => {
            matches!(low, SqlValue::Parameter(_)) || matches!(high, SqlValue::Parameter(_))
        }
        Condition::And(left, right) | Condition::Or(left, right) => {
            contains_param_in_condition(left) || contains_param_in_condition(right)
        }
    }
}
// Helper to check for parameter in a plan (recursively)
fn plan_has_param(plan: &crate::planner::ExecutionPlan) -> bool {
    use crate::parser::SqlValue;
    use crate::planner::ExecutionPlan;
    match plan {
        ExecutionPlan::PrimaryKeyLookup {
            pk_value,
            additional_filter,
            ..
        } => {
            matches!(pk_value, SqlValue::Parameter(_))
                || additional_filter
                    .as_ref()
                    .is_some_and(contains_param_in_condition)
        }
        ExecutionPlan::TableRangeScan {
            pk_range,
            additional_filter,
            ..
        } => {
            pk_range
                .start_bound
                .as_ref()
                .is_some_and(|b| matches!(b.value, SqlValue::Parameter(_)))
                || pk_range
                    .end_bound
                    .as_ref()
                    .is_some_and(|b| matches!(b.value, SqlValue::Parameter(_)))
                || additional_filter
                    .as_ref()
                    .is_some_and(contains_param_in_condition)
        }
        ExecutionPlan::TableScan { filter, .. } => {
            filter.as_ref().is_some_and(contains_param_in_condition)
        }
        ExecutionPlan::Insert { rows, .. } => rows
            .iter()
            .any(|row| row.values().any(|v| matches!(v, SqlValue::Parameter(_)))),
        ExecutionPlan::Update {
            assignments,
            scan_plan,
            ..
        } => assignments.iter().any(|a| expr_has_param(&a.value)) || plan_has_param(scan_plan),
        ExecutionPlan::Delete { scan_plan, .. } => plan_has_param(scan_plan),
        _ => false,
    }
}
// Helper to check for parameter in an expression
fn expr_has_param(expr: &crate::parser::Expression) -> bool {
    use crate::parser::Expression;
    match expr {
        Expression::Value(crate::parser::SqlValue::Parameter(_)) => true,
        Expression::Value(_) => false,
        Expression::Column(_) => false,
        Expression::BinaryOp { left, right, .. } => expr_has_param(left) || expr_has_param(right),
        Expression::FunctionCall { args, .. } => args.iter().any(expr_has_param),
        Expression::AggregateFunction { arg, .. } => expr_has_param(arg),
    }
}
// Extend instantiate_plan_with_params to handle INSERT, UPDATE, DELETE
fn instantiate_plan_with_params(
    plan: &crate::planner::ExecutionPlan,
    params: &[crate::parser::SqlValue],
) -> crate::planner::ExecutionPlan {
    use crate::parser::SqlValue;
    use crate::planner::{Assignment, ExecutionPlan, PkBound, PkRange};
    match plan {
        ExecutionPlan::PrimaryKeyLookup {
            table,
            pk_value,
            selected_columns,
            additional_filter,
        } => {
            let pk_value = match pk_value {
                SqlValue::Parameter(idx) => params.get(*idx).cloned().unwrap_or(SqlValue::Null),
                v => v.clone(),
            };
            ExecutionPlan::PrimaryKeyLookup {
                table: table.clone(),
                pk_value,
                selected_columns: selected_columns.clone(),
                additional_filter: additional_filter
                    .clone()
                    .map(|c| instantiate_condition_with_params(&c, params)),
            }
        }
        ExecutionPlan::TableRangeScan {
            table,
            selected_columns,
            pk_range,
            additional_filter,
            limit,
        } => {
            let start_bound = pk_range.start_bound.as_ref().map(|b| PkBound {
                value: match &b.value {
                    SqlValue::Parameter(idx) => params.get(*idx).cloned().unwrap_or(SqlValue::Null),
                    v => v.clone(),
                },
                inclusive: b.inclusive,
            });
            let end_bound = pk_range.end_bound.as_ref().map(|b| PkBound {
                value: match &b.value {
                    SqlValue::Parameter(idx) => params.get(*idx).cloned().unwrap_or(SqlValue::Null),
                    v => v.clone(),
                },
                inclusive: b.inclusive,
            });
            ExecutionPlan::TableRangeScan {
                table: table.clone(),
                selected_columns: selected_columns.clone(),
                pk_range: PkRange {
                    start_bound,
                    end_bound,
                },
                additional_filter: additional_filter
                    .clone()
                    .map(|c| instantiate_condition_with_params(&c, params)),
                limit: *limit,
            }
        }
        ExecutionPlan::TableScan {
            table,
            selected_columns,
            filter,
            limit,
        } => {
            let filter = filter
                .as_ref()
                .map(|c| instantiate_condition_with_params(c, params));
            ExecutionPlan::TableScan {
                table: table.clone(),
                selected_columns: selected_columns.clone(),
                filter,
                limit: *limit,
            }
        }
        ExecutionPlan::Insert {
            table,
            rows,
            conflict_resolution,
        } => {
            let new_rows = rows
                .iter()
                .map(|row| {
                    row.iter()
                        .map(|(k, v)| {
                            let new_v = match v {
                                SqlValue::Parameter(idx) => {
                                    params.get(*idx).cloned().unwrap_or(SqlValue::Null)
                                }
                                v => v.clone(),
                            };
                            (k.clone(), new_v)
                        })
                        .collect()
                })
                .collect();
            ExecutionPlan::Insert {
                table: table.clone(),
                rows: new_rows,
                conflict_resolution: conflict_resolution.clone(),
            }
        }
        ExecutionPlan::Update {
            table,
            assignments,
            scan_plan,
        } => {
            let new_assignments: Vec<Assignment> = assignments
                .iter()
                .map(|a| Assignment {
                    column: a.column.clone(),
                    value: instantiate_expr_with_params(&a.value, params),
                })
                .collect();
            ExecutionPlan::Update {
                table: table.clone(),
                assignments: new_assignments,
                scan_plan: Box::new(instantiate_plan_with_params(scan_plan, params)),
            }
        }
        ExecutionPlan::Delete { table, scan_plan } => ExecutionPlan::Delete {
            table: table.clone(),
            scan_plan: Box::new(instantiate_plan_with_params(scan_plan, params)),
        },
        _ => plan.clone(),
    }
}
// Helper to instantiate parameters in Expression
fn instantiate_expr_with_params(
    expr: &crate::parser::Expression,
    params: &[crate::parser::SqlValue],
) -> crate::parser::Expression {
    use crate::parser::Expression;
    match expr {
        Expression::Value(crate::parser::SqlValue::Parameter(idx)) => Expression::Value(
            params
                .get(*idx)
                .cloned()
                .unwrap_or(crate::parser::SqlValue::Null),
        ),
        Expression::Value(_) => expr.clone(),
        Expression::Column(_) => expr.clone(),
        Expression::BinaryOp {
            left,
            operator,
            right,
        } => Expression::BinaryOp {
            left: Box::new(instantiate_expr_with_params(left, params)),
            operator: *operator,
            right: Box::new(instantiate_expr_with_params(right, params)),
        },
        Expression::FunctionCall { name, args } => Expression::FunctionCall {
            name: name.clone(),
            args: args
                .iter()
                .map(|arg| instantiate_expr_with_params(arg, params))
                .collect(),
        },
        Expression::AggregateFunction { name, arg } => Expression::AggregateFunction {
            name: name.clone(),
            arg: Box::new(instantiate_expr_with_params(arg, params)),
        },
    }
}

// Helper to instantiate parameters in Condition
fn instantiate_condition_with_params(
    cond: &crate::parser::Condition,
    params: &[crate::parser::SqlValue],
) -> crate::parser::Condition {
    use crate::parser::{Condition, SqlValue};
    match cond {
        Condition::Comparison {
            left,
            operator,
            right,
        } => Condition::Comparison {
            left: left.clone(),
            operator: *operator,
            right: match right {
                SqlValue::Parameter(idx) => params.get(*idx).cloned().unwrap_or(SqlValue::Null),
                v => v.clone(),
            },
        },
        Condition::Between { column, low, high } => Condition::Between {
            column: column.clone(),
            low: match low {
                SqlValue::Parameter(idx) => params.get(*idx).cloned().unwrap_or(SqlValue::Null),
                v => v.clone(),
            },
            high: match high {
                SqlValue::Parameter(idx) => params.get(*idx).cloned().unwrap_or(SqlValue::Null),
                v => v.clone(),
            },
        },
        Condition::And(left, right) => Condition::And(
            Box::new(instantiate_condition_with_params(left, params)),
            Box::new(instantiate_condition_with_params(right, params)),
        ),
        Condition::Or(left, right) => Condition::Or(
            Box::new(instantiate_condition_with_params(left, params)),
            Box::new(instantiate_condition_with_params(right, params)),
        ),
    }
}
