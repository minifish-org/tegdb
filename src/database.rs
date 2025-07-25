//! High-level database interface
//!
//! This module provides a SQLite-like interface for TegDB, making it easy for users
//! to interact with the database without dealing with low-level engine details.

use crate::catalog::Catalog;
use crate::parser::{parse_sql, SqlValue, Statement};
use crate::parser::Expression;
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
                        .filter(|v| matches!(v, SqlValue::Parameter(_)))
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
            Expression::FunctionCall { .. } => 0,
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
    /// Create or open database
    ///
    /// On native platforms: Only accepts absolute paths with the file:// protocol.
    /// On WASM platforms: Supports browser://, localStorage://, and indexeddb:// protocols.
    ///
    /// Examples:
    /// - ✅ file:///absolute/path/to/db (native only)
    /// - ✅ browser://my-app-db (WASM only)
    /// - ✅ localStorage://user-data (WASM only)
    /// - ✅ indexeddb://app-cache (WASM only)
    /// - ❌ relative/path (no protocol)
    /// - ❌ file://relative/path (relative path with protocol)
    pub fn open<P: AsRef<str>>(path: P) -> Result<Self> {
        let path_str = path.as_ref();
        let (protocol, path_part) = crate::protocol_utils::parse_storage_identifier(path_str);

        #[cfg(not(target_arch = "wasm32"))]
        {
            // On native platforms, only support file protocol
            if protocol != "file" {
                return Err(crate::Error::Other(format!(
                    "Unsupported protocol: {protocol}. Only 'file://' protocol is supported on native platforms."
                )));
            }

            // Check if path is absolute
            let path_buf = std::path::Path::new(path_part);
            if !path_buf.is_absolute() {
                return Err(crate::Error::Other(format!(
                    "Path must be absolute. Got: '{path_str}'. Use absolute path like 'file:///absolute/path/to/db'"
                )));
            }

            let storage = StorageEngine::new(path_buf.to_path_buf())?;

            // Load all table schemas into the catalog at database initialization
            let catalog = Catalog::load_from_storage(&storage)?;

            Ok(Self { storage, catalog })
        }

        #[cfg(target_arch = "wasm32")]
        {
            // On WASM platforms, support browser protocols
            match protocol {
                "browser" | "localstorage" | "indexeddb" => {
                    // For browser backends, we use the full identifier string
                    let storage = StorageEngine::new_with_identifier(path_str.to_string())?;

                    // Load all table schemas into the catalog at database initialization
                    let catalog = Catalog::load_from_storage(&storage)?;

                    Ok(Self { storage, catalog })
                }
                "file" => {
                    // File protocol is not supported on WASM
                    return Err(crate::Error::Other(format!(
                        "File protocol is not supported on WASM. Use 'browser://', 'localstorage://', or 'indexeddb://' protocols instead."
                    )));
                }
                _ => {
                    return Err(crate::Error::Other(format!(
                        "Unsupported protocol: {}. On WASM, only 'browser://', 'localstorage://', and 'indexeddb://' protocols are supported.",
                        protocol
                    )));
                }
            }
        }
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
            _ => {} // No schema changes for other statements
        }
    }

    /// Centralized query execution helper to eliminate duplication
    /// Executes SELECT statements and returns QueryResult
    fn execute_query_with_processor(
        mut processor: QueryProcessor<'_>,
        sql: &str,
        schemas: &HashMap<String, Rc<TableSchema>>,
    ) -> Result<QueryResult> {
        // Get schemas in Rc format for the planner
        let rc_schemas = Self::get_schemas_rc(schemas);
        Self::execute_query_core(&mut processor, sql, &rc_schemas)
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
        let statement =
            parse_sql(sql).map_err(|e| crate::Error::Other(format!("SQL parse error: {e:?}")))?;

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

    /// Execute SQL statement, return number of affected rows
    pub fn execute(&mut self, sql: &str) -> Result<usize> {
        let statement =
            parse_sql(sql).map_err(|e| crate::Error::Other(format!("SQL parse error: {e:?}")))?;

        // Use a single transaction for this operation
        let transaction = self.storage.begin_transaction();

        // Get schemas in Rc format for shared ownership (no cloning needed)
        let schemas = Self::get_schemas_rc(self.catalog.get_all_schemas());

        // Use the new planner pipeline with executor
        let planner = QueryPlanner::new(schemas.clone());
        let mut processor = QueryProcessor::new_with_rc_schemas(transaction, schemas);

        // Generate and execute the plan (no need to begin transaction as it's already started)
        let plan = planner.plan(statement.clone())?;
        let result = processor.execute_plan(plan)?;

        // Process the result immediately to avoid lifetime conflicts
        let final_result = match result {
            crate::query_processor::ResultSet::Insert { rows_affected } => rows_affected,
            crate::query_processor::ResultSet::Update { rows_affected } => rows_affected,
            crate::query_processor::ResultSet::Delete { rows_affected } => rows_affected,
            crate::query_processor::ResultSet::CreateTable => 0,
            crate::query_processor::ResultSet::DropTable => 0,
            crate::query_processor::ResultSet::Begin => 0,
            crate::query_processor::ResultSet::Commit => 0,
            crate::query_processor::ResultSet::Rollback => 0,
            crate::query_processor::ResultSet::Select { .. } => {
                return Err(crate::Error::Other(
                    "execute() should not be used for SELECT statements. Use query() instead."
                        .to_string(),
                ))
            }
        };
        // Drop the result to release the borrow
        drop(result);

        // Update our shared schemas cache for DDL operations using centralized helper
        Self::update_schema_catalog_for_ddl(&mut self.catalog, &statement);

        // Actually commit the engine transaction
        processor.transaction_mut().commit()?;

        Ok(final_result)
    }

    /// Execute SQL query, return all results materialized in memory
    /// This follows the parse -> plan -> execute_plan pipeline but returns simple QueryResult
    pub fn query(&mut self, sql: &str) -> Result<QueryResult> {
        // Get schemas in Rc format for shared ownership (no cloning needed)
        let schemas = Self::get_schemas_rc(self.catalog.get_all_schemas());

        // Use a single transaction for this operation
        let transaction = self.storage.begin_transaction();

        // Create executor with schemas
        let processor = QueryProcessor::new_with_rc_schemas(transaction, schemas.clone());

        // Use centralized query execution helper
        let result =
            Self::execute_query_with_processor(processor, sql, self.catalog.get_all_schemas())?;

        Ok(result)
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
        let statement =
            parse_sql(sql).map_err(|e| crate::Error::Other(format!("SQL parse error: {e:?}")))?;
        let query_schema = if let Statement::Select(ref select) = statement {
            let schemas = Self::get_schemas_rc(self.catalog.get_all_schemas());
            let columns: Vec<String> = select.columns.iter().map(|expr| {
                if let Expression::Column(ref name) = expr {
                    Ok(name.clone())
                } else {
                    Err(crate::Error::Other("Only column names are supported in SELECT for now".to_string()))
                }
            }).collect::<Result<Vec<_>>>()?;
            let schema = schemas.get(&select.table).ok_or_else(|| {
                crate::Error::Other(format!("Table '{}' not found", &select.table))
            })?;
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
            return Err(crate::Error::Other(format!(
                "Expected {} parameters, got {}",
                stmt.parameter_count(),
                params.len()
            )));
        }
        // Use plan template if available and valid
        if let Some(ref plan_template) = stmt.plan_template {
            let instantiated_plan = instantiate_plan_with_params(plan_template, params);
            let schemas = Self::get_schemas_rc(self.catalog.get_all_schemas());
            let transaction = self.storage.begin_transaction();
            let mut processor = QueryProcessor::new_with_rc_schemas(transaction, schemas);
            let result = processor.execute_plan(instantiated_plan)?;
            let final_result = match result {
                crate::query_processor::ResultSet::Insert { rows_affected } => rows_affected,
                crate::query_processor::ResultSet::Update { rows_affected } => rows_affected,
                crate::query_processor::ResultSet::Delete { rows_affected } => rows_affected,
                crate::query_processor::ResultSet::CreateTable => 0,
                crate::query_processor::ResultSet::DropTable => 0,
                crate::query_processor::ResultSet::Begin => 0,
                crate::query_processor::ResultSet::Commit => 0,
                crate::query_processor::ResultSet::Rollback => 0,
                crate::query_processor::ResultSet::Select { .. } => {
                    return Err(crate::Error::Other(
                        "execute_prepared() should not be used for SELECT statements. Use query_prepared() instead."
                            .to_string(),
                    ))
                }
            };
            drop(result);
            Self::update_schema_catalog_for_ddl(&mut self.catalog, &stmt.statement);
            processor.transaction_mut().commit()?;
            Ok(final_result)
        } else {
            // Fallback: bind parameters and plan as before
            let bound_stmt = Self::bind_parameters_to_statement(&stmt.statement, params)?;
            let schemas = Self::get_schemas_rc(self.catalog.get_all_schemas());
            let planner = QueryPlanner::new(schemas.clone());
            let plan = planner.plan(bound_stmt.clone())?;
            let transaction = self.storage.begin_transaction();
            let mut processor = QueryProcessor::new_with_rc_schemas(transaction, schemas.clone());
            let result = processor.execute_plan(plan)?;
            let final_result = match result {
                crate::query_processor::ResultSet::Insert { rows_affected } => rows_affected,
                crate::query_processor::ResultSet::Update { rows_affected } => rows_affected,
                crate::query_processor::ResultSet::Delete { rows_affected } => rows_affected,
                crate::query_processor::ResultSet::CreateTable => 0,
                crate::query_processor::ResultSet::DropTable => 0,
                crate::query_processor::ResultSet::Begin => 0,
                crate::query_processor::ResultSet::Commit => 0,
                crate::query_processor::ResultSet::Rollback => 0,
                crate::query_processor::ResultSet::Select { .. } => {
                    return Err(crate::Error::Other(
                        "execute_prepared() should not be used for SELECT statements. Use query_prepared() instead."
                            .to_string(),
                    ))
                }
            };
            drop(result);
            Self::update_schema_catalog_for_ddl(&mut self.catalog, &bound_stmt);
            processor.transaction_mut().commit()?;
            Ok(final_result)
        }
    }

    /// Execute a prepared SELECT statement with parameters
    /// This is similar to SQLite's prepared statement query execution
    pub fn query_prepared(
        &mut self,
        stmt: &PreparedStatement,
        params: &[SqlValue],
    ) -> Result<QueryResult> {
        if params.len() != stmt.parameter_count() {
            return Err(crate::Error::Other(format!(
                "Expected {} parameters, got {}",
                stmt.parameter_count(),
                params.len()
            )));
        }
        // Use plan template if available and valid
        if let Some(ref plan_template) = stmt.plan_template {
            let instantiated_plan = instantiate_plan_with_params(plan_template, params);
            let schemas = Self::get_schemas_rc(self.catalog.get_all_schemas());
            let transaction = self.storage.begin_transaction();
            let mut processor = QueryProcessor::new_with_rc_schemas(transaction, schemas.clone());
            let result = match &stmt.query_schema {
                Some(query_schema) => {
                    processor.execute_plan_with_query_schema(instantiated_plan, query_schema)
                }
                None => processor.execute_plan(instantiated_plan),
            }?;
            match result {
                crate::query_processor::ResultSet::Select { columns, rows } => {
                    let collected_rows: Result<Vec<Vec<crate::parser::SqlValue>>> = rows.collect();
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
        } else {
            // Fallback: bind parameters and plan as before
            let bound_stmt = Self::bind_parameters_to_statement(&stmt.statement, params)?;
            let schemas = Self::get_schemas_rc(self.catalog.get_all_schemas());
            let planner = QueryPlanner::new(schemas.clone());
            let plan = planner.plan(bound_stmt)?;
            let transaction = self.storage.begin_transaction();
            let mut processor = QueryProcessor::new_with_rc_schemas(transaction, schemas.clone());
            let result = match &stmt.query_schema {
                Some(query_schema) => processor.execute_plan_with_query_schema(plan, query_schema),
                None => processor.execute_plan(plan),
            }?;
            match result {
                crate::query_processor::ResultSet::Select { columns, rows } => {
                    let collected_rows: Result<Vec<Vec<crate::parser::SqlValue>>> = rows.collect();
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
                        return Err(crate::Error::Other(format!(
                            "Parameter index {} out of bounds (only {} parameters provided)",
                            index + 1,
                            params.len()
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
                    args: args.iter().map(|arg| bind_expr(arg, params)).collect::<Result<Vec<_>>>()?,
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
                let where_clause = if let Some(wc) = &s.where_clause {
                    Some(crate::parser::WhereClause {
                        condition: bind_condition(&wc.condition, params)?,
                    })
                } else {
                    None
                };
                Ok(Statement::Select(crate::parser::SelectStatement {
                    columns: s.columns.clone(), // Already Vec<Expression>
                    table: s.table.clone(),
                    where_clause,
                    limit: s.limit,
                }))
            }
            Statement::Insert(s) => {
                let mut values = Vec::new();
                for row in &s.values {
                    let mut new_row = Vec::new();
                    for v in row {
                        new_row.push(bind_value(v, params)?);
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
        let statement =
            parse_sql(sql).map_err(|e| crate::Error::Other(format!("SQL parse error: {e:?}")))?;

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
        Expression::FunctionCall { .. } => false,
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
            args: args.iter().map(|arg| instantiate_expr_with_params(arg, params)).collect(),
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
