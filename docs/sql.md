# SQL Reference

TegDB implements a focused SQL subset for single-table workloads. This guide
covers supported syntax, behavior, and the parsed abstract syntax tree (AST).

## Scope and Conventions

- Absolute database paths are required, for example `file:///tmp/example.teg`.
- Identifiers are case-insensitive; string literals use single quotes.
- Unsupported (currently): joins, foreign keys, subqueries, views, triggers.

## Data Types

- `INTEGER` — 64-bit signed.
- `REAL` — 64-bit floating point.
- `TEXT(n)` — UTF-8 string with required length bound `n`.
- `NULL` — null literal.
- `VECTOR(n)` — fixed-dimension vector of `REAL` for similarity (if enabled by
  build).

## Data Definition (DDL)

```sql
CREATE TABLE products (
    id INTEGER PRIMARY KEY,
    name TEXT(64) NOT NULL,
    price REAL,
    category TEXT(32)
);

DROP TABLE IF EXISTS products;
```

Constraints: `PRIMARY KEY`, `NOT NULL`, `UNIQUE`. Only the primary key is
indexed; no secondary indexes.

## Data Manipulation (DML)

```sql
-- Insert single and multiple rows
INSERT INTO products (id, name, price) VALUES (1, 'Widget', 19.99);
INSERT INTO products (id, name, price) VALUES
  (2, 'Gadget', 29.99),
  (3, 'Tool', 39.99);

-- Update with predicate
UPDATE products SET price = 24.99 WHERE id = 1;

-- Delete with predicate
DELETE FROM products WHERE price < 20.0;

-- Select with filtering and limits
SELECT name, price FROM products WHERE category = 'Hardware' LIMIT 10;
```

## Transactions

```sql
BEGIN;
UPDATE accounts SET balance = balance - 100 WHERE id = 1;
UPDATE accounts SET balance = balance + 100 WHERE id = 2;
COMMIT; -- or ROLLBACK;
```

- Write-through with snapshot-like reads.
- Transactions auto-rollback on drop if not committed.

## Built-in Functions

**String (extension: `tegdb_string`)**

- `UPPER(text)` — uppercase.
- `LOWER(text)` — lowercase.
- `LENGTH(text)` — length in characters.
- `TRIM(text)`, `LTRIM(text)`, `RTRIM(text)` — trim whitespace.
- `SUBSTR(text, start, length)` — substring.
- `REPLACE(text, from, to)` — replace occurrences.
- `CONCAT(text, ...)` — variadic concatenation.
- `REVERSE(text)` — reverse string.

**Math (extension: `tegdb_math`)**

- `ABS(num)` — absolute value.
- `CEIL(num)`, `FLOOR(num)` — ceiling/floor.
- `ROUND(num, decimals)` — round to decimals.
- `SQRT(num)` — square root.
- `POW(base, exp)` — exponentiation.
- `MOD(a, b)` — modulo.
- `SIGN(num)` — -1/0/1 sign.

## Errors and Limits

- `TEXT(n)` enforces length; exceeding it returns a validation error.
- Type mismatches or missing columns surface as parser/execution errors.
- Only primary-key lookups are indexed; other predicates scan.
- No joins, foreign keys, subqueries, or views yet.

## AST Overview

Parser output is defined in `src/parser.rs`:

Simplified AST shape (Rust-style) for reference:

```rust
pub enum Statement {
    Select(SelectStatement),
    Insert(InsertStatement),
    Update(UpdateStatement),
    Delete(DeleteStatement),
    CreateTable(CreateTableStatement),
    DropTable(DropTableStatement),
    CreateIndex(CreateIndexStatement),
    DropIndex(DropIndexStatement),
    CreateExtension(CreateExtensionStatement),
    DropExtension(DropExtensionStatement),
    Begin,
    Commit,
    Rollback,
}

pub struct SelectStatement {
    pub columns: Vec<Expression>,
    pub table: String,
    pub where_clause: Option<WhereClause>,
    pub order_by: Option<OrderByClause>,
    pub limit: Option<u64>,
}

pub struct InsertStatement {
    pub table: String,
    pub columns: Vec<String>,
    pub values: Vec<Vec<Expression>>,
}

pub struct UpdateStatement {
    pub table: String,
    pub assignments: Vec<Assignment>,
    pub where_clause: Option<WhereClause>,
}

pub struct DeleteStatement {
    pub table: String,
    pub where_clause: Option<WhereClause>,
}

pub struct CreateTableStatement {
    pub table: String,
    pub columns: Vec<ColumnDefinition>,
}

pub struct DropTableStatement {
    pub table: String,
    pub if_exists: bool,
}

pub struct CreateIndexStatement {
    pub index_name: String,
    pub table_name: String,
    pub column_name: String,
    pub unique: bool,
    pub index_type: Option<IndexType>,
}

pub struct DropIndexStatement {
    pub index_name: String,
    pub if_exists: bool,
}

pub struct CreateExtensionStatement {
    pub extension_name: String,
    pub library_path: Option<String>,
}

pub struct DropExtensionStatement {
    pub extension_name: String,
}

pub enum Expression {
    Value(SqlValue),
    Column(String),
    BinaryOp { left: Box<Expression>, operator: ArithmeticOperator, right: Box<Expression> },
    FunctionCall { name: String, args: Vec<Expression> },
    AggregateFunction { name: String, arg: Box<Expression> },
}

pub enum Condition {
    Comparison { left: Expression, operator: ComparisonOperator, right: SqlValue },
    Between { column: String, low: SqlValue, high: SqlValue },
    And(Box<Condition>, Box<Condition>),
    Or(Box<Condition>, Box<Condition>),
}

pub struct WhereClause {
    pub condition: Condition,
}

pub struct OrderByClause {
    pub items: Vec<OrderByItem>,
}

pub struct OrderByItem {
    pub expression: Expression,
    pub direction: OrderDirection,
}

pub struct Assignment {
    pub column: String,
    pub value: Expression,
}

pub struct ColumnDefinition {
    pub name: String,
    pub data_type: DataType,
    pub constraints: Vec<ColumnConstraint>,
}

pub enum DataType {
    Integer,
    Text(Option<usize>),
    Real,
    Vector(Option<usize>),
}

pub enum ColumnConstraint {
    PrimaryKey,
    NotNull,
    Unique,
}

pub enum IndexType {
    BTree,
    HNSW,
    IVF,
    LSH,
}

pub enum SqlValue {
    Integer(i64),
    Real(f64),
    Text(String),
    Vector(Vec<f64>),
    Null,
    Parameter(usize),
}
```

- `Statement`: `Select`, `Insert`, `Update`, `Delete`, `CreateTable`,
  `DropTable`, `CreateIndex`, `DropIndex`, `CreateExtension`, `DropExtension`,
  `Begin`, `Commit`, `Rollback`.
- `Expression`: column refs, literals, arithmetic, function calls, aggregates
  (`Expression::AggregateFunction`), parameters.
- `Condition`: comparisons (`=, !=, <, <=, >, >=, LIKE`), `AND`, `OR`,
  `BETWEEN`.
- `CreateTableStatement`: `table`, `columns: Vec<ColumnDefinition>` where
  `data_type` supports `Integer`, `Real`, `Text(Option<usize>)`,
  `Vector(Option<usize>)`; constraints include `PrimaryKey`, `NotNull`,
  `Unique`.
- `CreateIndexStatement`: `index_name`, `table_name`, `column_name`, `unique`,
  `index_type` (`BTree`, `HNSW`, `IVF`, `LSH`).
- `SqlValue`: `Integer`, `Real`, `Text`, `Vector`, `Null`, `Parameter(idx)`.

These structures form the AST handed to execution; unsupported syntax fails
parsing before execution.

## Quick Example (CLI)

```bash
DB=file:///$(pwd)/quickstart.teg
tg "$DB" --command "CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT(32));"
tg "$DB" --command "INSERT INTO users (id, name) VALUES (1, 'Alice'), (2, 'Bob');"
tg "$DB" --command "SELECT name FROM users WHERE id = 1;"
```

## Prepare/Execute Protocol (Library)

The Rust API supports prepared statements with parameter binding and optional
plan templates:

```rust
use tegdb::{Database, SqlValue};

let mut db = Database::open("file:///tmp/app.teg")?;
let stmt = db.prepare("INSERT INTO users (id, name) VALUES ($1, $2)")?;
db.execute_prepared(&stmt, &[SqlValue::Integer(1), SqlValue::Text("Alice".into())])?;

let q = db.prepare("SELECT name FROM users WHERE id = $1")?;
let result = db.query_prepared(&q, &[SqlValue::Integer(1)])?;
```

Helpers accept simple types (anything `Into<SqlValue>`):

```rust
db.execute_prepared_simple(&stmt, &[1, "Bob"])?;
let rows = db.query_prepared_simple(&q, &[1])?;
```

- `prepare(sql)` parses SQL and, for SELECT, records column metadata; it may
  cache a plan template when parameters are present.
- `execute_prepared` / `query_prepared` validate arity and reuse the cached
  plan when possible, otherwise rebind parameters and plan.
- Use parameters (`$1`, `$2`, ...) instead of string interpolation to avoid SQL
  injection and reuse plans.
