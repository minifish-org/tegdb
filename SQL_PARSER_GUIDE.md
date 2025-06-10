# TegDB SQL Parser Implementation

This document provides examples and usage patterns for the SQL parser implementation in TegDB using the `nom` crate.

## Overview

The SQL parser supports the following statements:
- `CREATE TABLE` - Table creation with column definitions and constraints
- `INSERT` - Single and multiple row insertion
- `SELECT` - Data retrieval with WHERE clauses, ORDER BY, and LIMIT
- `UPDATE` - Row updates with WHERE conditions  
- `DELETE` - Row deletion with WHERE conditions

## Basic Usage

```rust
use tegdb::{Engine, sql::{parse_sql, SqlStatement}, sql_executor::{SqlExecutor, SqlResult}};

// Create database and SQL executor
let engine = Engine::new("mydb.db".into())?;
let mut executor = SqlExecutor::new(engine);

// Parse and execute SQL
let sql = "CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT NOT NULL, age INTEGER)";
let (_, statement) = parse_sql(sql)?;
let result = executor.execute(statement)?;
```

## Supported SQL Features

### CREATE TABLE
```sql
CREATE TABLE users (
    id INTEGER PRIMARY KEY,
    name TEXT NOT NULL,
    age INTEGER,
    email TEXT UNIQUE
)
```

### INSERT
```sql
-- Single row
INSERT INTO users (id, name, age) VALUES (1, 'John', 25)

-- Multiple rows
INSERT INTO users (id, name, age) VALUES 
    (1, 'John', 25), 
    (2, 'Jane', 30),
    (3, 'Bob', 35)
```

### SELECT
```sql
-- Select all columns
SELECT * FROM users

-- Select specific columns  
SELECT name, age FROM users

-- With WHERE clause
SELECT * FROM users WHERE age > 25

-- With ORDER BY (parsed but not executed yet)
SELECT * FROM users ORDER BY age DESC

-- With LIMIT
SELECT * FROM users LIMIT 10
```

### UPDATE
```sql
-- Update with condition
UPDATE users SET age = 26 WHERE name = 'John'

-- Multiple assignments
UPDATE users SET age = 30, email = 'john@example.com' WHERE id = 1
```

### DELETE
```sql
-- Delete with condition
DELETE FROM users WHERE age < 18

-- Delete all (use with caution!)
DELETE FROM users
```

## WHERE Clause Operators

The parser supports the following comparison operators:
- `=` - Equal
- `!=` or `<>` - Not equal
- `<` - Less than
- `<=` - Less than or equal
- `>` - Greater than
- `>=` - Greater than or equal
- `LIKE` - Pattern matching (simplified implementation)

### Logical Operators
- `AND` - Logical AND
- `OR` - Logical OR

Example:
```sql
SELECT * FROM users WHERE age > 25 AND name LIKE 'J%'
```

## Data Types

Supported data types:
- `INTEGER` / `INT` - 64-bit signed integers
- `TEXT` / `VARCHAR` - UTF-8 strings
- `REAL` / `FLOAT` - 64-bit floating point numbers
- `BLOB` - Binary data

## Column Constraints

- `PRIMARY KEY` - Primary key constraint
- `NOT NULL` - Non-null constraint
- `UNIQUE` - Unique value constraint

## Error Handling

```rust
match parse_sql("INVALID SQL") {
    Ok((remaining, statement)) => {
        // Handle successful parse
        println!("Parsed: {:?}", statement);
        if !remaining.trim().is_empty() {
            println!("Unparsed: {}", remaining);
        }
    }
    Err(e) => {
        // Handle parse error
        println!("Parse error: {:?}", e);
    }
}
```

## AST Structure

The parser generates an Abstract Syntax Tree (AST) with the following main types:

```rust
pub enum SqlStatement {
    Select(SelectStatement),
    Insert(InsertStatement), 
    Update(UpdateStatement),
    Delete(DeleteStatement),
    CreateTable(CreateTableStatement),
}
```

You can pattern match on the parsed statements:

```rust
match statement {
    SqlStatement::Select(select) => {
        println!("Table: {}", select.table);
        println!("Columns: {:?}", select.columns);
    }
    SqlStatement::Insert(insert) => {
        println!("Inserting {} rows into {}", insert.values.len(), insert.table);
    }
    // ... handle other statement types
}
```

## Limitations and Future Improvements

Current limitations:
- No support for JOINs or subqueries
- ORDER BY is parsed but not executed
- Limited aggregate function support
- No arithmetic expressions in WHERE clauses
- BETWEEN operator not implemented
- No support for complex LIKE patterns

Planned improvements:
- Full ORDER BY execution
- Aggregate functions (COUNT, SUM, AVG, etc.)
- JOIN operations
- Subquery support
- Indexing for efficient WHERE clauses
- Transaction integration
- More advanced SQL features

## Performance Considerations

The current implementation:
- Uses a simple key-value storage model
- Performs full table scans for WHERE clauses
- Stores row data as serialized maps
- Uses reference counting for memory efficiency

For production use, consider:
- Adding indexes for frequently queried columns
- Implementing query optimization
- Using more efficient serialization formats
- Adding connection pooling and caching

## Examples

See the `examples/` directory for complete working examples:
- `sql_parser_demo.rs` - Basic parser demonstration
- `complete_sql_demo.rs` - Full parser and executor demo
