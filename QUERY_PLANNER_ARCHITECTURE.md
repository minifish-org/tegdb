# Query Planner and Optimizer Architecture

This document describes the query planner and optimizer architecture in TegDB, which sits between the SQL parser and executor, similar to PostgreSQL and SQLite.

## Architecture Overview

```
┌─────────────┐    ┌─────────────┐    ┌─────────────┐    ┌─────────────┐
│   SQL Text  │ -> │   Parser    │ -> │   Planner   │ -> │  Executor   │
└─────────────┘    └─────────────┘    └─────────────┘    └─────────────┘
       │                   │                   │                   │
       │                   │                   │                   │
 "SELECT..."         Statement         ExecutionPlan        ResultSet
```

### Components

1. **Parser**: Converts SQL text into an Abstract Syntax Tree (AST)
2. **Planner**: Analyzes the AST and generates an optimized execution plan
3. **Executor**: Executes the plan against the storage engine

## Query Planner (`src/planner.rs`)

The `QueryPlanner` is responsible for:
- Taking parsed SQL statements
- Analyzing table schemas and statistics
- Generating optimized execution plans
- Choosing the best plan based on cost estimation

### Key Features

#### Cost-Based Optimization
The planner uses cost estimation to choose between different execution strategies:

```rust
pub struct Cost {
    pub io_cost: f64,      // Estimated I/O operations
    pub cpu_cost: f64,     // Estimated CPU operations
    pub memory_cost: f64,  // Estimated memory usage
}
```

#### Table Statistics
Statistics drive optimization decisions:

```rust
pub struct TableStatistics {
    pub row_count: u64,
    pub avg_row_size: usize,
    pub column_stats: HashMap<String, ColumnStatistics>,
}
```

#### Configuration
Customizable optimization parameters:

```rust
pub struct PlannerConfig {
    pub seq_scan_cost: f64,
    pub index_scan_cost: f64,
    pub pk_lookup_cost: f64,
    pub enable_pk_optimization: bool,
    pub enable_predicate_pushdown: bool,
    pub enable_limit_pushdown: bool,
    // ... more configuration options
}
```

## Execution Plans

The planner generates different types of execution plans:

### 1. Primary Key Lookup
Most efficient for equality conditions on primary keys:

```rust
ExecutionPlan::PrimaryKeyLookup {
    table: String,
    pk_values: HashMap<String, SqlValue>,
    selected_columns: Vec<String>,
    additional_filter: Option<Condition>,
}
```

**Example**: `SELECT * FROM users WHERE id = 42`
- **Complexity**: O(1)
- **Use Case**: Direct key access using IOT (Index Organized Table) structure

### 2. Index Scan
For secondary index lookups (future enhancement):

```rust
ExecutionPlan::IndexScan {
    table: String,
    index_name: String,
    key_conditions: Vec<IndexCondition>,
    selected_columns: Vec<String>,
    filter: Option<Condition>,
    limit: Option<u64>,
}
```

### 3. Table Scan
Sequential scan with optimizations:

```rust
ExecutionPlan::TableScan {
    table: String,
    selected_columns: Vec<String>,
    filter: Option<Condition>,
    limit: Option<u64>,
    early_termination: bool,
}
```

**Example**: `SELECT * FROM users WHERE age > 30 LIMIT 10`
- **Complexity**: O(n) but with early termination
- **Optimizations**: Predicate pushdown, limit pushdown

### 4. Modification Plans
For INSERT, UPDATE, DELETE operations:

```rust
ExecutionPlan::Insert {
    table: String,
    rows: Vec<HashMap<String, SqlValue>>,
    conflict_resolution: ConflictResolution,
}

ExecutionPlan::Update {
    table: String,
    assignments: Vec<Assignment>,
    scan_plan: Box<ExecutionPlan>,
}

ExecutionPlan::Delete {
    table: String,
    scan_plan: Box<ExecutionPlan>,
}
```

## Optimization Strategies

### 1. Primary Key Optimization
Detects when a query can use direct primary key lookup:

```sql
-- Optimized to primary key lookup
SELECT name, email FROM users WHERE id = 42;

-- Cannot optimize (still uses table scan)
SELECT name, email FROM users WHERE name = 'John';
```

### 2. Predicate Pushdown
Applies filters as early as possible during scanning:

```sql
-- Filter applied during scan, not after
SELECT * FROM users WHERE age > 30;
```

### 3. Limit Pushdown
Enables early termination when LIMIT is specified:

```sql
-- Stops scanning after finding 10 rows
SELECT * FROM users LIMIT 10;
```

### 4. Condition Analysis
Intelligently handles complex WHERE clauses:

```sql
-- Uses primary key lookup + additional filter
SELECT * FROM users WHERE id = 42 AND age > 25;

-- OR conditions prevent PK optimization
SELECT * FROM users WHERE id = 42 OR name = 'John';
```

## Plan Executor (`src/plan_executor.rs`)

The `PlanExecutor` takes execution plans and executes them against the storage engine:

```rust
pub struct PlanExecutor<'a> {
    executor: Executor<'a>,
}

impl<'a> PlanExecutor<'a> {
    pub fn execute_plan(&mut self, plan: ExecutionPlan) -> Result<ResultSet> {
        match plan {
            ExecutionPlan::PrimaryKeyLookup { .. } => 
                self.execute_primary_key_lookup(..),
            ExecutionPlan::TableScan { .. } => 
                self.execute_table_scan_optimized(..),
            // ... handle other plan types
        }
    }
}
```

## Integration with Existing Code

The planner integrates with the existing TegDB architecture:

### Database Layer Integration
The high-level `Database` struct can be enhanced to use the planner:

```rust
impl Database {
    pub fn query_with_plan(&mut self, sql: &str) -> Result<QueryResult> {
        // 1. Parse SQL
        let statement = parse_sql(sql)?;
        
        // 2. Create planner
        let schemas = self.get_table_schemas();
        let planner = QueryPlanner::new(schemas);
        
        // 3. Generate plan
        let plan = planner.plan(statement)?;
        
        // 4. Execute plan
        let transaction = self.engine.begin_transaction();
        let mut plan_executor = PlanExecutor::new(transaction, schemas);
        plan_executor.execute_plan(plan)
    }
}
```

### Backward Compatibility
The planner works alongside the existing executor:
- Existing executor remains for direct SQL execution
- Planner provides an additional optimization layer
- Both can coexist during transition

## Performance Benefits

### 1. Primary Key Optimization
```
Before: O(n) table scan for WHERE id = 42
After:  O(1) direct key lookup
```

### 2. Early Termination
```
Before: Scan entire table, then apply LIMIT
After:  Stop scanning after LIMIT rows found
```

### 3. Predicate Pushdown
```
Before: Load all rows, then filter
After:  Filter during scan, reduce memory usage
```

### 4. Cost-Based Decisions
```
Before: Fixed execution strategy
After:  Choose best strategy based on data characteristics
```

## Future Enhancements

### 1. Secondary Indexes
Add support for secondary indexes to improve non-primary key queries:

```rust
// Future enhancement
ExecutionPlan::IndexScan {
    index_name: "users_email_idx",
    // ...
}
```

### 2. Join Optimization
Support for multi-table queries:

```rust
// Future enhancement
ExecutionPlan::NestedLoopJoin {
    left_plan: Box<ExecutionPlan>,
    right_plan: Box<ExecutionPlan>,
    join_condition: Condition,
}
```

### 3. Aggregate Pushdown
Optimize GROUP BY and aggregate functions:

```rust
// Future enhancement
ExecutionPlan::Aggregate {
    input_plan: Box<ExecutionPlan>,
    group_by: Vec<String>,
    aggregates: Vec<AggregateFunction>,
}
```

### 4. Statistics Collection
Automatic statistics gathering for better cost estimation:

```rust
// Future enhancement
impl Database {
    pub fn analyze_table(&mut self, table: &str) -> Result<()> {
        // Collect and update table statistics
    }
}
```

## Usage Examples

### Basic Usage
```rust
use tegdb::{QueryPlanner, ExecutionPlan, PlanExecutor};

// Create planner with schemas
let planner = QueryPlanner::new(table_schemas);

// Generate plan
let plan = planner.plan(parsed_statement)?;

// Execute plan
let mut executor = PlanExecutor::new(transaction, schemas);
let result = executor.execute_plan(plan)?;
```

### With Configuration
```rust
let config = PlannerConfig {
    enable_pk_optimization: true,
    enable_predicate_pushdown: true,
    small_table_threshold: 1000,
    ..Default::default()
};

let planner = QueryPlanner::with_config(schemas, config);
```

### With Statistics
```rust
let mut planner = QueryPlanner::new(schemas);

// Add table statistics for better optimization
planner.update_table_stats("users".to_string(), TableStatistics {
    row_count: 100000,
    avg_row_size: 256,
    column_stats: HashMap::new(),
});
```

## Testing

Run the planner demo to see the architecture in action:

```bash
cargo run --example planner_demo --features dev
```

This demonstrates:
- Query parsing and planning pipeline
- Different optimization strategies
- Plan type selection
- Performance characteristics

## Conclusion

The query planner and optimizer provide a robust foundation for query optimization in TegDB, similar to production database systems like PostgreSQL and SQLite. The modular design allows for incremental improvements and future enhancements while maintaining backward compatibility with existing code.
