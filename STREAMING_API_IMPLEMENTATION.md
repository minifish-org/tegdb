# TegDB Streaming API Implementation

## Overview

The streaming API implementation provides a memory-efficient, lazy-evaluation approach to processing query results in TegDB. Instead of loading all rows into memory at once, the streaming API processes rows one at a time using Rust's iterator pattern.

## Key Components

### 1. RowIterator<'a>
A lazy iterator that yields rows on-demand:
```rust
pub struct RowIterator<'a> {
    scan_iter: Box<dyn Iterator<Item = (Vec<u8>, Vec<u8>)> + 'a>,
    schema: TableSchema,
    selected_columns: Vec<String>,
    filter: Option<Condition>,
    storage_format: StorageFormat,
    limit: Option<u64>,
    count: u64,
}
```

**Benefits:**
- **Memory Efficiency**: Only one row in memory at a time (O(1) vs O(n))
- **Early Termination**: LIMIT clauses stop processing immediately when reached
- **Lazy Filtering**: Conditions are evaluated during iteration, not after loading
- **Composable**: Standard Rust iterator methods (take, filter, collect, etc.)

### 2. StreamingResultSet<'a>
Wrapper around RowIterator with query metadata:
```rust
pub struct StreamingResultSet<'a> {
    pub columns: Vec<String>,
    pub rows: RowIterator<'a>,
}
```

**Key Methods:**
- `collect_rows()` - Convert to Vec for backward compatibility
- `take(n)` - Get first N rows
- `filter(predicate)` - Apply additional filtering

### 3. StreamingResult<'a>
High-level enum for different result types:
```rust
pub enum StreamingResult<'a> {
    Select(StreamingResultSet<'a>),
    Other(ResultSet),
}
```

## Performance Improvements

### Memory Usage
- **Before**: `Vec<Vec<SqlValue>>` - loads all rows into memory
- **After**: Iterator yields one row at a time - constant memory usage

### Query Latency
- **Before**: Wait for all rows to be processed before returning any results
- **After**: First results available immediately as they're found

### Early Termination
- **Before**: LIMIT applied after loading all matching rows
- **After**: LIMIT stops the iterator when reached, saving unnecessary work

### Filter Efficiency
- **Before**: Load all rows, then apply WHERE conditions
- **After**: WHERE conditions applied during scan, non-matching rows never fully processed

## API Usage Examples

### Basic Streaming Query
```rust
let streaming_result = executor.execute_streaming_query(
    "large_table",
    Some(&["id", "name", "value"]),
    None,
    Some(1000)
)?;

for row_result in streaming_result.rows {
    match row_result {
        Ok(row) => process_row(row),
        Err(e) => handle_error(e),
    }
}
```

### Memory-Efficient Aggregation
```rust
let stream = executor.execute_streaming_query("sales", Some(&["amount"]), None, None)?;
let total: f64 = stream.rows
    .filter_map(|row| row.ok()?.get(0)?.as_real())
    .sum();
// Processes millions of rows using constant memory!
```

### Pagination
```rust
let stream = executor.execute_streaming_query("products", None, None, None)?;
let page: Vec<_> = stream.rows
    .skip(page_size * page_num)
    .take(page_size)
    .collect();
```

### Early Termination
```rust
let stream = executor.execute_streaming_query("logs", None, Some(error_filter), None)?;
if let Some(first_error) = stream.rows.next() {
    // Found the error immediately, no need to scan entire table
    handle_first_error(first_error);
}
```

## Integration Points

### Executor Methods
- `execute_plan_streaming()` - Streaming version of execute_plan
- `execute_streaming_query()` - High-level streaming API
- `execute_table_scan_streaming()` - Internal streaming table scan

### Backward Compatibility
The streaming API maintains full backward compatibility:
- Existing `ResultSet` enum unchanged
- `StreamingResultSet::collect_rows()` converts to traditional Vec format
- Non-streaming operations (INSERT, UPDATE, DELETE) use existing code paths

## Real-World Benefits

### IoT Data Processing
- Process millions of sensor readings without running out of memory
- Early detection of anomalies without scanning entire dataset

### Log Analysis
- Stream through large log files efficiently
- Find specific events without loading entire logs

### ETL Workflows
- Transform data from source to destination in streaming fashion
- Handle datasets larger than available RAM

### Financial Reporting
- Generate reports with pagination support
- Calculate aggregates over large transaction histories

### Real-time Analytics
- Process data as it arrives, not in batches
- Immediate response to threshold violations

## Technical Architecture

### Iterator Chain
1. `Transaction::scan()` returns storage-level iterator
2. `RowIterator` wraps scan iterator with deserialization
3. Filter conditions applied during iteration
4. LIMIT enforced by iterator count
5. Selected columns extracted on-demand

### Lazy Evaluation
- Deserialization only happens when iterator is consumed
- Filter evaluation deferred until row is accessed
- Storage scan stops when iterator is dropped or limit reached

### Error Handling
- Deserialization errors propagated through `Result<Vec<SqlValue>>`
- Iterator continues on recoverable errors
- Transactional consistency maintained

## Future Enhancements

### Parallel Processing
Could be extended with `rayon` for parallel row processing:
```rust
stream.rows.par_bridge().map(process_row).collect()
```

### Streaming Joins
Extend to support streaming hash joins and merge joins for large table joins.

### Adaptive Batching
Dynamically adjust batch sizes based on memory pressure and query patterns.

### Query Plan Integration
Integrate streaming execution plans with the query planner for optimal performance.

## Conclusion

The streaming API transforms TegDB from a memory-bound to a streaming-capable database engine. This architectural change enables:

1. **Scalability**: Handle datasets larger than available RAM
2. **Responsiveness**: Immediate results for interactive queries
3. **Efficiency**: Only process data that's actually needed
4. **Flexibility**: Composable operations using Rust's iterator ecosystem

This implementation positions TegDB as suitable for modern data processing workloads where memory efficiency and low latency are critical requirements.
