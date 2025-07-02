# Engine vs Transaction Benchmark Fairness Improvements

## Issues Identified and Fixed

### 1. **Separate Engine Instances for Fair Comparison**

**Problem**: Using the same engine instance for both raw KV operations and transactional operations could lead to cross-contamination, where the state changes from one benchmark affect another.

**Fix**: Created separate engine instances for each benchmark category:
- `raw_engine` for direct engine operations  
- `tx_engine` for transaction-wrapped operations
- Each engine pair gets identical pre-populated data

```rust
// Separate engines prevent cross-contamination
let mut raw_engine = Engine::new(raw_path.clone()).expect("Failed to create raw engine");
let mut tx_engine = Engine::new(tx_path.clone()).expect("Failed to create tx engine");

// Both engines get identical data for fair comparison
for i in 0..100 {
    let key = format!("key{}", i);
    let value = format!("value{}", i);
    raw_engine.set(key.as_bytes(), value.as_bytes().to_vec()).unwrap();
    tx_engine.set(key.as_bytes(), value.as_bytes().to_vec()).unwrap();
}
```

**Problem**: The GET operation benchmark was creating the transaction outside the benchmark loop for transaction tests, while engine tests had no such overhead.

**Fix**: Moved transaction creation and cleanup inside the benchmark loop to measure the true cost of transactional GET operations.

```rust
// Before (unfair)
c.bench_function("transaction get", |b| {
    let tx = engine.begin_transaction(); // Created once, reused
    b.iter(|| {
        let _ = black_box(tx.get(black_box(key)));
    })
});

// After (fair)
c.bench_function("transaction get", |b| {
    b.iter(|| {
        let tx = engine.begin_transaction(); // Created each iteration
        let _ = black_box(tx.get(black_box(key)));
        drop(tx); // Explicit cleanup
    })
});
```

### 2. **Transaction Lifecycle Inconsistencies**

**Problem**: The GET operation benchmark was creating the transaction outside the benchmark loop for transaction tests, while engine tests had no such overhead.

**Fix**: Moved transaction creation and cleanup inside the benchmark loop to measure the true cost of transactional GET operations.

```rust
// Before (unfair)
c.bench_function("transaction get", |b| {
    let tx = engine.begin_transaction(); // Created once, reused
    b.iter(|| {
        let _ = black_box(tx.get(black_box(key)));
    })
});

// After (fair)
c.bench_function("transaction get", |b| {
    b.iter(|| {
        let tx = tx_engine.begin_transaction(); // Created each iteration
        let _ = black_box(tx.get(black_box(key)));
        drop(tx); // Explicit cleanup
    })
});
```

### 3. **DELETE Operation Data Consistency**

**Problem**: DELETE operations were not ensuring the key existed before attempting deletion, leading to inconsistent behavior between iterations.

**Fix**: Each DELETE benchmark now ensures the key exists before attempting deletion.

```rust
c.bench_function("engine delete", |b| {
    b.iter(|| {
        // Ensure key exists before deleting
        engine.set(black_box(key), black_box(value.to_vec())).unwrap();
        engine.del(black_box(key)).unwrap();
    })
});
```

### 3. **SCAN Operation Result Handling**

**Problem**: Engine scan was collecting results into a Vec, while transaction scan was not, making the comparison unfair.

**Fix**: Both scans now collect results consistently.

```rust
// Both now do:
let iter = scan_operation(...).unwrap();
let results: Vec<_> = iter.collect();
black_box(results);
```

### 4. **Enhanced Batch Operations**

**Added**: Mixed operation benchmarks that combine SET, GET, and DELETE operations to simulate realistic workloads.

### 5. **Comprehensive Overhead Analysis**

**Enhanced**: Added more detailed transaction overhead measurements including:
- Read-only transaction overhead
- Rollback vs commit performance
- Lifecycle comparison of write operations

### 6. **Error and Edge Case Scenarios**

**Added**: New benchmark function to test performance with:
- Non-existent key operations  
- Empty scan ranges
- Error handling paths

### 7. **Data Preparation and Warmup**

**Improved**: 
- Pre-populated databases with realistic data
- Ensured benchmark keys exist where needed
- Added warm-up data for consistent cache behavior

## Benchmark Categories

### Core Operations
- `engine set` vs `transaction set (no commit)` vs `transaction set + commit`
- `engine get` vs `transaction get` 
- `engine delete` vs `transaction delete (no commit)` vs `transaction delete + commit`
- `engine scan` vs `transaction scan`

### Batch Operations  
- Individual operations vs batch transactions (sizes: 1, 10, 100)
- Mixed workload comparisons (set + get + delete)

### Overhead Analysis
- Transaction creation/destruction costs
- Read-only transaction overhead
- Rollback vs commit performance
- Direct engine vs wrapped operations

### Edge Cases
- Non-existent key operations
- Empty scan ranges  
- Error path performance

## Key Fairness Principles Applied

1. **Consistent State**: All operations start from a known, consistent state
2. **Equal Work**: Both paths do equivalent work (e.g., both collect scan results)
3. **Realistic Scenarios**: Tests use realistic data sizes and access patterns
4. **Resource Management**: Explicit cleanup prevents resource leaks affecting subsequent tests
5. **Isolation**: Each benchmark iteration is independent and repeatable

This improved benchmark now provides a fair and comprehensive comparison between direct engine operations and transaction-wrapped operations, helping identify the true performance characteristics and overhead of the transaction layer.
