# TegDB vs SQLite Performance Comparison

## Benchmark Results Summary

This document presents the corrected performance comparison between TegDB and SQLite after fixing the O(N²) scaling issue caused by data accumulation between benchmarks.

### Corrected Benchmark Results

| Operation | TegDB Time (µs) | SQLite Time (µs) | TegDB Advantage |
|-----------|-----------------|------------------|-----------------|
| **INSERT** | 7.66 | 33.95 | **4.4x faster** |
| **SELECT** | 2.62 | 2.57 | 1.02x slower |
| **SELECT WHERE** | 2.38 | 2.65 | **1.1x faster** |
| **UPDATE** | 4.91 | 3.57 | 1.4x slower |
| **TRANSACTION** | 7.50 | 32.62 | **4.3x faster** |
| **DELETE** | 1.16 | 3.43 | **3.0x faster** |

### Key Findings

#### TegDB Strengths
1. **Write Operations**: TegDB significantly outperforms SQLite in write-heavy operations:
   - INSERT: 4.4x faster (7.66µs vs 33.95µs)
   - TRANSACTION: 4.3x faster (7.50µs vs 32.62µs)
   - DELETE: 3.0x faster (1.16µs vs 3.43µs)

2. **Conditional Queries**: TegDB is slightly faster for WHERE clauses:
   - SELECT WHERE: 1.1x faster (2.38µs vs 2.65µs)

#### SQLite Strengths
1. **Read Operations**: SQLite has slight advantages in pure read operations:
   - SELECT: 1.02x faster (2.57µs vs 2.62µs) - essentially equivalent
   - UPDATE: 1.4x faster (3.57µs vs 4.91µs)

### Technical Analysis

#### Resolved O(N²) Issue
- **Problem**: Original benchmark had data accumulation between tests, causing O(N²) scaling
- **Solution**: Added cleanup (`DELETE FROM benchmark_test WHERE id != 1`) between each operation
- **Result**: All operations now run in microseconds instead of seconds

#### Performance Characteristics
1. **TegDB**: Optimized for write throughput with efficient in-memory operations
2. **SQLite**: Mature with optimized query execution, especially for complex reads

#### Sync (fsync) Impact
- Previous testing showed that enabling sync in TegDB causes 300-400x slowdown
- This benchmark runs with sync disabled for fair comparison with SQLite's default settings

### Benchmark Design Improvements

#### Fixed Issues
1. **Data Accumulation**: Prevented by cleaning up between tests
2. **Fair Comparison**: Both databases now start each test with identical state
3. **Realistic Performance**: Results now reflect actual database performance, not benchmark artifacts

#### Current Benchmark Flow
```rust
// For each operation:
1. Setup initial state (single row with id=1)
2. Run the actual operation being benchmarked
3. Clean up (DELETE FROM benchmark_test WHERE id != 1)
4. Repeat for next iteration
```

### Conclusions

1. **TegDB excels at write-heavy workloads** with 3-4x better performance for INSERT, TRANSACTION, and DELETE operations

2. **SQLite remains competitive for read operations** with slightly better UPDATE performance and equivalent SELECT performance

3. **Both databases show excellent absolute performance** with all operations completing in microseconds

4. **The choice between TegDB and SQLite** should depend on workload characteristics:
   - Choose TegDB for write-heavy applications
   - Choose SQLite for read-heavy or complex query applications

### Future Optimization Opportunities

1. **TegDB**: Could improve UPDATE performance and add query optimization
2. **SQLite**: Already highly optimized, but could potentially improve write performance
3. **Indexing**: Adding proper indexing could significantly improve both databases' performance for WHERE clauses

---

*Benchmark conducted with Criterion.rs on macOS with corrected data cleanup to prevent O(N²) scaling artifacts.*
