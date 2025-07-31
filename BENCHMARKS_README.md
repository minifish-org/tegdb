# 🚀 Vector Search Benchmarks

This document describes the comprehensive benchmark suite for TegDB's vector search features, covering all the new functionality from Steps 1-6 of the vector search implementation.

## 📊 Benchmark Overview

The benchmark suite consists of two main benchmark files:

1. **`vector_search_benchmark.rs`** - Comprehensive SQL-level benchmarks
2. **`vector_index_performance_benchmark.rs`** - Low-level vector indexing performance

## 🎯 Benchmark Coverage

### Step 1: Expression Framework ✅
- **Arithmetic expressions**: `value * 2 + 10`
- **Function calls in expressions**: `ABS(value - 5000)`
- **Complex expressions**: `(value * 2 + 10) / 3`
- **Expressions in WHERE clause**: `WHERE value * 2 > 10000`
- **Expressions in ORDER BY**: `ORDER BY value * 2 DESC`

### Step 2: Secondary Index Support ✅
- **Index scan performance**: Queries using secondary indexes
- **Index vs table scan comparison**: Performance difference with/without indexes
- **Multi-condition index usage**: Complex WHERE clauses with indexes
- **Index creation and maintenance**: CREATE/DROP INDEX operations

### Step 3: ORDER BY Support ✅
- **ASC/DESC ordering**: Both ascending and descending sorts
- **ORDER BY on indexed columns**: Performance with index support
- **ORDER BY on non-indexed columns**: Baseline performance
- **ORDER BY with vector similarity**: `ORDER BY COSINE_SIMILARITY(...) DESC`

### Step 4: Vector Similarity Functions ✅
- **COSINE_SIMILARITY(vec1, vec2)**: Cosine similarity calculation
- **EUCLIDEAN_DISTANCE(vec1, vec2)**: Euclidean distance calculation
- **DOT_PRODUCT(vec1, vec2)**: Dot product calculation
- **L2_NORMALIZE(vec)**: Vector normalization

### Step 5: Vector Search Operations ✅
- **K-NN queries**: `ORDER BY COSINE_SIMILARITY(...) DESC LIMIT 10`
- **Similarity thresholds**: `WHERE COSINE_SIMILARITY(...) > 0.8`
- **Range queries**: `WHERE EUCLIDEAN_DISTANCE(...) < 0.5`

### Step 6: Vector Indexing ✅
- **HNSW (Hierarchical Navigable Small World)**: Approximate nearest neighbor search
- **IVF (Inverted File Index)**: Clustering-based search
- **LSH (Locality Sensitive Hashing)**: High-dimensional similarity search
- **Performance comparison**: Head-to-head index type comparison
- **Dimension scaling**: Performance across different vector dimensions

## 🏃‍♂️ Running Benchmarks

### Quick Start
```bash
# Run all vector search benchmarks
./run_vector_benchmarks.sh

# Or run individual benchmarks
cargo bench --bench vector_search_benchmark
cargo bench --bench vector_index_performance_benchmark
```

### Detailed Benchmark Commands

#### Vector Search Benchmarks
```bash
# Run all vector search benchmarks
cargo bench --bench vector_search_benchmark

# Run specific benchmark groups
cargo bench --bench vector_search_benchmark -- "Vector Similarity Functions"
cargo bench --bench vector_search_benchmark -- "Vector Search Operations"
cargo bench --bench vector_search_benchmark -- "Aggregate Functions"
cargo bench --bench vector_search_benchmark -- "Secondary Indexes"
cargo bench --bench vector_search_benchmark -- "ORDER BY Operations"
cargo bench --bench vector_search_benchmark -- "Expression Framework"
cargo bench --bench vector_search_benchmark -- "Comprehensive Vector Operations"
```

#### Vector Index Performance Benchmarks
```bash
# Run all vector indexing benchmarks
cargo bench --bench vector_index_performance_benchmark

# Run specific index type benchmarks
cargo bench --bench vector_index_performance_benchmark -- "HNSW Index Performance"
cargo bench --bench vector_index_performance_benchmark -- "IVF Index Performance"
cargo bench --bench vector_index_performance_benchmark -- "LSH Index Performance"
cargo bench --bench vector_index_performance_benchmark -- "Index Type Comparison"
cargo bench --bench vector_index_performance_benchmark -- "Vector Dimension Scaling"
```

### Benchmark Options
```bash
# Run with custom parameters
cargo bench --bench vector_search_benchmark -- --measurement-time=5 --warm-up-time=2

# Generate HTML reports
cargo bench --bench vector_search_benchmark -- --output-format=html

# Save baseline for comparison
cargo bench --bench vector_search_benchmark -- --save-baseline=my_baseline

# Compare against baseline
cargo bench --bench vector_search_benchmark -- --baseline=my_baseline
```

## 📈 Performance Expectations

### Vector Similarity Functions
| Function | Expected Performance | Notes |
|----------|---------------------|-------|
| COSINE_SIMILARITY | ~1-5 μs | Most computationally intensive |
| EUCLIDEAN_DISTANCE | ~1-5 μs | Similar to cosine similarity |
| DOT_PRODUCT | ~0.5-2 μs | Fastest vector operation |
| L2_NORMALIZE | ~1-3 μs | Requires square root calculation |

### Vector Search Operations
| Operation | Expected Performance | Notes |
|-----------|---------------------|-------|
| K-NN queries | Variable | Depends on dataset size and index usage |
| Similarity thresholds | Faster than table scans | Should benefit from vector indexing |
| Range queries | Faster than table scans | Should benefit from vector indexing |

### Index Performance Comparison
| Index Type | Build Time | Search Time | Memory Usage | Best For |
|------------|------------|-------------|--------------|----------|
| HNSW | Medium | Fast | High | Approximate nearest neighbor |
| IVF | Fast | Medium | Medium | Clustering-based search |
| LSH | Fast | Fast | Low | High-dimensional similarity |

### Secondary Indexes
| Operation | Expected Performance | Notes |
|-----------|---------------------|-------|
| Index scans | 10-100x faster than table scans | Depends on selectivity |
| Multi-condition queries | Faster with index intersection | Multiple indexes can be used |
| Index maintenance | Minimal overhead | Indexes updated automatically |

### ORDER BY Operations
| Scenario | Expected Performance | Notes |
|----------|---------------------|-------|
| Indexed column ordering | Fast | Can use index for sorting |
| Non-indexed column ordering | Slower | Requires full sort |
| Vector similarity ordering | Variable | Depends on vector indexing |

## 🔧 Benchmark Configuration

### Test Data Generation
- **Vector dimensions**: 3D for SQL benchmarks, 64-512D for index benchmarks
- **Dataset sizes**: 1K-10K rows for SQL benchmarks, 100-5K vectors for index benchmarks
- **Vector normalization**: All test vectors are L2-normalized for consistency

### Benchmark Parameters
- **Measurement time**: 5 seconds per benchmark (configurable)
- **Warm-up time**: 2 seconds per benchmark (configurable)
- **Sample size**: 100 measurements per benchmark
- **Confidence level**: 95%

## 📊 Interpreting Results

### Performance Metrics
- **Mean time**: Average execution time
- **Standard deviation**: Variability in performance
- **Confidence intervals**: Statistical significance of results
- **Throughput**: Operations per second

### Key Performance Indicators
1. **Vector similarity function speed**: Should be in microseconds
2. **Index vs table scan ratio**: Indexes should be significantly faster
3. **Scaling behavior**: Performance should scale reasonably with dataset size
4. **Memory usage**: Should be reasonable for the dataset size

### Common Performance Patterns
- **Vector operations**: Should scale linearly with vector dimension
- **Index searches**: Should scale logarithmically with dataset size
- **Table scans**: Should scale linearly with dataset size
- **ORDER BY**: Should scale with dataset size and sort complexity

## 🐛 Troubleshooting

### Common Issues
1. **Benchmark fails to compile**: Check that all vector search features are implemented
2. **Out of memory**: Reduce dataset size or vector dimensions
3. **Slow benchmarks**: Increase measurement time or reduce sample size
4. **Inconsistent results**: Check for system load or thermal throttling

### Debug Commands
```bash
# Run with verbose output
cargo bench --bench vector_search_benchmark -- --verbose

# Run single benchmark for debugging
cargo bench --bench vector_search_benchmark -- "COSINE_SIMILARITY" --verbose

# Check benchmark compilation
cargo check --benches
```

## 📝 Adding New Benchmarks

### Adding to Existing Benchmark Files
1. Add new benchmark function to the appropriate file
2. Follow the existing naming convention
3. Include proper setup and cleanup
4. Add to the criterion_group! macro

### Creating New Benchmark Files
1. Create new file in `benches/` directory
2. Add benchmark configuration to `Cargo.toml`
3. Follow the existing structure and patterns
4. Include comprehensive documentation

### Benchmark Best Practices
- Use `black_box()` to prevent compiler optimization
- Clean up temporary files and databases
- Use realistic test data sizes
- Include both positive and negative test cases
- Document expected performance characteristics

## 🔗 Related Documentation

- [Vector Search Implementation](NEXT_STEPS_VECTOR_SEARCH.md)
- [TegDB Architecture](README.md)
- [Contributing Guidelines](CONTRIBUTING.md)
- [Criterion.rs Documentation](https://bheisler.github.io/criterion.rs/book/)

## 📞 Support

For questions about the benchmarks or performance issues:
1. Check the troubleshooting section above
2. Review the existing test cases in `tests/integration/vector_search_tests.rs`
3. Consult the vector search implementation documentation
4. Open an issue on the project repository 