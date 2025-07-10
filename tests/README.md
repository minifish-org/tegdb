# TegDB Test Suite

This directory contains the comprehensive test suite for TegDB, organized into logical categories for better maintainability and clarity.

## Directory Structure

```
tests/
├── mod.rs                    # Main test organization and documentation
├── unit/                     # Unit tests for individual components
│   ├── mod.rs
│   ├── catalog_tests.rs      # Schema catalog management
│   ├── planner_tests.rs      # Query planning and optimization
│   ├── storage_format_tests.rs # Data serialization/deserialization
│   ├── protocol_utils_tests.rs # Storage protocol parsing
│   └── lib_tests.rs          # Library-level functionality
├── integration/              # Integration tests for component interactions
│   ├── mod.rs
│   ├── database_tests.rs     # High-level database API
│   ├── engine_tests.rs       # Storage engine operations
│   ├── transaction_tests.rs  # Transaction management
│   ├── sql_integration_tests.rs # SQL parsing and execution
│   ├── sql_parser_tests.rs   # SQL parser functionality
│   ├── transaction_parsing_tests.rs # Transaction statement parsing
│   ├── explicit_transaction_integration_tests.rs # Explicit transaction handling
│   ├── planner_database_integration_test.rs # Planner-database integration
│   ├── executor_validation_test.rs # Query execution validation
│   ├── executor_acid_tests_new.rs # ACID compliance testing
│   ├── query_iterator_test.rs # Query result iteration
│   ├── read_only_transaction_test.rs # Read-only transaction optimization
│   ├── schema_persistence_test.rs # Schema persistence across sessions
│   ├── simplified_api_test.rs # Simplified API functionality
│   ├── backend_compatibility_test.rs # Multi-backend compatibility
│   ├── commit_marker_tests.rs # Commit marker functionality
│   └── drop_table_integration_test.rs # Table dropping integration
├── performance/              # Performance tests
│   ├── mod.rs
│   ├── high_level_api_performance_test.rs # High-level API performance
│   └── schema_performance_test.rs # Schema operations performance
├── wasm/                     # WASM-specific tests
│   ├── mod.rs
│   ├── wasm_tests.rs         # Basic WASM functionality
│   └── wasm_integration_tests.rs # WASM integration scenarios
├── arithmetic/               # Arithmetic expression tests
│   ├── mod.rs
│   ├── arithmetic_expressions_test.rs # Basic arithmetic operations
│   ├── arithmetic_edge_cases_test.rs # Edge cases in arithmetic
│   └── arithmetic_parser_tests.rs # Arithmetic expression parsing
└── helpers/                  # Test utilities and documentation
    ├── mod.rs
    ├── test_helpers.rs       # Common test utilities
    ├── README.md             # Test helper documentation
    └── convert_test_example.sh # Test conversion examples
```

## Running Tests

### Run All Tests
```bash
cargo test --features dev
```

### Run Specific Test Categories

#### Unit Tests
```bash
cargo test --features dev unit
```

#### Integration Tests
```bash
cargo test --features dev integration
```

#### Performance Tests
```bash
cargo test --features dev performance
```

#### WASM Tests
```bash
cargo test --features dev wasm
```

#### Arithmetic Tests
```bash
cargo test --features dev arithmetic
```

### Run Specific Test Files
```bash
# Run a specific test file
cargo test --features dev --test database_tests
cargo test --features dev --test engine_tests

# Run tests matching a pattern
cargo test --features dev --test database_tests test_database_basic_operations
```

### Run Tests with Output
```bash
# Show test output
cargo test --features dev -- --nocapture

# Run tests in parallel (default)
cargo test --features dev

# Run tests sequentially
cargo test --features dev -- --test-threads=1
```

## Test Categories

### Unit Tests (`unit/`)
Tests for individual components and modules in isolation. These tests focus on:
- Individual function behavior
- Small component functionality
- Edge cases and error conditions
- Internal API validation

### Integration Tests (`integration/`)
Tests for component interactions and end-to-end functionality. These tests focus on:
- Component interactions
- Complete workflows
- Real-world usage scenarios
- Cross-module functionality

### Performance Tests (`performance/`)
Tests focused on performance and scalability. These tests focus on:
- Performance benchmarking
- Memory usage patterns
- Scalability characteristics
- Performance regression detection

### WASM Tests (`wasm/`)
Tests specific to WebAssembly platform. These tests focus on:
- WASM-specific functionality
- Browser storage compatibility
- WASM integration scenarios
- Platform-specific features

### Arithmetic Tests (`arithmetic/`)
Tests for arithmetic expression handling. These tests focus on:
- Mathematical operations
- Expression parsing
- Edge cases in arithmetic
- Type conversions

## Test Organization Principles

1. **Separation of Concerns**: Each test category has a specific focus
2. **Maintainability**: Related tests are grouped together
3. **Discoverability**: Clear naming and organization make tests easy to find
4. **Scalability**: Structure supports adding new tests without reorganization
5. **Documentation**: Each category and file is well-documented

## Adding New Tests

### Guidelines for Test Placement

1. **Unit Tests** (`unit/`): Place here for testing individual functions or small components
2. **Integration Tests** (`integration/`): Place here for testing component interactions
3. **Performance Tests** (`performance/`): Place here for benchmarking and performance validation
4. **WASM Tests** (`wasm/`): Place here for WASM-specific functionality
5. **Arithmetic Tests** (`arithmetic/`): Place here for mathematical expression handling

### Test File Naming Conventions

- Use descriptive names that explain what is being tested
- Use snake_case for file names
- Include the word "test" in the filename
- Group related functionality in the same file

### Test Function Naming Conventions

- Use descriptive test names that explain what is being tested
- Use snake_case for function names
- Start with `test_` prefix
- Include both positive and negative test cases

## Test Dependencies

### Feature Requirements
Most tests require the `dev` feature to access internal APIs:
```rust
#[cfg(feature = "dev")]
```

### Test Helpers
Use the test helpers for multi-backend testing:
```rust
mod crate::helpers::test_helpers;
use crate::helpers::test_helpers::run_with_both_backends;
```

### Example Test Structure
```rust
#[cfg(feature = "dev")]
mod crate::helpers::test_helpers;
use crate::helpers::test_helpers::run_with_both_backends;

#[test]
fn test_my_functionality() -> Result<()> {
    run_with_both_backends("test_my_functionality", |db_path| {
        let mut db = Database::open(&format!("file://{}", db_path.display()))?;
        
        // Test logic here...
        
        Ok(())
    })
}
```

## Test Coverage

The test suite provides comprehensive coverage of:

- **Core Functionality**: Database operations, transactions, queries
- **Storage Engines**: File and browser storage backends
- **SQL Parser**: SQL statement parsing and validation
- **Query Planning**: Query optimization and execution planning
- **ACID Properties**: Transaction atomicity, consistency, isolation, durability
- **Performance**: Performance characteristics and benchmarks
- **WASM Support**: WebAssembly platform compatibility
- **Arithmetic**: Mathematical expression handling
- **Error Handling**: Error conditions and edge cases

## Continuous Integration

The test suite is designed to run in CI/CD environments:

- **Native Platforms**: All tests except WASM-specific ones
- **WASM Platforms**: All tests including WASM-specific ones
- **Performance Tests**: Run separately to avoid slowing down CI
- **Feature Flags**: Tests use appropriate feature flags for different environments

## Troubleshooting

### Common Issues

1. **Test Failures**: Check that the `dev` feature is enabled
2. **Import Errors**: Ensure test_helpers are imported correctly
3. **Backend Issues**: Verify that appropriate storage backends are available
4. **Performance Issues**: Performance tests may be slow on some systems

### Debugging Tests

```bash
# Run with verbose output
cargo test --features dev -- --nocapture --test-threads=1

# Run specific failing test
cargo test --features dev --test database_tests test_specific_function -- --nocapture
```

## Contributing

When adding new tests:

1. Follow the existing organization structure
2. Use appropriate test categories
3. Follow naming conventions
4. Include comprehensive documentation
5. Ensure tests pass on all supported platforms
6. Update this README if adding new test categories

## Test Helpers Documentation

For detailed information about test helpers and multi-backend testing, see [helpers/README.md](helpers/README.md). 