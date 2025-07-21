//! TegDB Test Suite Organization
//!
//! This test suite is organized into the following categories:
//!
//! ## Unit Tests (`unit/`)
//! Tests for individual components and modules:
//! - `catalog_tests.rs` - Schema catalog management
//! - `planner_tests.rs` - Query planning and optimization
//! - `storage_format_tests.rs` - Data serialization/deserialization
//! - `protocol_utils_tests.rs` - Storage protocol parsing
//! - `lib_tests.rs` - Library-level functionality
//!
//! ## Integration Tests (`integration/`)
//! Tests for component interactions and end-to-end functionality:
//! - `database_tests.rs` - High-level database API
//! - `engine_tests.rs` - Storage engine operations
//! - `transaction_tests.rs` - Transaction management
//! - `sql_integration_tests.rs` - SQL parsing and execution
//! - `sql_parser_tests.rs` - SQL parser functionality
//! - `transaction_parsing_tests.rs` - Transaction statement parsing
//! - `explicit_transaction_integration_tests.rs` - Explicit transaction handling
//! - `planner_database_integration_test.rs` - Planner-database integration
//! - `query_processor_validation_test.rs` - Query execution validation
//! - `query_processor_acid_tests_new.rs` - ACID compliance testing
//! - `query_iterator_test.rs` - Query result iteration
//! - `read_only_transaction_test.rs` - Read-only transaction optimization
//! - `schema_persistence_test.rs` - Schema persistence across sessions
//! - `simplified_api_test.rs` - Simplified API functionality
//! - `backend_compatibility_test.rs` - Multi-backend compatibility
//! - `commit_marker_tests.rs` - Commit marker functionality
//! - `drop_table_integration_test.rs` - Table dropping integration
//!
//! ## Performance Tests (`performance/`)
//! Tests focused on performance and scalability:
//! - `high_level_api_performance_test.rs` - High-level API performance
//! - `schema_performance_test.rs` - Schema operations performance
//!
//! ## WASM Tests (`wasm/`)
//! Tests specific to WebAssembly platform:
//! - `wasm_tests.rs` - Basic WASM functionality
//! - `wasm_integration_tests.rs` - WASM integration scenarios
//!
//! ## Arithmetic Tests (`arithmetic/`)
//! Tests for arithmetic expression handling:
//! - `arithmetic_expressions_test.rs` - Basic arithmetic operations
//! - `arithmetic_edge_cases_test.rs` - Edge cases in arithmetic
//! - `arithmetic_parser_tests.rs` - Arithmetic expression parsing
//!
//! ## Test Helpers (`helpers/`)
//! Shared utilities and documentation:
//! - `test_helpers.rs` - Common test utilities
//! - `README.md` - Test helper documentation
//! - `convert_test_example.sh` - Test conversion examples
//!
//! ## Running Tests
//!
//! ### Run all tests:
//! ```bash
//! cargo test --features dev
//! ```
//!
//! ### Run specific test categories:
//! ```bash
//! # Unit tests only
//! cargo test --features dev unit
//!
//! # Integration tests only
//! cargo test --features dev integration
//!
//! # Performance tests only
//! cargo test --features dev performance
//!
//! # WASM tests only
//! cargo test --features dev wasm
//!
//! # Arithmetic tests only
//! cargo test --features dev arithmetic
//! ```
//!
//! ### Run specific test files:
//! ```bash
//! cargo test --features dev --test database_tests
//! cargo test --features dev --test engine_tests
//! ```
//!
//! ## Test Organization Principles
//!
//! 1. **Unit Tests**: Test individual components in isolation
//! 2. **Integration Tests**: Test component interactions and end-to-end workflows
//! 3. **Performance Tests**: Measure and validate performance characteristics
//! 4. **WASM Tests**: Ensure WASM-specific functionality works correctly
//! 5. **Arithmetic Tests**: Validate mathematical expression handling
//! 6. **Test Helpers**: Provide reusable utilities and documentation
//!
//! ## Adding New Tests
//!
//! When adding new tests, follow these guidelines:
//!
//! 1. **Unit Tests**: Place in `unit/` for testing individual functions or small components
//! 2. **Integration Tests**: Place in `integration/` for testing component interactions
//! 3. **Performance Tests**: Place in `performance/` for benchmarking and performance validation
//! 4. **WASM Tests**: Place in `wasm/` for WASM-specific functionality
//! 5. **Arithmetic Tests**: Place in `arithmetic/` for mathematical expression handling
//!
//! ## Test Naming Conventions
//!
//! - Use descriptive test names that explain what is being tested
//! - Group related tests in the same file
//! - Use consistent naming patterns within each category
//! - Include both positive and negative test cases
//!
//! ## Test Dependencies
//!
//! Most tests require the `dev` feature to access internal APIs:
//! ```rust
//! #[cfg(feature = "dev")]
//! ```
//!
//! Use the test helpers for multi-backend testing:
//! ```rust
//! ```

// Re-export test modules for easy access
pub mod arithmetic;
pub mod helpers;
pub mod integration;
pub mod performance;
pub mod unit;
pub mod wasm;
