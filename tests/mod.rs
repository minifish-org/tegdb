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
//! - `vector_search_tests.rs` - Vector search and similarity functionality
//!
//! ## Performance Tests (`performance/`)
//! Tests focused on performance and scalability:
//! - `high_level_api_performance_test.rs` - High-level API performance
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
//! cargo test
//! ```
//!
//! ### Run specific test categories:
//! ```bash
//! # Unit tests only
//! cargo test unit
//!
//! # Integration tests only
//! cargo test integration
//!
//! # Performance tests only
//! cargo test performance
//!
//! # Arithmetic tests only
//! cargo test arithmetic
//! ```
//!
//! ### Run specific test files:
//! ```bash
//! cargo test --test database_tests
//! cargo test --test engine_tests
//! ```
//!
//! ## Test Organization Principles
//!
//! 1. **Unit Tests**: Test individual components in isolation
//! 2. **Integration Tests**: Test component interactions and end-to-end workflows
//! 3. **Performance Tests**: Measure and validate performance characteristics
//! 4. **Arithmetic Tests**: Validate mathematical expression handling
//! 5. **Vector Tests**: Validate vector search and similarity functionality
//! 6. **Test Helpers**: Provide reusable utilities and documentation
//!
//! ## Adding New Tests
//!
//! When adding new tests, follow these guidelines:
//!
//! 1. **Unit Tests**: Place in `unit/` for testing individual functions or small components
//! 2. **Integration Tests**: Place in `integration/` for testing component interactions
//! 3. **Performance Tests**: Place in `performance/` for benchmarking and performance validation
//! 4. **Arithmetic Tests**: Place in `arithmetic/` for mathematical expression handling
//! 5. **Vector Tests**: Place in `integration/` for vector search and similarity functionality
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
//! All low-level APIs are now always available for tests and examples.
//!
//! Use the test helpers for consistent file-backed testing.

// Re-export test modules for easy access
pub mod arithmetic;
pub mod integration;
pub mod performance;
pub mod unit;
