//! Integration tests for component interactions and end-to-end functionality

pub mod backend_compatibility_test;
pub mod commit_marker_tests;
// Removed database_tests module
pub mod drop_table_integration_test;
pub mod engine_tests;
pub mod executor_acid_tests_new;
pub mod executor_validation_test;
pub mod explicit_transaction_integration_tests;
pub mod planner_database_integration_test;
pub mod query_iterator_test;
pub mod read_only_transaction_test;
pub mod schema_persistence_test;
pub mod simplified_api_test;
pub mod sql_integration_tests;
// Removed sql_parser_tests module
pub mod transaction_parsing_tests;
pub mod transaction_tests;
