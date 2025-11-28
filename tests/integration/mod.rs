//! Integration tests for component interactions and end-to-end functionality

pub mod backend_compatibility_test;
pub mod commit_marker_tests;
// Removed database_tests module
pub mod drop_table_integration_test;
pub mod explicit_transaction_integration_tests;
pub mod parse_embed_unit_test;
pub mod parser_insert_vector_regression;
pub mod planner_database_integration_test;
pub mod query_iterator_test;
pub mod query_processor_acid_tests_new;
pub mod query_processor_validation_test;
pub mod read_only_transaction_test;
pub mod schema_persistence_test;
pub mod simplified_api_test;
pub mod sql_integration_tests;
// Removed sql_parser_tests module
// Moved to tests/unit/: engine_tests, transaction_tests, header_version_test,
// preallocate_memory_test, preallocate_disk_test, transaction_parsing_tests
pub mod vector_search_tests;
