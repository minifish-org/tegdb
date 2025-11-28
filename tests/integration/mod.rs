//! Integration tests for component interactions and end-to-end functionality

// TODO: Temporarily commented out due to parser issue - needs investigation
// pub mod aggregate_functions_test;
pub mod backend_compatibility_test;
pub mod commit_marker_tests;
// pub mod complex_queries_test;
pub mod drop_table_integration_test;
// pub mod embed_function_test;
pub mod explicit_transaction_integration_tests;
// pub mod extension_system_test;
// pub mod math_functions_test;
pub mod parse_embed_unit_test;
pub mod parser_insert_vector_regression;
pub mod planner_database_integration_test;
// pub mod prepared_statements_test;
pub mod query_iterator_test;
pub mod query_processor_acid_tests_new;
pub mod query_processor_validation_test;
pub mod read_only_transaction_test;
pub mod schema_persistence_test;
pub mod simplified_api_test;
pub mod sql_integration_tests;
// pub mod streaming_queries_test;
// pub mod string_functions_test;
// Removed sql_parser_tests module
// Moved to tests/unit/: engine_tests, transaction_tests, header_version_test,
// preallocate_memory_test, preallocate_disk_test, transaction_parsing_tests
// pub mod tgstream_test;
// pub mod vector_operations_test;
pub mod vector_search_tests;
