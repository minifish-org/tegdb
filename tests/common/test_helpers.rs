/// Test helpers for running TegDB tests on native backends
///
/// This file is included directly in test files using `include!` macro
/// to avoid duplicate module issues when multiple test files need the helpers.
use tegdb::Result;
use tempfile::NamedTempFile;

/// Run a test function against the native file backend.
///
/// # Arguments
/// * `test_name` - Name of the test for logging
/// * `test_fn` - The test function to run with the backend
pub fn run_with_both_backends<F>(test_name: &str, test_fn: F) -> Result<()>
where
    F: Fn(&str) -> Result<()>,
{
    println!("Running {test_name} with file backend");
    let temp_file = NamedTempFile::new().expect("Failed to create temp file");
    let db_path = format!("file://{}", temp_file.path().display());
    test_fn(&db_path)
}

