#![allow(clippy::duplicate_mod)]

//! Test helpers for running TegDB tests on native backends

use tegdb::Result;
use tempfile::NamedTempFile;

/// Run a test function against the native file backend.
///
/// # Arguments
/// * `test_name` - Name of the test for logging
/// * `test_fn` - The test function to run with the backend
#[allow(dead_code)]
pub fn run_with_both_backends<F>(test_name: &str, test_fn: F) -> Result<()>
where
    F: Fn(&str) -> Result<()>,
{
    println!("Running {test_name} with file backend");
    let temp_file = NamedTempFile::new().expect("Failed to create temp file");
    let db_path = format!("file://{}", temp_file.path().display());
    test_fn(&db_path)
}

/// Run a test function with the file backend only.
#[allow(dead_code)]
pub fn run_with_file_backend<F>(test_name: &str, test_fn: F) -> Result<()>
where
    F: Fn(&str) -> Result<()>,
{
    run_with_both_backends(test_name, test_fn)
}

/// Create a database path for testing with the native backend.
#[allow(dead_code)]
pub fn create_test_db_path(backend: &str) -> String {
    match backend {
        "file" => {
            let temp_file = NamedTempFile::new().expect("Failed to create temp file");
            format!("file://{}", temp_file.path().display())
        }
        _ => panic!("Unsupported backend: {backend}"),
    }
}

/// Run a test function with a specific backend (currently file only).
#[allow(dead_code)]
pub fn run_with_backend<F>(test_name: &str, backend: &str, test_fn: F) -> Result<()>
where
    F: Fn(&str) -> Result<()>,
{
    if backend != "file" {
        panic!("Unsupported backend: {backend}");
    }

    run_with_both_backends(test_name, test_fn)
}
