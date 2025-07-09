//! Test helpers for running tests with different backends

use tegdb::Result;

#[cfg(not(target_arch = "wasm32"))]
use tempfile::NamedTempFile;

/// Run a test function with both file and browser backends
///
/// This function takes a test closure and runs it with different database paths
/// to test both the file backend (native) and browser backend (WASM).
///
/// # Arguments
/// * `test_name` - Name of the test for logging
/// * `test_fn` - The test function to run with each backend
///
/// # Example
/// ```
/// use tests::test_helpers::run_with_both_backends;
///
/// run_with_both_backends("my_test", |db_path| {
///     let mut db = Database::open(db_path)?;
///     // ... test logic ...
///     Ok(())
/// });
/// ```
pub fn run_with_both_backends<F>(test_name: &str, test_fn: F) -> Result<()>
where
    F: Fn(&str) -> Result<()>,
{
    // Test with file backend (native)
    #[cfg(not(target_arch = "wasm32"))]
    {
        println!("Running {test_name} with file backend");
        let temp_file = NamedTempFile::new().expect("Failed to create temp file");
        let db_path = format!("file://{}", temp_file.path().display());
        test_fn(&db_path)?;
    }

    // Test with browser backend (WASM) - only if we're targeting WASM
    #[cfg(target_arch = "wasm32")]
    {
        println!("Running {} with browser backend", test_name);
        let browser_path = "localstorage://test_db";
        test_fn(browser_path)?;
    }

    #[cfg(not(target_arch = "wasm32"))]
    {
        println!("Skipping browser backend test for {test_name} (not targeting WASM)");
    }

    Ok(())
}

/// Run a test function with file backend only
///
/// This is useful when you want to test only the file backend,
/// or when the test logic is specific to file-based storage.
#[allow(dead_code)]
pub fn run_with_file_backend<F>(_test_name: &str, _test_fn: F) -> Result<()>
where
    F: Fn(&str) -> Result<()>,
{
    #[cfg(not(target_arch = "wasm32"))]
    {
        println!("Running {_test_name} with file backend");
        let temp_file = NamedTempFile::new().expect("Failed to create temp file");
        let db_path = format!("file://{}", temp_file.path().display());
        _test_fn(&db_path)
    }

    #[cfg(target_arch = "wasm32")]
    {
        println!("File backend not available on WASM target");
        Ok(())
    }
}

/// Run a test function with browser backend only
///
/// This is useful when you want to test only the browser backend,
/// or when the test logic is specific to browser-based storage.
#[cfg(target_arch = "wasm32")]
#[allow(dead_code)]
pub fn run_with_browser_backend<F>(test_name: &str, test_fn: F) -> Result<()>
where
    F: Fn(&str) -> Result<()>,
{
    println!("Running {} with browser backend", test_name);
    let browser_path = "localstorage://test_db";
    test_fn(browser_path)
}

/// Create a database path for testing with the specified backend
#[allow(dead_code)]
pub fn create_test_db_path(backend: &str) -> String {
    match backend {
        "file" => {
            #[cfg(not(target_arch = "wasm32"))]
            {
                let temp_file = NamedTempFile::new().expect("Failed to create temp file");
                format!("file://{}", temp_file.path().display())
            }
            #[cfg(target_arch = "wasm32")]
            {
                "localstorage://test_db".to_string()
            }
        }
        "browser" => "browser://test_db".to_string(),
        "localstorage" => "localstorage://test_db".to_string(),
        "indexeddb" => "indexeddb://test_db".to_string(),
        _ => panic!("Unsupported backend: {backend}"),
    }
}

/// Run a test function with a specific backend
#[allow(dead_code)]
pub fn run_with_backend<F>(test_name: &str, backend: &str, test_fn: F) -> Result<()>
where
    F: Fn(&str) -> Result<()>,
{
    println!("Running {test_name} with {backend} backend");
    let db_path = create_test_db_path(backend);
    test_fn(&db_path)
}
