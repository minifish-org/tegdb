//! Test helpers for running tests with different backends

use tegdb::Result;
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
///     let mut db = Database::open(&format!("file://{}", db_path.display()))?;
///     // ... test logic ...
///     Ok(())
/// });
/// ```
pub fn run_with_both_backends<F>(test_name: &str, test_fn: F) -> Result<()>
where
    F: Fn(&std::path::Path) -> Result<()>,
{
    // Test with file backend (native)
    println!("Running {} with file backend", test_name);
    let temp_file = NamedTempFile::new().expect("Failed to create temp file");
    let db_path = temp_file.path();
    test_fn(db_path)?;

    // Test with browser backend (WASM) - only if we're targeting WASM
    #[cfg(target_arch = "wasm32")]
    {
        println!("Running {} with browser backend", test_name);
        let browser_path = "browser://test_db";
        // For browser backend, we need to create a different path format
        // The test function will need to handle the browser:// protocol
        test_fn_with_browser_backend(browser_path, &test_fn)?;
    }

    #[cfg(not(target_arch = "wasm32"))]
    {
        println!("Skipping browser backend test for {} (not targeting WASM)", test_name);
    }

    Ok(())
}

/// Run a test function specifically with the browser backend
#[cfg(target_arch = "wasm32")]
fn test_fn_with_browser_backend<F>(browser_path: &str, test_fn: &F) -> Result<()>
where
    F: Fn(&std::path::Path) -> Result<()>,
{
    // For browser backend, we need to create a temporary path that can be converted
    // to a browser storage identifier
    let temp_path = std::path::Path::new(browser_path);
    test_fn(temp_path)
}

/// Run a test function with file backend only
/// 
/// This is useful when you want to test only the file backend,
/// or when the test logic is specific to file-based storage.
#[allow(dead_code)]
pub fn run_with_file_backend<F>(test_name: &str, test_fn: F) -> Result<()>
where
    F: Fn(&std::path::Path) -> Result<()>,
{
    println!("Running {} with file backend", test_name);
    let temp_file = NamedTempFile::new().expect("Failed to create temp file");
    let db_path = temp_file.path();
    test_fn(db_path)
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
    let browser_path = "browser://test_db";
    test_fn(browser_path)
}

/// Create a database path for testing with the specified backend
#[allow(dead_code)]
pub fn create_test_db_path(backend: &str) -> String {
    match backend {
        "file" => {
            let temp_file = NamedTempFile::new().expect("Failed to create temp file");
            format!("file://{}", temp_file.path().display())
        }
        "browser" => "browser://test_db".to_string(),
        "localstorage" => "localstorage://test_db".to_string(),
        "indexeddb" => "indexeddb://test_db".to_string(),
        _ => panic!("Unsupported backend: {}", backend),
    }
}

/// Run a test function with a specific backend
#[allow(dead_code)]
pub fn run_with_backend<F>(test_name: &str, backend: &str, test_fn: F) -> Result<()>
where
    F: Fn(&str) -> Result<()>,
{
    println!("Running {} with {} backend", test_name, backend);
    let db_path = create_test_db_path(backend);
    test_fn(&db_path)
} 