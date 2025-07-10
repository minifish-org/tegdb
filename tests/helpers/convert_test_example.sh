#!/bin/bash

# Example script showing how to convert existing tests to use both backends
# This script demonstrates the conversion process

echo "=== Converting Existing Tests to Use Both Backends ==="
echo ""

echo "1. Create test_helpers.rs in your test file:"
echo "   mod test_helpers;"
echo "   use test_helpers::run_with_both_backends;"
echo ""

echo "2. Replace this pattern:"
echo "   #[test]"
echo "   fn test_something() -> Result<()> {"
echo "       let temp_file = NamedTempFile::new().expect(\"Failed to create temp file\");"
echo "       let db_path = temp_file.path();"
echo "       let mut db = Database::open(&format!(\"file://{}\", db_path.display()))?;"
echo "       // test logic..."
echo "       Ok(())"
echo "   }"
echo ""

echo "3. With this pattern:"
echo "   #[test]"
echo "   fn test_something() -> Result<()> {"
echo "       run_with_both_backends(\"test_something\", |db_path| {"
echo "           let mut db = Database::open(&format!(\"file://{}\", db_path.display()))?;"
echo "           // test logic..."
echo "           Ok(())"
echo "       })"
echo "   }"
echo ""

echo "4. Benefits:"
echo "   - Test automatically runs with file backend (native)"
echo "   - Test automatically runs with browser backend (WASM) when targeting WASM"
echo "   - No code duplication"
echo "   - Easy to maintain"
echo ""

echo "5. Available helper functions:"
echo "   - run_with_both_backends() - Run with both backends"
echo "   - run_with_file_backend() - Run with file backend only"
echo "   - run_with_browser_backend() - Run with browser backend only"
echo "   - run_with_backend() - Run with specific backend"
echo ""

echo "6. Example conversion:"
echo "   See tests/backend_compatibility_test.rs for working examples"
echo ""

echo "=== End of Conversion Guide ===" 