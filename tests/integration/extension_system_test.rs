mod test_helpers {
    include!("../common/test_helpers.rs");
}
use test_helpers::run_with_both_backends;

use tegdb::{
    Database, Extension, MathFunctionsExtension, Result, ScalarFunction, SqlValue,
    StringFunctionsExtension,
};

#[test]
fn test_create_extension_via_sql() -> Result<()> {
    run_with_both_backends("test_create_extension_via_sql", |db_path| {
        let mut db = Database::open(db_path)?;

        // Create extension via SQL
        db.execute("CREATE EXTENSION tegdb_string")?;

        // Verify function is available
        assert!(db.has_function("UPPER"));
        assert!(db.has_function("LOWER"));

        // Use function in SQL
        let result = db.query("SELECT UPPER('hello')")?;
        assert_eq!(result.rows()[0][0], SqlValue::Text("HELLO".to_string()));

        // Create another extension
        db.execute("CREATE EXTENSION tegdb_math")?;

        // Verify math functions are available
        assert!(db.has_function("ABS"));
        assert!(db.has_function("SQRT"));

        // Use math function
        let result = db.query("SELECT SQRT(144)")?;
        assert_eq!(result.rows()[0][0], SqlValue::Real(12.0));

        Ok(())
    })
}

#[test]
fn test_drop_extension_via_sql() -> Result<()> {
    run_with_both_backends("test_drop_extension_via_sql", |db_path| {
        let mut db = Database::open(db_path)?;

        // Create extension
        db.execute("CREATE EXTENSION tegdb_string")?;
        assert!(db.has_function("UPPER"));

        // Drop extension
        db.execute("DROP EXTENSION tegdb_string")?;

        // Verify function is no longer available
        assert!(!db.has_function("UPPER"));

        // Try to use function - should fail
        let result = db.query("SELECT UPPER('hello')");
        assert!(result.is_err());

        Ok(())
    })
}

#[test]
fn test_extension_persistence() -> Result<()> {
    run_with_both_backends("test_extension_persistence", |db_path| {
        // Phase 1: Create database and extension
        {
            let mut db = Database::open(db_path)?;
            db.execute("CREATE EXTENSION tegdb_string")?;
            db.execute("CREATE EXTENSION tegdb_math")?;

            // Verify extensions are registered
            let extensions = db.list_extensions();
            assert_eq!(extensions.len(), 2);
        }

        // Phase 2: Reopen database
        {
            let mut db = Database::open(db_path)?;

            // Extensions should be automatically loaded
            let extensions = db.list_extensions();
            assert_eq!(extensions.len(), 2);

            // Functions should be available
            assert!(db.has_function("UPPER"));
            assert!(db.has_function("ABS"));

            // Use functions
            let result = db.query("SELECT UPPER('hello'), ABS(-5)")?;
            assert_eq!(result.rows()[0][0], SqlValue::Text("HELLO".to_string()));
            assert_eq!(result.rows()[0][1], SqlValue::Integer(5));
        }

        Ok(())
    })
}

#[test]
fn test_extension_error_cases() -> Result<()> {
    run_with_both_backends("test_extension_error_cases", |db_path| {
        let mut db = Database::open(db_path)?;

        // Try to create non-existent extension
        let result = db.execute("CREATE EXTENSION non_existent_extension");
        assert!(result.is_err());

        // Try to drop non-existent extension
        let result = db.execute("DROP EXTENSION non_existent_extension");
        assert!(result.is_err());

        // Try to create extension twice
        db.execute("CREATE EXTENSION tegdb_string")?;
        let result = db.execute("CREATE EXTENSION tegdb_string");
        assert!(result.is_err());

        Ok(())
    })
}

#[test]
fn test_list_extensions() -> Result<()> {
    run_with_both_backends("test_list_extensions", |db_path| {
        let mut db = Database::open(db_path)?;

        // Initially no extensions
        let extensions = db.list_extensions();
        assert_eq!(extensions.len(), 0);

        // Add extensions
        db.execute("CREATE EXTENSION tegdb_string")?;
        db.execute("CREATE EXTENSION tegdb_math")?;

        // List extensions
        let extensions = db.list_extensions();
        assert_eq!(extensions.len(), 2);

        // Verify extension names and versions
        let mut names: Vec<&str> = extensions.iter().map(|(n, _)| *n).collect();
        names.sort();
        assert!(names.contains(&"tegdb_string"));
        assert!(names.contains(&"tegdb_math"));

        Ok(())
    })
}

#[test]
fn test_register_extension_via_rust_api() -> Result<()> {
    run_with_both_backends("test_register_extension_via_rust_api", |db_path| {
        let mut db = Database::open(db_path)?;

        // Register extension via Rust API
        db.register_extension(Box::new(StringFunctionsExtension))?;

        // Verify extension is registered
        let extensions = db.list_extensions();
        assert_eq!(extensions.len(), 1);
        assert_eq!(extensions[0].0, "tegdb_string");

        // Verify functions are available
        assert!(db.has_function("UPPER"));
        assert!(db.has_function("LOWER"));

        // Use function
        let result = db.query("SELECT UPPER('hello')")?;
        assert_eq!(result.rows()[0][0], SqlValue::Text("HELLO".to_string()));

        Ok(())
    })
}

#[test]
fn test_unregister_extension_via_rust_api() -> Result<()> {
    run_with_both_backends("test_unregister_extension_via_rust_api", |db_path| {
        let mut db = Database::open(db_path)?;

        // Register extension
        db.register_extension(Box::new(StringFunctionsExtension))?;
        assert!(db.has_function("UPPER"));

        // Unregister extension
        db.unregister_extension("tegdb_string")?;

        // Verify function is no longer available
        assert!(!db.has_function("UPPER"));

        // Try to use function - should fail
        let result = db.query("SELECT UPPER('hello')");
        assert!(result.is_err());

        Ok(())
    })
}

#[test]
fn test_custom_extension_registration() -> Result<()> {
    run_with_both_backends("test_custom_extension_registration", |db_path| {
        let mut db = Database::open(db_path)?;

        // Define a custom function
        struct DoubleFunction;

        impl ScalarFunction for DoubleFunction {
            fn name(&self) -> &'static str {
                "DOUBLE"
            }

            fn signature(&self) -> tegdb::FunctionSignature {
                tegdb::FunctionSignature::new(vec![tegdb::ArgType::Numeric], tegdb::DataType::Real)
            }

            fn execute(&self, args: &[SqlValue]) -> std::result::Result<SqlValue, String> {
                match &args[0] {
                    SqlValue::Integer(i) => Ok(SqlValue::Integer(i * 2)),
                    SqlValue::Real(r) => Ok(SqlValue::Real(r * 2.0)),
                    SqlValue::Null => Ok(SqlValue::Null),
                    _ => Err("Expected numeric argument".to_string()),
                }
            }
        }

        // Define a custom extension
        struct MyCustomExtension;

        impl Extension for MyCustomExtension {
            fn name(&self) -> &'static str {
                "my_custom_extension"
            }

            fn version(&self) -> &'static str {
                "1.0.0"
            }

            fn scalar_functions(&self) -> Vec<Box<dyn ScalarFunction>> {
                vec![Box::new(DoubleFunction)]
            }
        }

        // Register custom extension
        db.register_extension(Box::new(MyCustomExtension))?;

        // Verify extension is registered
        let extensions = db.list_extensions();
        assert_eq!(extensions.len(), 1);
        assert_eq!(extensions[0].0, "my_custom_extension");

        // Verify function is available
        assert!(db.has_function("DOUBLE"));

        // Use function via SQL
        let result = db.query("SELECT DOUBLE(21)")?;
        assert_eq!(result.rows()[0][0], SqlValue::Integer(42));

        // Use function via Rust API
        let result = db.call_function("DOUBLE", &[SqlValue::Integer(10)])?;
        assert_eq!(result, SqlValue::Integer(20));

        Ok(())
    })
}

#[test]
fn test_multiple_extensions() -> Result<()> {
    run_with_both_backends("test_multiple_extensions", |db_path| {
        let mut db = Database::open(db_path)?;

        // Register multiple extensions
        db.register_extension(Box::new(StringFunctionsExtension))?;
        db.register_extension(Box::new(MathFunctionsExtension))?;

        // Verify both are registered
        let extensions = db.list_extensions();
        assert_eq!(extensions.len(), 2);

        // Verify functions from both extensions are available
        assert!(db.has_function("UPPER")); // From string extension
        assert!(db.has_function("ABS")); // From math extension

        // Use functions from both extensions in one query
        let result = db.query("SELECT UPPER('hello'), ABS(-5)")?;
        assert_eq!(result.rows()[0][0], SqlValue::Text("HELLO".to_string()));
        assert_eq!(result.rows()[0][1], SqlValue::Integer(5));

        Ok(())
    })
}

#[test]
fn test_extension_function_availability() -> Result<()> {
    run_with_both_backends("test_extension_function_availability", |db_path| {
        let mut db = Database::open(db_path)?;

        // No extensions initially
        assert!(!db.has_function("UPPER"));
        assert!(!db.has_function("ABS"));

        // Register string extension
        db.execute("CREATE EXTENSION tegdb_string")?;

        // String functions should be available
        assert!(db.has_function("UPPER"));
        assert!(db.has_function("LOWER"));
        assert!(db.has_function("LENGTH"));
        assert!(db.has_function("TRIM"));
        assert!(db.has_function("SUBSTR"));
        assert!(db.has_function("REPLACE"));
        assert!(db.has_function("CONCAT"));
        assert!(db.has_function("REVERSE"));

        // Math functions should not be available yet
        assert!(!db.has_function("ABS"));

        // Register math extension
        db.execute("CREATE EXTENSION tegdb_math")?;

        // Math functions should now be available
        assert!(db.has_function("ABS"));
        assert!(db.has_function("CEIL"));
        assert!(db.has_function("FLOOR"));
        assert!(db.has_function("ROUND"));
        assert!(db.has_function("SQRT"));
        assert!(db.has_function("POW"));
        assert!(db.has_function("MOD"));
        assert!(db.has_function("SIGN"));

        Ok(())
    })
}
