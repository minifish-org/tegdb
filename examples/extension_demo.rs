//! Extension System Demo
//!
//! This example demonstrates TegDB's extension system, which allows you to
//! add custom functions to the database.
//!
//! Run with: cargo run --example extension_demo

use tegdb::{
    ArgType, DataType, Database, Extension, FunctionSignature, MathFunctionsExtension, Result,
    ScalarFunction, SqlValue, StringFunctionsExtension,
};

fn main() -> Result<()> {
    println!("=== TegDB Extension System Demo ===\n");

    // Create a temporary database
    let db_path = std::env::temp_dir().join("extension_demo.teg");
    let _ = std::fs::remove_file(&db_path);
    let mut db = Database::open(format!("file://{}", db_path.display()))?;

    // ========================================================================
    // 1. Register Built-in Extensions
    // ========================================================================
    println!("1. Registering built-in extensions...\n");

    // Register string functions extension
    db.register_extension(Box::new(StringFunctionsExtension))?;
    println!("   ✓ Registered: tegdb_string v0.1.0");
    println!("     Functions: UPPER, LOWER, LENGTH, TRIM, LTRIM, RTRIM, SUBSTR, REPLACE, CONCAT, REVERSE\n");

    // Register math functions extension
    db.register_extension(Box::new(MathFunctionsExtension))?;
    println!("   ✓ Registered: tegdb_math v0.1.0");
    println!("     Functions: ABS, CEIL, FLOOR, ROUND, SQRT, POW, MOD, SIGN\n");

    // List all registered extensions
    println!("   Registered extensions:");
    for (name, version) in db.list_extensions() {
        println!("     - {} v{}", name, version);
    }
    println!();

    // ========================================================================
    // 2. Create a Custom Extension
    // ========================================================================
    println!("2. Creating a custom extension...\n");

    // Define a custom extension with domain-specific functions
    struct MyCustomExtension;

    impl Extension for MyCustomExtension {
        fn name(&self) -> &'static str {
            "my_custom_extension"
        }

        fn version(&self) -> &'static str {
            "1.0.0"
        }

        fn description(&self) -> Option<&'static str> {
            Some("Custom functions for demonstration")
        }

        fn scalar_functions(&self) -> Vec<Box<dyn ScalarFunction>> {
            vec![
                Box::new(DoubleFunction),
                Box::new(IsEvenFunction),
                Box::new(RepeatFunction),
            ]
        }
    }

    // DOUBLE(x) - doubles a number
    struct DoubleFunction;

    impl ScalarFunction for DoubleFunction {
        fn name(&self) -> &'static str {
            "DOUBLE"
        }

        fn signature(&self) -> FunctionSignature {
            FunctionSignature::new(vec![ArgType::Numeric], DataType::Real)
        }

        fn execute(&self, args: &[SqlValue]) -> std::result::Result<SqlValue, String> {
            match &args[0] {
                SqlValue::Integer(i) => Ok(SqlValue::Integer(i * 2)),
                SqlValue::Real(r) => Ok(SqlValue::Real(r * 2.0)),
                SqlValue::Null => Ok(SqlValue::Null),
                _ => Err("DOUBLE requires numeric argument".to_string()),
            }
        }

        fn description(&self) -> Option<&'static str> {
            Some("Double the input value")
        }
    }

    // IS_EVEN(x) - check if a number is even
    struct IsEvenFunction;

    impl ScalarFunction for IsEvenFunction {
        fn name(&self) -> &'static str {
            "IS_EVEN"
        }

        fn signature(&self) -> FunctionSignature {
            FunctionSignature::new(vec![ArgType::Exact(DataType::Integer)], DataType::Integer)
        }

        fn execute(&self, args: &[SqlValue]) -> std::result::Result<SqlValue, String> {
            match &args[0] {
                SqlValue::Integer(i) => Ok(SqlValue::Integer(if i % 2 == 0 { 1 } else { 0 })),
                SqlValue::Null => Ok(SqlValue::Null),
                _ => Err("IS_EVEN requires integer argument".to_string()),
            }
        }

        fn description(&self) -> Option<&'static str> {
            Some("Return 1 if the number is even, 0 otherwise")
        }
    }

    // REPEAT(text, n) - repeat text n times
    struct RepeatFunction;

    impl ScalarFunction for RepeatFunction {
        fn name(&self) -> &'static str {
            "REPEAT"
        }

        fn signature(&self) -> FunctionSignature {
            FunctionSignature::new(
                vec![ArgType::TextLike, ArgType::Exact(DataType::Integer)],
                DataType::Text(None),
            )
        }

        fn execute(&self, args: &[SqlValue]) -> std::result::Result<SqlValue, String> {
            let text = match &args[0] {
                SqlValue::Text(s) => s,
                SqlValue::Null => return Ok(SqlValue::Null),
                _ => return Err("REPEAT first argument must be text".to_string()),
            };

            let count = match &args[1] {
                SqlValue::Integer(n) => *n as usize,
                SqlValue::Null => return Ok(SqlValue::Null),
                _ => return Err("REPEAT second argument must be integer".to_string()),
            };

            Ok(SqlValue::Text(text.repeat(count)))
        }

        fn description(&self) -> Option<&'static str> {
            Some("Repeat text n times")
        }
    }

    // Register the custom extension
    db.register_extension(Box::new(MyCustomExtension))?;
    println!("   ✓ Registered: my_custom_extension v1.0.0");
    println!("     Functions: DOUBLE, IS_EVEN, REPEAT\n");

    // ========================================================================
    // 3. List All Available Functions
    // ========================================================================
    println!("3. All available scalar functions:");
    let functions = db.list_scalar_functions();
    for func in &functions {
        println!("     - {}", func);
    }
    println!();

    // ========================================================================
    // 4. Test Functions Using db.call_function()
    // ========================================================================
    println!("4. Testing functions via db.call_function():\n");

    // Test UPPER
    let result = db.call_function("UPPER", &[SqlValue::Text("hello world".to_string())]);
    println!("   UPPER('hello world') = {:?}", result.unwrap());

    // Test LENGTH
    let result = db.call_function("LENGTH", &[SqlValue::Text("TegDB".to_string())]);
    println!("   LENGTH('TegDB') = {:?}", result.unwrap());

    // Test SQRT
    let result = db.call_function("SQRT", &[SqlValue::Integer(144)]);
    println!("   SQRT(144) = {:?}", result.unwrap());

    // Test ROUND
    let result = db.call_function("ROUND", &[SqlValue::Real(1.2345), SqlValue::Integer(2)]);
    println!("   ROUND(1.2345, 2) = {:?}", result.unwrap());

    // Test custom DOUBLE
    let result = db.call_function("DOUBLE", &[SqlValue::Integer(21)]);
    println!("   DOUBLE(21) = {:?}", result.unwrap());

    // Test custom IS_EVEN
    let result = db.call_function("IS_EVEN", &[SqlValue::Integer(42)]);
    println!("   IS_EVEN(42) = {:?}", result.unwrap());

    let result = db.call_function("IS_EVEN", &[SqlValue::Integer(7)]);
    println!("   IS_EVEN(7) = {:?}", result.unwrap());

    // Test custom REPEAT
    let result = db.call_function(
        "REPEAT",
        &[SqlValue::Text("Ha".to_string()), SqlValue::Integer(3)],
    );
    println!("   REPEAT('Ha', 3) = {:?}", result.unwrap());

    // Test CONCAT (variadic)
    let result = db.call_function(
        "CONCAT",
        &[
            SqlValue::Text("Hello".to_string()),
            SqlValue::Text(", ".to_string()),
            SqlValue::Text("World".to_string()),
            SqlValue::Text("!".to_string()),
        ],
    );
    println!(
        "   CONCAT('Hello', ', ', 'World', '!') = {:?}",
        result.unwrap()
    );

    // Test REVERSE
    let result = db.call_function("REVERSE", &[SqlValue::Text("TegDB".to_string())]);
    println!("   REVERSE('TegDB') = {:?}", result.unwrap());

    println!();

    // ========================================================================
    // 5. Function Validation
    // ========================================================================
    println!("5. Testing function validation:\n");

    // Wrong number of arguments
    let result = db.call_function("UPPER", &[]);
    println!("   UPPER() with no args: {:?}", result);

    // Wrong argument type
    let result = db.call_function("UPPER", &[SqlValue::Integer(42)]);
    println!("   UPPER(42) with wrong type: {:?}", result);

    // SQRT of negative number
    let result = db.call_function("SQRT", &[SqlValue::Integer(-1)]);
    println!("   SQRT(-1): {:?}", result);

    println!();

    // ========================================================================
    // 6. Check Function Existence
    // ========================================================================
    println!("6. Checking function existence:\n");

    println!("   Has UPPER? {}", db.has_function("UPPER"));
    println!("   Has DOUBLE? {}", db.has_function("DOUBLE"));
    println!("   Has UNKNOWN? {}", db.has_function("UNKNOWN"));
    println!(
        "   Has upper (case insensitive)? {}",
        db.has_function("upper")
    );

    println!();

    // ========================================================================
    // 7. Unregister Extension
    // ========================================================================
    println!("7. Unregistering extension:\n");

    println!(
        "   Before unregister, has DOUBLE? {}",
        db.has_function("DOUBLE")
    );

    db.unregister_extension("my_custom_extension")?;
    println!("   ✓ Unregistered: my_custom_extension");

    println!(
        "   After unregister, has DOUBLE? {}",
        db.has_function("DOUBLE")
    );

    println!("\n=== Demo Complete ===");

    // Cleanup
    let _ = std::fs::remove_file(&db_path);

    Ok(())
}
