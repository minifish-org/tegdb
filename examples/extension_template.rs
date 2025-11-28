//! Extension Template - How to Create a Loadable Extension
//!
//! This example shows how to create a TegDB extension that can be loaded
//! as a dynamic library via `CREATE EXTENSION` SQL command.
//!
//! To use this as a template:
//! 1. Create a new Rust library project: `cargo new --lib my_extension`
//! 2. Copy this code structure
//! 3. Configure Cargo.toml with `crate-type = ["cdylib"]`
//! 4. Build with `cargo build --release`
//! 5. Place the .so/.dylib/.dll in an extension search path
//! 6. Load with `CREATE EXTENSION my_extension;`
//!
//! Note: This is a template/example. To actually build a loadable extension,
//! you need to create a separate crate with cdylib crate-type.

use tegdb::{
    ArgType, DataType, Extension, ExtensionWrapper, FunctionSignature, ScalarFunction, SqlValue,
};

// Example: A function that doubles a number
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

// Example: A function that repeats text
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

// Define the extension
struct MyTemplateExtension;

impl Extension for MyTemplateExtension {
    fn name(&self) -> &'static str {
        "my_template_extension"
    }

    fn version(&self) -> &'static str {
        "1.0.0"
    }

    fn description(&self) -> Option<&'static str> {
        Some("Template extension demonstrating how to create loadable extensions")
    }

    fn scalar_functions(&self) -> Vec<Box<dyn ScalarFunction>> {
        vec![Box::new(DoubleFunction), Box::new(RepeatFunction)]
    }
}

// Export the extension creation function
// This is the entry point that TegDB will call when loading the extension
#[no_mangle]
pub extern "C" fn create_extension() -> *mut ExtensionWrapper {
    Box::into_raw(Box::new(ExtensionWrapper {
        extension: Box::new(MyTemplateExtension),
    }))
}

// Example usage (for testing - this won't work as a standalone example
// since it needs to be compiled as a cdylib)
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_double_function() {
        let func = DoubleFunction;
        assert_eq!(
            func.execute(&[SqlValue::Integer(21)]).unwrap(),
            SqlValue::Integer(42)
        );
        assert_eq!(
            func.execute(&[SqlValue::Real(2.5)]).unwrap(),
            SqlValue::Real(5.0)
        );
    }

    #[test]
    fn test_repeat_function() {
        let func = RepeatFunction;
        assert_eq!(
            func.execute(&[SqlValue::Text("Ha".to_string()), SqlValue::Integer(3)])
                .unwrap(),
            SqlValue::Text("HaHaHa".to_string())
        );
    }
}

// Main function for the example - this file is a template showing how to create extensions
fn main() {
    println!("Extension Template Example");
    println!("=========================");
    println!();
    println!("This file demonstrates how to create a loadable TegDB extension.");
    println!("To use this as a template:");
    println!();
    println!("1. Create a new Rust library project: cargo new --lib my_extension");
    println!("2. Copy this code structure");
    println!("3. Configure Cargo.toml with: [lib] crate-type = [\"cdylib\"]");
    println!("4. Build with: cargo build --release");
    println!("5. Place the .so/.dylib/.dll in an extension search path");
    println!("6. Load with: CREATE EXTENSION my_extension;");
    println!();
    println!("See README.md for more details on extension development.");
}
