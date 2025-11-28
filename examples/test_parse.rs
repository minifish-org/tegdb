use tegdb::{
    ArgType, DataType, Database, Extension, FunctionSignature, MathFunctionsExtension, Result,
    ScalarFunction, SqlValue, StringFunctionsExtension,
};

struct MyCustomExtension;
impl Extension for MyCustomExtension {
    fn name(&self) -> &'static str {
        "my_custom_extension"
    }
    fn version(&self) -> &'static str {
        "1.0.0"
    }
    fn scalar_functions(&self) -> Vec<Box<dyn ScalarFunction>> {
        vec![
            Box::new(DoubleFunction),
            Box::new(IsEvenFunction),
            Box::new(RepeatFunction),
        ]
    }
}
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
            _ => Err("err".to_string()),
        }
    }
}
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
            _ => Err("err".to_string()),
        }
    }
}
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
            _ => return Err("err".to_string()),
        };
        let count = match &args[1] {
            SqlValue::Integer(n) => *n as usize,
            _ => return Err("err".to_string()),
        };
        Ok(SqlValue::Text(text.repeat(count)))
    }
}

fn main() -> Result<()> {
    let db_path = std::env::temp_dir().join("test_parse.teg");
    let _ = std::fs::remove_file(&db_path);
    let mut db = Database::open(format!("file://{}", db_path.display()))?;

    db.register_extension(Box::new(StringFunctionsExtension))?;
    db.register_extension(Box::new(MathFunctionsExtension))?;
    db.register_extension(Box::new(MyCustomExtension))?;

    // First 5 only
    let _ = db.call_function("UPPER", &[SqlValue::Text("hello world".to_string())]);
    let _ = db.call_function("LENGTH", &[SqlValue::Text("TegDB".to_string())]);
    let _ = db.call_function("SQRT", &[SqlValue::Integer(144)]);
    let _ = db.call_function("ROUND", &[SqlValue::Real(1.2345), SqlValue::Integer(2)]);
    let _ = db.call_function("DOUBLE", &[SqlValue::Integer(21)]);

    let sql = "CREATE TABLE test (id INTEGER PRIMARY KEY)";
    match db.execute(sql) {
        Ok(_) => println!("5 calls: OK"),
        Err(e) => println!("5 calls: Error {:?}", e),
    }

    // 6th call
    let _ = db.call_function("IS_EVEN", &[SqlValue::Integer(42)]);
    let sql = "SELECT 1";
    match db.query(sql) {
        Ok(_) => println!("6 calls: OK"),
        Err(e) => println!("6 calls: Error {:?}", e),
    }

    let _ = std::fs::remove_file(&db_path);
    Ok(())
}
