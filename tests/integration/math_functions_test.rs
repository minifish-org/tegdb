mod test_helpers {
    include!("../common/test_helpers.rs");
}
use test_helpers::run_with_both_backends;

use tegdb::{Database, MathFunctionsExtension, Result, SqlValue};

#[test]
fn test_abs_function() -> Result<()> {
    run_with_both_backends("test_abs_function", |db_path| {
        let mut db = Database::open(db_path)?;
        db.register_extension(Box::new(MathFunctionsExtension))?;

        // Positive integer
        let result = db.query("SELECT ABS(5)")?;
        assert_eq!(result.rows()[0][0], SqlValue::Integer(5));

        // Negative integer
        let result = db.query("SELECT ABS(-5)")?;
        assert_eq!(result.rows()[0][0], SqlValue::Integer(5));

        // Zero
        let result = db.query("SELECT ABS(0)")?;
        assert_eq!(result.rows()[0][0], SqlValue::Integer(0));

        // Positive real
        let result = db.query("SELECT ABS(2.5)")?;
        assert_eq!(result.rows()[0][0], SqlValue::Real(2.5));

        // Negative real
        let result = db.query("SELECT ABS(-2.5)")?;
        assert_eq!(result.rows()[0][0], SqlValue::Real(2.5));

        // NULL handling
        let result = db.query("SELECT ABS(NULL)")?;
        assert_eq!(result.rows()[0][0], SqlValue::Null);

        Ok(())
    })
}

#[test]
fn test_ceil_function() -> Result<()> {
    run_with_both_backends("test_ceil_function", |db_path| {
        let mut db = Database::open(db_path)?;
        db.register_extension(Box::new(MathFunctionsExtension))?;

        // Positive decimal
        let result = db.query("SELECT CEIL(3.7)")?;
        assert_eq!(result.rows()[0][0], SqlValue::Integer(4));

        // Negative decimal
        let result = db.query("SELECT CEIL(-3.7)")?;
        assert_eq!(result.rows()[0][0], SqlValue::Integer(-3));

        // Already integer
        let result = db.query("SELECT CEIL(5)")?;
        assert_eq!(result.rows()[0][0], SqlValue::Integer(5));

        // Zero
        let result = db.query("SELECT CEIL(0.0)")?;
        assert_eq!(result.rows()[0][0], SqlValue::Integer(0));

        // NULL handling
        let result = db.query("SELECT CEIL(NULL)")?;
        assert_eq!(result.rows()[0][0], SqlValue::Null);

        Ok(())
    })
}

#[test]
fn test_floor_function() -> Result<()> {
    run_with_both_backends("test_floor_function", |db_path| {
        let mut db = Database::open(db_path)?;
        db.register_extension(Box::new(MathFunctionsExtension))?;

        // Positive decimal
        let result = db.query("SELECT FLOOR(3.7)")?;
        assert_eq!(result.rows()[0][0], SqlValue::Integer(3));

        // Negative decimal
        let result = db.query("SELECT FLOOR(-3.7)")?;
        assert_eq!(result.rows()[0][0], SqlValue::Integer(-4));

        // Already integer
        let result = db.query("SELECT FLOOR(5)")?;
        assert_eq!(result.rows()[0][0], SqlValue::Integer(5));

        // Zero
        let result = db.query("SELECT FLOOR(0.0)")?;
        assert_eq!(result.rows()[0][0], SqlValue::Integer(0));

        // NULL handling
        let result = db.query("SELECT FLOOR(NULL)")?;
        assert_eq!(result.rows()[0][0], SqlValue::Null);

        Ok(())
    })
}

#[test]
fn test_round_function() -> Result<()> {
    run_with_both_backends("test_round_function", |db_path| {
        let mut db = Database::open(db_path)?;
        db.register_extension(Box::new(MathFunctionsExtension))?;

        // Round to 0 decimals
        let result = db.query("SELECT ROUND(3.7, 0)")?;
        assert_eq!(result.rows()[0][0], SqlValue::Real(4.0));

        // Round to 1 decimal
        let result = db.query("SELECT ROUND(3.75, 1)")?;
        assert_eq!(result.rows()[0][0], SqlValue::Real(3.8));

        // Round to 2 decimals
        let result = db.query("SELECT ROUND(2.34567, 2)")?;
        if let SqlValue::Real(rounded) = result.rows()[0][0] {
            assert!((rounded - 2.35).abs() < 0.001);
        } else {
            panic!("Expected Real result");
        }

        // Round up
        let result = db.query("SELECT ROUND(3.5, 0)")?;
        assert_eq!(result.rows()[0][0], SqlValue::Real(4.0));

        // Round down
        let result = db.query("SELECT ROUND(3.4, 0)")?;
        assert_eq!(result.rows()[0][0], SqlValue::Real(3.0));

        // Negative number
        let result = db.query("SELECT ROUND(-3.5, 0)")?;
        assert_eq!(result.rows()[0][0], SqlValue::Real(-4.0));

        // NULL handling
        let result = db.query("SELECT ROUND(NULL, 2)")?;
        assert_eq!(result.rows()[0][0], SqlValue::Null);

        Ok(())
    })
}

#[test]
fn test_sqrt_function() -> Result<()> {
    run_with_both_backends("test_sqrt_function", |db_path| {
        let mut db = Database::open(db_path)?;
        db.register_extension(Box::new(MathFunctionsExtension))?;

        // Perfect square
        let result = db.query("SELECT SQRT(16)")?;
        assert_eq!(result.rows()[0][0], SqlValue::Real(4.0));

        // Another perfect square
        let result = db.query("SELECT SQRT(144)")?;
        assert_eq!(result.rows()[0][0], SqlValue::Real(12.0));

        // Decimal
        let result = db.query("SELECT SQRT(2.25)")?;
        assert_eq!(result.rows()[0][0], SqlValue::Real(1.5));

        // Zero
        let result = db.query("SELECT SQRT(0)")?;
        assert_eq!(result.rows()[0][0], SqlValue::Real(0.0));

        // One
        let result = db.query("SELECT SQRT(1)")?;
        assert_eq!(result.rows()[0][0], SqlValue::Real(1.0));

        // Negative number should error
        let result = db.query("SELECT SQRT(-1)");
        assert!(result.is_err());

        // NULL handling
        let result = db.query("SELECT SQRT(NULL)")?;
        assert_eq!(result.rows()[0][0], SqlValue::Null);

        Ok(())
    })
}

#[test]
fn test_pow_function() -> Result<()> {
    run_with_both_backends("test_pow_function", |db_path| {
        let mut db = Database::open(db_path)?;
        db.register_extension(Box::new(MathFunctionsExtension))?;

        // Positive exponent
        let result = db.query("SELECT POW(2, 3)")?;
        assert_eq!(result.rows()[0][0], SqlValue::Real(8.0));

        // Zero exponent
        let result = db.query("SELECT POW(5, 0)")?;
        assert_eq!(result.rows()[0][0], SqlValue::Real(1.0));

        // One exponent
        let result = db.query("SELECT POW(5, 1)")?;
        assert_eq!(result.rows()[0][0], SqlValue::Real(5.0));

        // Negative exponent
        let result = db.query("SELECT POW(2, -2)")?;
        assert_eq!(result.rows()[0][0], SqlValue::Real(0.25));

        // Base zero
        let result = db.query("SELECT POW(0, 5)")?;
        assert_eq!(result.rows()[0][0], SqlValue::Real(0.0));

        // Decimal base
        let result = db.query("SELECT POW(2.5, 2)")?;
        assert_eq!(result.rows()[0][0], SqlValue::Real(6.25));

        // NULL handling
        let result = db.query("SELECT POW(NULL, 2)")?;
        assert_eq!(result.rows()[0][0], SqlValue::Null);

        Ok(())
    })
}

#[test]
fn test_mod_function() -> Result<()> {
    run_with_both_backends("test_mod_function", |db_path| {
        let mut db = Database::open(db_path)?;
        db.register_extension(Box::new(MathFunctionsExtension))?;

        // Basic modulo
        let result = db.query("SELECT MOD(10, 3)")?;
        assert_eq!(result.rows()[0][0], SqlValue::Integer(1));

        // Even division
        let result = db.query("SELECT MOD(10, 5)")?;
        assert_eq!(result.rows()[0][0], SqlValue::Integer(0));

        // Larger number
        let result = db.query("SELECT MOD(17, 5)")?;
        assert_eq!(result.rows()[0][0], SqlValue::Integer(2));

        // Negative dividend
        let result = db.query("SELECT MOD(-10, 3)")?;
        assert_eq!(result.rows()[0][0], SqlValue::Integer(-1));

        // Real numbers
        let result = db.query("SELECT MOD(10.5, 3.2)")?;
        // Result should be approximately 0.9 (implementation dependent)
        if let SqlValue::Real(r) = result.rows()[0][0] {
            assert!(r > 0.8 && r < 1.0);
        } else {
            panic!("Expected Real result");
        }

        // Zero divisor should error
        let result = db.query("SELECT MOD(10, 0)");
        assert!(result.is_err());

        // NULL handling
        let result = db.query("SELECT MOD(NULL, 3)")?;
        assert_eq!(result.rows()[0][0], SqlValue::Null);

        Ok(())
    })
}

#[test]
fn test_sign_function() -> Result<()> {
    run_with_both_backends("test_sign_function", |db_path| {
        let mut db = Database::open(db_path)?;
        db.register_extension(Box::new(MathFunctionsExtension))?;

        // Positive integer
        let result = db.query("SELECT SIGN(5)")?;
        assert_eq!(result.rows()[0][0], SqlValue::Integer(1));

        // Negative integer
        let result = db.query("SELECT SIGN(-5)")?;
        assert_eq!(result.rows()[0][0], SqlValue::Integer(-1));

        // Zero
        let result = db.query("SELECT SIGN(0)")?;
        assert_eq!(result.rows()[0][0], SqlValue::Integer(0));

        // Positive real
        let result = db.query("SELECT SIGN(3.7)")?;
        assert_eq!(result.rows()[0][0], SqlValue::Integer(1));

        // Negative real
        let result = db.query("SELECT SIGN(-3.7)")?;
        assert_eq!(result.rows()[0][0], SqlValue::Integer(-1));

        // Zero real
        let result = db.query("SELECT SIGN(0.0)")?;
        assert_eq!(result.rows()[0][0], SqlValue::Integer(0));

        // NULL handling
        let result = db.query("SELECT SIGN(NULL)")?;
        assert_eq!(result.rows()[0][0], SqlValue::Null);

        Ok(())
    })
}

#[test]
fn test_math_functions_in_table_queries() -> Result<()> {
    run_with_both_backends("test_math_functions_in_table_queries", |db_path| {
        let mut db = Database::open(db_path)?;
        db.register_extension(Box::new(MathFunctionsExtension))?;

        // Create table with numeric data
        db.execute("CREATE TABLE products (id INTEGER PRIMARY KEY, price REAL, quantity INTEGER)")?;
        db.execute("INSERT INTO products (id, price, quantity) VALUES (1, 19.99, 5), (2, -10.50, 3), (3, 0.0, 0)")?;

        // Use ABS in SELECT
        let result = db.query("SELECT ABS(price) FROM products WHERE id = 2")?;
        assert_eq!(result.rows()[0][0], SqlValue::Real(10.50));

        // Use functions in WHERE clause
        let result = db.query("SELECT id FROM products WHERE ABS(price) > 10")?;
        assert_eq!(result.len(), 2);

        // Use CEIL in SELECT
        let result = db.query("SELECT CEIL(price) FROM products WHERE id = 1")?;
        assert_eq!(result.rows()[0][0], SqlValue::Integer(20));

        // Use FLOOR in SELECT
        let result = db.query("SELECT FLOOR(price) FROM products WHERE id = 1")?;
        assert_eq!(result.rows()[0][0], SqlValue::Integer(19));

        // Use ROUND in SELECT
        let result = db.query("SELECT ROUND(price, 1) FROM products WHERE id = 1")?;
        assert_eq!(result.rows()[0][0], SqlValue::Real(20.0));

        // Use SIGN in SELECT
        let result = db.query("SELECT SIGN(price) FROM products WHERE id = 2")?;
        assert_eq!(result.rows()[0][0], SqlValue::Integer(-1));

        Ok(())
    })
}
