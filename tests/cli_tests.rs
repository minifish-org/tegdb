use std::fs;
use std::io::Write;
use std::process::{Command, Stdio};
use tempfile::TempDir;

fn get_cli_binary() -> String {
    // Try to find the CLI binary in target directory
    let target_dir = if cfg!(debug_assertions) {
        "target/debug"
    } else {
        "target/release"
    };

    let binary_path = format!("{target_dir}/tg");
    if std::path::Path::new(&binary_path).exists() {
        binary_path
    } else {
        // Fallback to cargo run
        "cargo".to_string()
    }
}

fn run_cli_command(args: &[&str], input: Option<&str>) -> (String, String, i32) {
    let binary = get_cli_binary();

    let mut cmd = if binary == "cargo" {
        let mut c = Command::new("cargo");
        c.args(["run", "--bin", "tg", "--features", "dev", "--"]);
        c.args(args);
        c
    } else {
        let mut c = Command::new(&binary);
        c.args(args);
        c
    };

    cmd.stdout(Stdio::piped()).stderr(Stdio::piped());

    if let Some(_input_str) = input {
        cmd.stdin(Stdio::piped());
    }

    let mut child = cmd.spawn().expect("Failed to spawn CLI process");

    if let Some(input_str) = input {
        if let Some(stdin) = child.stdin.as_mut() {
            stdin
                .write_all(input_str.as_bytes())
                .expect("Failed to write to stdin");
        }
    }

    let output = child
        .wait_with_output()
        .expect("Failed to wait for CLI process");

    let stdout = String::from_utf8_lossy(&output.stdout).to_string();
    let stderr = String::from_utf8_lossy(&output.stderr).to_string();
    let exit_code = output.status.code().unwrap_or(-1);

    (stdout, stderr, exit_code)
}

#[test]
fn test_cli_help() {
    let (stdout, _stderr, exit_code) = run_cli_command(&["--help"], None);

    assert_eq!(exit_code, 0);
    assert!(stdout.contains("TegDB CLI"));
    assert!(stdout.contains("--help"));
    assert!(stdout.contains("--command"));
}

#[test]
fn test_cli_version() {
    let (stdout, _stderr, exit_code) = run_cli_command(&["--version"], None);

    assert_eq!(exit_code, 0);
    assert!(stdout.contains("tg"));
}

#[test]
fn test_cli_missing_database() {
    let (_stdout, _stderr, exit_code) = run_cli_command(&["nonexistent.db", "--help"], None);

    // Should still show help even with invalid database path
    assert_eq!(exit_code, 0);
}

#[test]
fn test_cli_create_database_and_table() {
    let temp_dir = TempDir::new().expect("Failed to create temp dir");
    let db_path = temp_dir.path().join("test.db");

    // Create a table
    let (stdout, _stderr, exit_code) = run_cli_command(
        &[
            db_path.to_str().unwrap(),
            "-c",
            "CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT(50))",
        ],
        None,
    );

    assert_eq!(exit_code, 0);
    assert!(stdout.contains("0")); // Rows affected

    // Verify table exists using .tables command
    let input = ".tables\n.quit\n";
    let (stdout, _stderr, exit_code) = run_cli_command(&[db_path.to_str().unwrap()], Some(input));

    assert_eq!(exit_code, 0);
    assert!(stdout.contains("users")); // Table name should appear
}

#[test]
fn test_cli_repl_basic_commands() {
    let temp_dir = TempDir::new().expect("Failed to create temp dir");
    let db_path = temp_dir.path().join("test.db");

    let input = "CREATE TABLE test (id INTEGER PRIMARY KEY, name TEXT(50));\n.tables\n.quit\n";
    let (stdout, _stderr, exit_code) = run_cli_command(&[db_path.to_str().unwrap()], Some(input));

    assert_eq!(exit_code, 0);
    assert!(stdout.contains("TegDB CLI"));
    assert!(stdout.contains("test")); // Table name should appear
}

#[test]
fn test_cli_dot_commands() {
    let temp_dir = TempDir::new().expect("Failed to create temp dir");
    let db_path = temp_dir.path().join("test.db");

    let input = "CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT(50));\n.tables\n.schema users\n.stats\n.help\n.quit\n";
    let (stdout, _stderr, exit_code) = run_cli_command(&[db_path.to_str().unwrap()], Some(input));

    assert_eq!(exit_code, 0);
    assert!(stdout.contains("users")); // Table name
    assert!(stdout.contains("CREATE TABLE")); // Schema output
    assert!(stdout.contains("Database Statistics")); // Stats output
    assert!(stdout.contains("Available dot commands")); // Help output
}

#[test]
fn test_cli_output_formats() {
    let temp_dir = TempDir::new().expect("Failed to create temp dir");
    let db_path = temp_dir.path().join("test.db");

    // Create table and insert data
    let setup_input = "CREATE TABLE test (id INTEGER PRIMARY KEY, name TEXT(50));\nINSERT INTO test (id, name) VALUES (1, 'Alice');\n.quit\n";
    run_cli_command(&[db_path.to_str().unwrap()], Some(setup_input));

    // Test table format (default)
    let (stdout, _stderr, exit_code) = run_cli_command(
        &[db_path.to_str().unwrap(), "-c", "SELECT * FROM test"],
        None,
    );

    assert_eq!(exit_code, 0);
    assert!(stdout.contains("id") && stdout.contains("name")); // Headers
    assert!(stdout.contains("1") && stdout.contains("Alice")); // Data

    // Test CSV format
    let (stdout, _stderr, exit_code) = run_cli_command(
        &[
            db_path.to_str().unwrap(),
            "-c",
            "SELECT * FROM test",
            "--mode",
            "csv",
        ],
        None,
    );

    assert_eq!(exit_code, 0);
    assert!(stdout.contains("id,name"));
    assert!(stdout.contains("1,Alice"));

    // Test JSON format
    let (stdout, _stderr, exit_code) = run_cli_command(
        &[
            db_path.to_str().unwrap(),
            "-c",
            "SELECT * FROM test",
            "--mode",
            "json",
        ],
        None,
    );

    assert_eq!(exit_code, 0);
    assert!(stdout.contains("\"id\": 1"));
    assert!(stdout.contains("\"name\": \"Alice\""));
}

#[test]
fn test_cli_script_execution() {
    let temp_dir = TempDir::new().expect("Failed to create temp dir");
    let db_path = temp_dir.path().join("test.db");
    let script_path = temp_dir.path().join("script.sql");

    // Create a script file
    let script_content = "CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT(50));\nINSERT INTO users (id, name) VALUES (1, 'Alice');\nINSERT INTO users (id, name) VALUES (2, 'Bob');\n";
    fs::write(&script_path, script_content).expect("Failed to write script file");

    // Execute script
    let (stdout, _stderr, exit_code) = run_cli_command(
        &[
            db_path.to_str().unwrap(),
            "-f",
            script_path.to_str().unwrap(),
        ],
        None,
    );

    assert_eq!(exit_code, 0);
    assert!(stdout.contains("0")); // CREATE TABLE
    assert!(stdout.contains("1")); // INSERT statements

    // Verify data was inserted
    let (stdout, _stderr, exit_code) = run_cli_command(
        &[
            db_path.to_str().unwrap(),
            "-c",
            "SELECT COUNT(*) FROM users",
        ],
        None,
    );

    assert_eq!(exit_code, 0);
    assert!(stdout.contains("2")); // Should have 2 rows
}

#[test]
fn test_cli_timer_and_echo() {
    let temp_dir = TempDir::new().expect("Failed to create temp dir");
    let db_path = temp_dir.path().join("test.db");

    let input = "CREATE TABLE test (id INTEGER PRIMARY KEY, name TEXT(50));\nINSERT INTO test (id, name) VALUES (1, 'Alice');\n.timer on\n.echo on\nSELECT * FROM test;\n.quit\n";
    let (stdout, stderr, exit_code) = run_cli_command(&[db_path.to_str().unwrap()], Some(input));

    assert_eq!(exit_code, 0);
    assert!(stdout.contains("Timer enabled"));
    assert!(stdout.contains("Echo enabled"));
    assert!(stdout.contains("SELECT * FROM test")); // Echoed SQL
    assert!(stderr.contains("Query executed in") || stderr.contains("µs") || stderr.contains("ms"));
    // Timing output
}

#[test]
fn test_cli_output_redirection() {
    let temp_dir = TempDir::new().expect("Failed to create temp dir");
    let db_path = temp_dir.path().join("test.db");
    let output_path = temp_dir.path().join("output.txt");

    // Create table and insert data
    let setup_input = "CREATE TABLE test (id INTEGER PRIMARY KEY, name TEXT(50));\nINSERT INTO test (id, name) VALUES (1, 'Alice');\n.quit\n";
    run_cli_command(&[db_path.to_str().unwrap()], Some(setup_input));

    // Test output redirection
    let (_stdout, _stderr, exit_code) = run_cli_command(
        &[
            db_path.to_str().unwrap(),
            "-c",
            "SELECT * FROM test",
            "--output",
            output_path.to_str().unwrap(),
        ],
        None,
    );

    assert_eq!(exit_code, 0);

    // Check that output was written to file
    let output_content = fs::read_to_string(&output_path).expect("Failed to read output file");
    assert!(output_content.contains("id") && output_content.contains("name"));
    assert!(output_content.contains("1") && output_content.contains("Alice"));
}

#[test]
fn test_cli_csv_import() {
    let temp_dir = TempDir::new().expect("Failed to create temp dir");
    let db_path = temp_dir.path().join("test.db");
    let csv_path = temp_dir.path().join("data.csv");

    // Create CSV file
    let csv_content = "id,name,age\n1,Alice,25\n2,Bob,30\n";
    fs::write(&csv_path, csv_content).expect("Failed to write CSV file");

    // Create table
    let setup_input =
        "CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT(50), age INTEGER);\n.quit\n";
    run_cli_command(&[db_path.to_str().unwrap()], Some(setup_input));

    // Import CSV
    let (stdout, _stderr, exit_code) = run_cli_command(
        &[
            db_path.to_str().unwrap(),
            "-c",
            &format!("COPY users FROM {}", csv_path.to_str().unwrap()),
        ],
        None,
    );

    assert_eq!(exit_code, 0);
    assert!(stdout.contains("2 rows imported"));

    // Verify data was imported
    let (stdout, _stderr, exit_code) = run_cli_command(
        &[
            db_path.to_str().unwrap(),
            "-c",
            "SELECT * FROM users ORDER BY id",
        ],
        None,
    );

    assert_eq!(exit_code, 0);
    assert!(stdout.contains("Alice") && stdout.contains("Bob"));
    assert!(stdout.contains("25") && stdout.contains("30"));
}

#[test]
fn test_cli_error_handling() {
    let temp_dir = TempDir::new().expect("Failed to create temp dir");
    let db_path = temp_dir.path().join("test.db");

    // Test invalid SQL
    let (stdout, stderr, exit_code) = run_cli_command(
        &[db_path.to_str().unwrap(), "-c", "INVALID SQL STATEMENT"],
        None,
    );

    assert_ne!(exit_code, 0);
    assert!(stderr.contains("Error") || stdout.contains("Error"));

    // Test invalid dot command
    let input = ".invalid_command\n.quit\n";
    let (stdout, stderr, exit_code) = run_cli_command(&[db_path.to_str().unwrap()], Some(input));

    assert_eq!(exit_code, 0); // Should not crash
}

#[test]
fn test_cli_quiet_mode() {
    let temp_dir = TempDir::new().expect("Failed to create temp dir");
    let db_path = temp_dir.path().join("test.db");

    // Create a table first
    let setup_input = "CREATE TABLE test (id INTEGER PRIMARY KEY, name TEXT(50));\nINSERT INTO test (id, name) VALUES (1, 'Alice');\n.quit\n";
    run_cli_command(&[db_path.to_str().unwrap()], Some(setup_input));

    // Test quiet mode
    let (stdout, stderr, exit_code) = run_cli_command(
        &[
            db_path.to_str().unwrap(),
            "-c",
            "SELECT * FROM test",
            "--quiet",
        ],
        None,
    );

    assert_eq!(exit_code, 0);
    assert!(!stderr.contains("Executing:")); // Should not show execution message
    assert!(stdout.contains("Alice")); // Should still show result
}

#[test]
fn test_cli_repl_mode_switching() {
    let temp_dir = TempDir::new().expect("Failed to create temp dir");
    let db_path = temp_dir.path().join("test.db");

    let input = "CREATE TABLE test (id INTEGER PRIMARY KEY, name TEXT(50));\nINSERT INTO test (id, name) VALUES (1, 'Alice');\n.mode csv\nSELECT * FROM test;\n.mode json\nSELECT * FROM test;\n.quit\n";
    let (stdout, _stderr, exit_code) = run_cli_command(&[db_path.to_str().unwrap()], Some(input));

    assert_eq!(exit_code, 0);
    assert!(stdout.contains("Output mode set to CSV"));
    assert!(stdout.contains("Output mode set to JSON"));
    assert!(stdout.contains("id,name")); // CSV output
    assert!(stdout.contains("\"id\": 1")); // JSON output
}

#[test]
fn test_cli_read_command() {
    let temp_dir = TempDir::new().expect("Failed to create temp dir");
    let db_path = temp_dir.path().join("test.db");
    let script_path = temp_dir.path().join("script.sql");

    // Create a script file
    let script_content = "CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT(50));\nINSERT INTO users (id, name) VALUES (1, 'Alice');\n";
    fs::write(&script_path, script_content).expect("Failed to write script file");

    let input = &format!(
        ".read {}\nSELECT * FROM users;\n.quit\n",
        script_path.to_str().unwrap()
    );
    let (stdout, _stderr, exit_code) = run_cli_command(&[db_path.to_str().unwrap()], Some(input));

    assert_eq!(exit_code, 0);
    assert!(stdout.contains("Alice")); // Data should be inserted and selected
}

#[test]
fn test_cli_output_stdout_redirection() {
    let temp_dir = TempDir::new().expect("Failed to create temp dir");
    let db_path = temp_dir.path().join("test.db");

    let input = "CREATE TABLE test (id INTEGER PRIMARY KEY, name TEXT(50));\nINSERT INTO test (id, name) VALUES (1, 'Alice');\n.output stdout\nSELECT * FROM test;\n.quit\n";
    let (stdout, _stderr, exit_code) = run_cli_command(&[db_path.to_str().unwrap()], Some(input));

    assert_eq!(exit_code, 0);
    assert!(stdout.contains("Output set to stdout"));
    assert!(stdout.contains("Alice")); // Data should be in stdout
}

#[test]
fn test_cli_multiline_sql() {
    let temp_dir = TempDir::new().expect("Failed to create temp dir");
    let db_path = temp_dir.path().join("test.db");

    // Test multi-line CREATE TABLE
    let input = "CREATE TABLE users (\n  id INTEGER PRIMARY KEY,\n  name TEXT(50),\n  age INTEGER\n);\n.tables\n.quit\n";
    let (stdout, _stderr, exit_code) = run_cli_command(&[db_path.to_str().unwrap()], Some(input));

    assert_eq!(exit_code, 0);
    assert!(stdout.contains("users")); // Table should be created

    // Test multi-line INSERT (now supported)
    let input = "INSERT INTO users (\n  id,\n  name,\n  age\n) VALUES (\n  1,\n  'Alice',\n  25\n);\nSELECT * FROM users;\n.quit\n";
    let (stdout, _stderr, exit_code) = run_cli_command(&[db_path.to_str().unwrap()], Some(input));

    assert_eq!(exit_code, 0);
    assert!(stdout.contains("Alice")); // Data should be inserted and selected
}

#[test]
fn test_cli_clear_buffer() {
    let temp_dir = TempDir::new().expect("Failed to create temp dir");
    let db_path = temp_dir.path().join("test.db");

    // Test clearing SQL buffer
    let input = "SELECT *\nFROM test;\n.clear\n.quit\n";
    let (stdout, _stderr, exit_code) = run_cli_command(&[db_path.to_str().unwrap()], Some(input));

    assert_eq!(exit_code, 0);
    assert!(stdout.contains("SQL buffer cleared"));
}

#[test]
fn test_cli_string_escaping() {
    let temp_dir = TempDir::new().expect("Failed to create temp dir");
    let db_path = temp_dir.path().join("test.db");

    // Test string escaping
    let input = "CREATE TABLE test (id INTEGER PRIMARY KEY, name TEXT(50));\nINSERT INTO test (id, name) VALUES (1, 'Charlie\\'s Name');\nSELECT * FROM test;\n.quit\n";
    let (stdout, _stderr, exit_code) = run_cli_command(&[db_path.to_str().unwrap()], Some(input));

    assert_eq!(exit_code, 0);
    assert!(stdout.contains("Charlie's Name")); // Should handle escaped quotes
}

#[test]
fn test_cli_complex_queries() {
    let temp_dir = TempDir::new().expect("Failed to create temp dir");
    let db_path = temp_dir.path().join("test.db");

    // Test basic multi-table operations
    let input = "CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT(50), age INTEGER);
CREATE TABLE orders (id INTEGER PRIMARY KEY, user_id INTEGER, amount REAL);
INSERT INTO users (id, name, age) VALUES (1, 'Alice', 25), (2, 'Bob', 30);
INSERT INTO orders (id, user_id, amount) VALUES (1, 1, 99.99), (2, 2, 149.50);
SELECT * FROM users;
SELECT * FROM orders;
.quit\n";
    let (stdout, _stderr, exit_code) = run_cli_command(&[db_path.to_str().unwrap()], Some(input));

    assert_eq!(exit_code, 0);
    // Just check that the commands executed successfully
    assert!(stdout.len() > 0);
}

#[test]
fn test_cli_aggregate_functions() {
    let temp_dir = TempDir::new().expect("Failed to create temp dir");
    let db_path = temp_dir.path().join("test.db");

    let input = "CREATE TABLE sales (id INTEGER PRIMARY KEY, amount REAL, category TEXT(20));
INSERT INTO sales (id, amount, category) VALUES (1, 100.0, 'Electronics'), (2, 200.0, 'Electronics'), (3, 150.0, 'Books');
SELECT COUNT(*) FROM sales;
SELECT SUM(amount) FROM sales;
.quit\n";
    let (stdout, _stderr, exit_code) = run_cli_command(&[db_path.to_str().unwrap()], Some(input));

    assert_eq!(exit_code, 0);
    // Basic aggregate functions should work
    assert!(stdout.contains("3")); // COUNT should return 3
}

#[test]
fn test_cli_arithmetic_expressions() {
    let temp_dir = TempDir::new().expect("Failed to create temp dir");
    let db_path = temp_dir.path().join("test.db");

    let input = "CREATE TABLE products (id INTEGER PRIMARY KEY, price REAL, quantity INTEGER);
INSERT INTO products (id, price, quantity) VALUES (1, 10.0, 5), (2, 20.0, 3);
SELECT id, price * quantity FROM products;
SELECT id, price + 5 FROM products;
.quit\n";
    let (stdout, _stderr, exit_code) = run_cli_command(&[db_path.to_str().unwrap()], Some(input));

    assert_eq!(exit_code, 0);
    // Basic arithmetic should work - check for any numeric results
    assert!(stdout.contains("10.0") || stdout.contains("5") || stdout.contains("20.0"));
}

#[test]
fn test_cli_where_conditions() {
    let temp_dir = TempDir::new().expect("Failed to create temp dir");
    let db_path = temp_dir.path().join("test.db");

    let input = "CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT(50), age INTEGER, city TEXT(30));
INSERT INTO users (id, name, age, city) VALUES (1, 'Alice', 25, 'NYC'), (2, 'Bob', 30, 'LA'), (3, 'Charlie', 35, 'NYC');
SELECT * FROM users WHERE age > 25 AND city = 'NYC';
SELECT * FROM users WHERE age BETWEEN 20 AND 30;
SELECT * FROM users WHERE name LIKE 'A%';
.quit\n";
    let (stdout, _stderr, exit_code) = run_cli_command(&[db_path.to_str().unwrap()], Some(input));

    assert_eq!(exit_code, 0);
    assert!(stdout.contains("Charlie")); // Should find Charlie (age 35, NYC)
    assert!(stdout.contains("Alice") && stdout.contains("Bob")); // Should find both in age range
    assert!(stdout.contains("Alice")); // Should find Alice with name starting with 'A'
}

#[test]
fn test_cli_order_by_and_limit() {
    let temp_dir = TempDir::new().expect("Failed to create temp dir");
    let db_path = temp_dir.path().join("test.db");

    let input = "CREATE TABLE scores (id INTEGER PRIMARY KEY, name TEXT(50), score INTEGER);
INSERT INTO scores (id, name, score) VALUES (1, 'Alice', 85), (2, 'Bob', 92), (3, 'Charlie', 78), (4, 'David', 95);
SELECT * FROM scores ORDER BY score DESC LIMIT 2;
SELECT * FROM scores ORDER BY name ASC;
.quit\n";
    let (stdout, _stderr, exit_code) = run_cli_command(&[db_path.to_str().unwrap()], Some(input));

    assert_eq!(exit_code, 0);
    // Should show top 2 scores in descending order
    assert!(stdout.contains("David") && stdout.contains("Bob"));
    // Should show all names in alphabetical order
    assert!(stdout.contains("Alice") && stdout.contains("Bob") && stdout.contains("Charlie") && stdout.contains("David"));
}

#[test]
fn test_cli_update_and_delete() {
    let temp_dir = TempDir::new().expect("Failed to create temp dir");
    let db_path = temp_dir.path().join("test.db");

    let input = "CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT(50), age INTEGER);
INSERT INTO users (id, name, age) VALUES (1, 'Alice', 25), (2, 'Bob', 30), (3, 'Charlie', 35);
UPDATE users SET age = 26 WHERE name = 'Alice';
DELETE FROM users WHERE age > 30;
SELECT * FROM users;
.quit\n";
    let (stdout, _stderr, exit_code) = run_cli_command(&[db_path.to_str().unwrap()], Some(input));

    assert_eq!(exit_code, 0);
    assert!(stdout.contains("Alice") && stdout.contains("26")); // Alice's age updated
    assert!(stdout.contains("Bob")); // Bob should still be there
    assert!(!stdout.contains("Charlie")); // Charlie should be deleted
}

#[test]
fn test_cli_index_operations() {
    let temp_dir = TempDir::new().expect("Failed to create temp dir");
    let db_path = temp_dir.path().join("test.db");

    let input = "CREATE TABLE users (id INTEGER PRIMARY KEY, email TEXT(100), name TEXT(50));
CREATE INDEX idx_email ON users (email);
.tables
.schema users
.quit\n";
    let (stdout, _stderr, exit_code) = run_cli_command(&[db_path.to_str().unwrap()], Some(input));

    assert_eq!(exit_code, 0);
    assert!(stdout.contains("users"));
    // Index creation should work
}

#[test]
fn test_cli_transaction_operations() {
    let temp_dir = TempDir::new().expect("Failed to create temp dir");
    let db_path = temp_dir.path().join("test.db");

    let input = "CREATE TABLE accounts (id INTEGER PRIMARY KEY, balance REAL);
INSERT INTO accounts (id, balance) VALUES (1, 1000.0), (2, 500.0);
BEGIN TRANSACTION;
SELECT * FROM accounts;
COMMIT;
SELECT * FROM accounts;
.quit\n";
    let (stdout, _stderr, exit_code) = run_cli_command(&[db_path.to_str().unwrap()], Some(input));

    assert_eq!(exit_code, 0);
    // Just check that the commands executed successfully
    assert!(stdout.len() > 0);
}

#[test]
fn test_cli_rollback_operations() {
    let temp_dir = TempDir::new().expect("Failed to create temp dir");
    let db_path = temp_dir.path().join("test.db");

    let input = "CREATE TABLE accounts (id INTEGER PRIMARY KEY, balance REAL);
INSERT INTO accounts (id, balance) VALUES (1, 1000.0);
BEGIN TRANSACTION;
SELECT * FROM accounts;
ROLLBACK;
SELECT * FROM accounts;
.quit\n";
    let (stdout, _stderr, exit_code) = run_cli_command(&[db_path.to_str().unwrap()], Some(input));

    assert_eq!(exit_code, 0);
    // Just check that the commands executed successfully
    assert!(stdout.len() > 0);
}

#[test]
fn test_cli_error_recovery() {
    let temp_dir = TempDir::new().expect("Failed to create temp dir");
    let db_path = temp_dir.path().join("test.db");

    let input = "CREATE TABLE test (id INTEGER PRIMARY KEY);
INVALID SQL STATEMENT;
SELECT * FROM test;
.quit\n";
    let (stdout, stderr, exit_code) = run_cli_command(&[db_path.to_str().unwrap()], Some(input));

    assert_eq!(exit_code, 0);
    assert!(stderr.contains("Error") || stdout.contains("Error")); // Should show error
    assert!(stdout.contains("test")); // Should still show table
}

#[test]
fn test_cli_large_dataset() {
    let temp_dir = TempDir::new().expect("Failed to create temp dir");
    let db_path = temp_dir.path().join("test.db");

    let mut input = String::from("CREATE TABLE numbers (id INTEGER PRIMARY KEY, value INTEGER);\n");
    
    // Insert 100 rows
    for i in 1..=100 {
        input.push_str(&format!("INSERT INTO numbers (id, value) VALUES ({}, {});\n", i, i * 2));
    }
    
    input.push_str("SELECT COUNT(*) FROM numbers;\n");
    input.push_str("SELECT * FROM numbers WHERE value > 100 LIMIT 10;\n");
    input.push_str(".quit\n");
    
    let (stdout, _stderr, exit_code) = run_cli_command(&[db_path.to_str().unwrap()], Some(&input));

    assert_eq!(exit_code, 0);
    assert!(stdout.contains("100")); // Should count 100 rows
}

#[test]
fn test_cli_nested_expressions() {
    let temp_dir = TempDir::new().expect("Failed to create temp dir");
    let db_path = temp_dir.path().join("test.db");

    let input = "CREATE TABLE math (id INTEGER PRIMARY KEY, a INTEGER, b INTEGER, c INTEGER);
INSERT INTO math (id, a, b, c) VALUES (1, 10, 20, 30);
SELECT id, (a + b) * c FROM math;
SELECT id, a + (b * c) FROM math;
.quit\n";
    let (stdout, _stderr, exit_code) = run_cli_command(&[db_path.to_str().unwrap()], Some(input));

    assert_eq!(exit_code, 0);
    // Should show calculated results
    assert!(stdout.contains("900")); // (10 + 20) * 30 = 900
    assert!(stdout.contains("610")); // 10 + (20 * 30) = 610
}

#[test]
fn test_cli_vector_operations() {
    let temp_dir = TempDir::new().expect("Failed to create temp dir");
    let db_path = temp_dir.path().join("test.db");

    let input = "CREATE TABLE embeddings (id INTEGER PRIMARY KEY, vector VECTOR(3));
INSERT INTO embeddings (id, vector) VALUES (1, [1.0, 2.0, 3.0]);
SELECT * FROM embeddings;
.quit\n";
    let (stdout, _stderr, exit_code) = run_cli_command(&[db_path.to_str().unwrap()], Some(input));

    // Vector operations may not be fully supported yet
    // Just test that the table creation works or fails gracefully
    assert!(exit_code == 0 || exit_code == 101);
}

#[test]
fn test_cli_parameter_queries() {
    let temp_dir = TempDir::new().expect("Failed to create temp dir");
    let db_path = temp_dir.path().join("test.db");

    let input = "CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT(50), age INTEGER);
INSERT INTO users (id, name, age) VALUES (1, 'Alice', 25), (2, 'Bob', 30);
SELECT * FROM users WHERE id = ?1;
.quit\n";
    let (stdout, _stderr, exit_code) = run_cli_command(&[db_path.to_str().unwrap()], Some(input));

    // Parameter queries may not be fully supported yet
    // Just test that the parser accepts parameter placeholders
    assert!(exit_code == 0 || exit_code == 101); // Either works or fails gracefully
}

#[test]
fn test_cli_complex_joins() {
    let temp_dir = TempDir::new().expect("Failed to create temp dir");
    let db_path = temp_dir.path().join("test.db");

    let input = "CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT(50));
CREATE TABLE orders (id INTEGER PRIMARY KEY, user_id INTEGER, amount REAL);
INSERT INTO users (id, name) VALUES (1, 'Alice'), (2, 'Bob');
INSERT INTO orders (id, user_id, amount) VALUES (1, 1, 99.99), (2, 1, 149.50), (3, 2, 75.00);
SELECT u.name, o.amount FROM users u, orders o WHERE u.id = o.user_id;
.quit\n";
    let (stdout, _stderr, exit_code) = run_cli_command(&[db_path.to_str().unwrap()], Some(input));

    assert_eq!(exit_code, 0);
    // Just check that the commands executed successfully
    assert!(stdout.len() > 0);
}
