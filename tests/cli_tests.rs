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

    // Test single-line INSERT (parser limitation)
    let input = "INSERT INTO users (id, name, age) VALUES (1, 'Alice', 25);\nSELECT * FROM users;\n.quit\n";
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
