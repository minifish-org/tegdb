use clap::Parser;
use rustyline::error::ReadlineError;
use rustyline::{Config, Editor};
use std::fs;
use std::io::Write;
use std::path::Path;
use std::process;
use tegdb::storage_engine::EngineConfig;
use tegdb::{Database, QueryResult, SqlValue};

fn format_sql_value(value: &SqlValue) -> String {
    match value {
        SqlValue::Integer(i) => i.to_string(),
        SqlValue::Real(f) => f.to_string(),
        SqlValue::Text(s) => s.clone(),
        SqlValue::Vector(v) => format!(
            "[{}]",
            v.iter()
                .map(|x| x.to_string())
                .collect::<Vec<_>>()
                .join(", ")
        ),
        SqlValue::Null => "NULL".to_string(),
        SqlValue::Parameter(idx) => format!("?{}", idx + 1),
    }
}

fn format_query_result(result: &QueryResult, format: OutputFormat) -> String {
    if result.rows().is_empty() {
        return "No rows returned".to_string();
    }

    match format {
        OutputFormat::Table => format_table_output(result),
        OutputFormat::Csv => format_csv_output(result),
        OutputFormat::Json => format_json_output(result),
    }
}

fn format_table_output(result: &QueryResult) -> String {
    // Calculate column widths
    let mut col_widths = Vec::new();
    for (i, col) in result.columns().iter().enumerate() {
        let mut max_width = col.len();
        for row in result.rows() {
            if i < row.len() {
                let cell_width = format_sql_value(&row[i]).len();
                max_width = max_width.max(cell_width);
            }
        }
        col_widths.push(max_width.min(20)); // Limit to 20 chars
    }

    let mut output = String::new();

    // Header
    for (i, col) in result.columns().iter().enumerate() {
        if i > 0 {
            output.push('|');
        }
        output.push_str(&format!(" {:width$} ", col, width = col_widths[i]));
    }
    output.push('\n');

    // Separator
    for (i, &width) in col_widths.iter().enumerate() {
        if i > 0 {
            output.push('+');
        }
        output.push_str(&format!("-{}-", "-".repeat(width)));
    }
    output.push('\n');

    // Rows
    for row in result.rows() {
        for (i, cell) in row.iter().enumerate() {
            if i > 0 {
                output.push('|');
            }
            let cell_str = format_sql_value(cell);
            let truncated = if cell_str.len() > col_widths[i] {
                format!("{}...", &cell_str[..col_widths[i].saturating_sub(3)])
            } else {
                cell_str
            };
            output.push_str(&format!(" {:width$} ", truncated, width = col_widths[i]));
        }
        output.push('\n');
    }

    output
}

fn format_csv_output(result: &QueryResult) -> String {
    let mut output = String::new();

    // Header
    for (i, col) in result.columns().iter().enumerate() {
        if i > 0 {
            output.push(',');
        }
        output.push_str(&escape_csv_field(col));
    }
    output.push('\n');

    // Rows
    for row in result.rows() {
        for (i, cell) in row.iter().enumerate() {
            if i > 0 {
                output.push(',');
            }
            output.push_str(&escape_csv_field(&format_sql_value(cell)));
        }
        output.push('\n');
    }

    output
}

fn format_json_output(result: &QueryResult) -> String {
    let mut output = String::new();
    output.push('[');

    for (row_idx, row) in result.rows().iter().enumerate() {
        if row_idx > 0 {
            output.push(',');
        }
        output.push('\n');
        output.push_str("  {");

        for (col_idx, cell) in row.iter().enumerate() {
            if col_idx > 0 {
                output.push(',');
            }
            let col_name = &result.columns()[col_idx];
            let cell_value = format_sql_value(cell);
            output.push_str(&format!(
                "\n    \"{}\": {}",
                col_name,
                escape_json_value(&cell_value)
            ));
        }

        output.push('\n');
        output.push_str("  }");
    }

    output.push('\n');
    output.push(']');
    output
}

fn escape_csv_field(field: &str) -> String {
    if field.contains(',') || field.contains('"') || field.contains('\n') {
        format!("\"{}\"", field.replace('"', "\"\""))
    } else {
        field.to_string()
    }
}

fn escape_json_value(value: &str) -> String {
    if value == "NULL" {
        "null".to_string()
    } else if value.starts_with('"') && value.ends_with('"') {
        // Already a string literal
        value.to_string()
    } else if value.parse::<i64>().is_ok() || value.parse::<f64>().is_ok() {
        // Number
        value.to_string()
    } else {
        // String - escape and quote
        format!(
            "\"{}\"",
            value
                .replace('"', "\\\"")
                .replace('\n', "\\n")
                .replace('\r', "\\r")
        )
    }
}

#[derive(Parser)]
#[command(name = "tg")]
#[command(about = "TegDB CLI - A lightweight embedded database")]
#[command(version)]
struct Cli {
    /// Database file path
    db_path: String,

    /// Execute SQL command and exit
    #[arg(short, long)]
    command: Option<String>,

    /// Read and execute SQL script from file
    #[arg(short, long)]
    file: Option<String>,

    /// Output results to file
    #[arg(short, long)]
    output: Option<String>,

    /// Enable/disable execution timing
    #[arg(long)]
    timer: Option<String>,

    /// Enable/disable SQL echo
    #[arg(long)]
    echo: Option<String>,

    /// Quiet mode (output results only)
    #[arg(short, long)]
    quiet: bool,

    /// Output format (table, csv, json)
    #[arg(long, default_value = "table")]
    mode: String,

    /// Maximum number of keys to keep in memory (0 disables the cap)
    #[arg(long, value_name = "KEYS")]
    max_keys: Option<usize>,

    /// Maximum on-disk log size in bytes (0 disables the cap)
    #[arg(long, value_name = "BYTES")]
    max_log_bytes: Option<u64>,
}

#[derive(Debug, Clone, Copy, PartialEq)]
enum OutputFormat {
    Table,
    Csv,
    Json,
}

struct CliState {
    db: Database,
    timer_enabled: bool,
    echo_enabled: bool,
    output_file: Option<String>,
    output_format: OutputFormat,
    sql_buffer: String, // Buffer for multi-line SQL
}

impl CliState {
    fn new(db_path: &str, config: EngineConfig) -> Result<Self, Box<dyn std::error::Error>> {
        let db = Database::open_with_config(db_path, config)?;
        Ok(CliState {
            db,
            timer_enabled: false,
            echo_enabled: false,
            output_file: None,
            output_format: OutputFormat::Table,
            sql_buffer: String::new(),
        })
    }

    fn execute_sql(&mut self, sql: &str) -> Result<(), Box<dyn std::error::Error>> {
        if self.echo_enabled {
            println!("{sql}");
        }

        let start = std::time::Instant::now();

        // Check if it's a SELECT statement
        let trimmed_sql = sql.trim().to_uppercase();
        let is_select = trimmed_sql.starts_with("SELECT");
        let is_copy = trimmed_sql.starts_with("COPY");

        let result = if is_select {
            let query_result = self.db.query(sql)?;
            format_query_result(&query_result, self.output_format)
        } else if is_copy {
            self.handle_copy_command(sql)?
        } else {
            let rows_affected = self.db.execute(sql)?;
            format!("{rows_affected}")
        };

        let duration = start.elapsed();

        if self.timer_enabled {
            eprintln!("Query executed in {duration:?}");
        }

        // Handle output
        if let Some(ref output_file) = self.output_file {
            let mut file = fs::File::create(output_file)?;
            writeln!(file, "{result}")?;
        } else {
            println!("{result}");
        }

        Ok(())
    }

    fn handle_sql_input(&mut self, input: &str) -> Result<bool, Box<dyn std::error::Error>> {
        // Add input to buffer
        if !self.sql_buffer.is_empty() {
            self.sql_buffer.push(' ');
        }
        self.sql_buffer.push_str(input.trim());

        // Check if SQL is complete (ends with semicolon)
        if self.sql_buffer.trim().ends_with(';') {
            // Execute exactly one statement (single or multi-line) as entered
            let sql = self.sql_buffer.trim().to_string();
            if !sql.is_empty() {
                self.execute_sql(&sql)?;
            }
            // Clear buffer
            self.sql_buffer.clear();
            return Ok(false); // Continue REPL
        }

        // SQL is incomplete, continue reading
        Ok(false)
    }

    fn handle_copy_command(&mut self, sql: &str) -> Result<String, Box<dyn std::error::Error>> {
        // Simple COPY command parser
        let parts: Vec<&str> = sql.split_whitespace().collect();
        if parts.len() < 4 {
            return Err(format!(
                "Invalid COPY command syntax: expected at least 4 parts, got {}",
                parts.len()
            )
            .into());
        }

        if parts[2].to_uppercase() == "FROM" {
            // COPY table FROM file
            let table_name = parts[1];
            let file_path = parts[3];

            // Read CSV file and insert into table
            let content = fs::read_to_string(file_path)?;
            let lines: Vec<&str> = content.lines().collect();

            if lines.is_empty() {
                return Ok("0 rows imported".to_string());
            }

            // Assume first line is header
            let header = lines[0];
            let columns: Vec<&str> = header.split(',').map(|s| s.trim()).collect();

            let mut imported = 0;
            for line in &lines[1..] {
                if line.trim().is_empty() {
                    continue;
                }

                let values: Vec<&str> = line.split(',').map(|s| s.trim()).collect();
                if values.len() != columns.len() {
                    continue; // Skip malformed rows
                }

                // Build INSERT statement
                let values_str = values
                    .iter()
                    .map(|v| {
                        if v.parse::<i64>().is_ok() || v.parse::<f64>().is_ok() {
                            v.to_string()
                        } else {
                            format!("'{v}'")
                        }
                    })
                    .collect::<Vec<_>>()
                    .join(", ");

                let insert_sql = format!(
                    "INSERT INTO {} ({}) VALUES ({})",
                    table_name,
                    columns.join(", "),
                    values_str
                );

                self.db.execute(&insert_sql)?;
                imported += 1;
            }

            Ok(format!("{imported} rows imported"))
        } else if parts[1].to_uppercase() == "TO" {
            // COPY (SELECT ...) TO file
            // This is more complex and would require parsing the SELECT part
            Err("COPY TO not yet implemented".into())
        } else {
            Err("Invalid COPY command syntax".into())
        }
    }

    fn handle_dot_command(&mut self, line: &str) -> Result<bool, Box<dyn std::error::Error>> {
        let parts: Vec<&str> = line.split_whitespace().collect();
        if parts.is_empty() {
            return Ok(false);
        }

        let command = parts[0];

        match command {
            ".quit" | ".exit" | ".q" => return Ok(true),
            ".help" | ".h" => {
                println!("Available dot commands:");
                println!("  .help/.h       - Show this help");
                println!("  .tables/.t     - List all tables");
                println!("  .schema [table] - Show table schema");
                println!("  .output FILE   - Set output file");
                println!("  .read FILE     - Execute SQL from file");
                println!("  .timer on|off  - Toggle execution timing");
                println!("  .echo on|off   - Toggle SQL echo");
                println!("  .mode table|csv|json - Set output format");
                println!("  .stats/.s      - Show database statistics");
                println!("  .clear/.c      - Clear current SQL buffer");
                println!("  .quit/.exit/.q - Exit REPL");
                println!();
                println!("Note: SQL statements can span multiple lines. End with ';' to execute.");
                println!();
            }
            ".tables" | ".t" => {
                let pattern = parts.get(1).unwrap_or(&"");
                let schemas = self.db.get_table_schemas_ref();
                let mut tables: Vec<&String> = schemas.keys().collect();
                tables.sort();

                if !pattern.is_empty() {
                    tables.retain(|name| name.contains(pattern));
                }

                if tables.is_empty() {
                    println!("No tables found");
                } else {
                    for table in tables {
                        println!("{table}");
                    }
                }
            }
            ".schema" => {
                let table = parts.get(1).unwrap_or(&"");
                if table.is_empty() {
                    println!("Usage: .schema <table_name>");
                } else {
                    let schemas = self.db.get_table_schemas_ref();
                    if let Some(schema) = schemas.get(*table) {
                        println!("CREATE TABLE {} (", schema.name);
                        for (i, col) in schema.columns.iter().enumerate() {
                            if i > 0 {
                                print!(", ");
                            }
                            print!("{} ", col.name);
                            match &col.data_type {
                                tegdb::DataType::Integer => print!("INTEGER"),
                                tegdb::DataType::Text(Some(len)) => print!("TEXT({len})"),
                                tegdb::DataType::Text(None) => print!("TEXT"),
                                tegdb::DataType::Real => print!("REAL"),
                                tegdb::DataType::Vector(Some(dim)) => print!("VECTOR({dim})"),
                                tegdb::DataType::Vector(None) => print!("VECTOR"),
                            }
                            for constraint in &col.constraints {
                                match constraint {
                                    tegdb::ColumnConstraint::PrimaryKey => print!(" PRIMARY KEY"),
                                    tegdb::ColumnConstraint::NotNull => print!(" NOT NULL"),
                                    tegdb::ColumnConstraint::Unique => print!(" UNIQUE"),
                                }
                            }
                        }
                        println!(")");
                    } else {
                        println!("Table '{table}' not found");
                    }
                }
            }
            ".output" => {
                if let Some(file) = parts.get(1) {
                    if *file == "stdout" {
                        self.output_file = None;
                        println!("Output set to stdout");
                    } else {
                        self.output_file = Some(file.to_string());
                        println!("Output set to {file}");
                    }
                } else {
                    println!("Usage: .output <file>|stdout");
                }
            }
            ".read" => {
                if let Some(file) = parts.get(1) {
                    let content = fs::read_to_string(file)?;
                    for line in content.lines() {
                        let trimmed = line.trim();
                        if !trimmed.is_empty() && !trimmed.starts_with("--") {
                            self.execute_sql(trimmed)?;
                        }
                    }
                } else {
                    println!("Usage: .read <file>");
                }
            }
            ".timer" => {
                if let Some(mode) = parts.get(1) {
                    match *mode {
                        "on" => {
                            self.timer_enabled = true;
                            println!("Timer enabled");
                        }
                        "off" => {
                            self.timer_enabled = false;
                            println!("Timer disabled");
                        }
                        _ => println!("Usage: .timer on|off"),
                    }
                } else {
                    println!("Usage: .timer on|off");
                }
            }
            ".echo" => {
                if let Some(mode) = parts.get(1) {
                    match *mode {
                        "on" => {
                            self.echo_enabled = true;
                            println!("Echo enabled");
                        }
                        "off" => {
                            self.echo_enabled = false;
                            println!("Echo disabled");
                        }
                        _ => println!("Usage: .echo on|off"),
                    }
                } else {
                    println!("Usage: .echo on|off");
                }
            }
            ".mode" => {
                if let Some(format) = parts.get(1) {
                    match *format {
                        "table" => {
                            self.output_format = OutputFormat::Table;
                            println!("Output mode set to table");
                        }
                        "csv" => {
                            self.output_format = OutputFormat::Csv;
                            println!("Output mode set to CSV");
                        }
                        "json" => {
                            self.output_format = OutputFormat::Json;
                            println!("Output mode set to JSON");
                        }
                        _ => println!("Usage: .mode table|csv|json"),
                    }
                } else {
                    println!("Usage: .mode table|csv|json");
                }
            }
            ".stats" | ".s" => {
                let schemas = self.db.get_table_schemas_ref();
                println!("Database Statistics:");
                println!("  Tables: {}", schemas.len());

                let mut total_columns = 0;
                let mut total_indexes = 0;

                for (table_name, schema) in schemas {
                    total_columns += schema.columns.len();
                    total_indexes += schema.indexes.len();
                    println!(
                        "  Table '{}': {} columns, {} indexes",
                        table_name,
                        schema.columns.len(),
                        schema.indexes.len()
                    );
                }

                println!("  Total columns: {total_columns}");
                println!("  Total indexes: {total_indexes}");
            }
            ".clear" | ".c" => {
                self.sql_buffer.clear();
                println!("SQL buffer cleared");
            }
            _ => {
                eprintln!(
                    "Error: Unknown command '{command}'. Type '.help' for available commands."
                );
                return Ok(false);
            }
        }
        Ok(false)
    }
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let cli = Cli::parse();

    let mut engine_config = EngineConfig::default();
    if let Some(max_keys) = cli.max_keys {
        engine_config.initial_capacity = if max_keys == 0 { None } else { Some(max_keys) };
    }
    if let Some(max_bytes) = cli.max_log_bytes {
        engine_config.preallocate_size = if max_bytes == 0 {
            None
        } else {
            Some(max_bytes)
        };
    }

    // Normalize DB identifier to always use file:// protocol expected by Database::open
    let db_path = if tegdb::protocol_utils::has_protocol(&cli.db_path, "file") {
        // Already a file:// identifier; require .teg strictly
        let raw = tegdb::protocol_utils::extract_path(&cli.db_path);
        let pb = std::path::PathBuf::from(raw);
        if pb.extension().and_then(|s| s.to_str()) != Some("teg") {
            eprintln!(
                "Error: Unsupported database file extension. Expected '.teg': {}",
                pb.display()
            );
            process::exit(1);
        }
        format!("file://{}", pb.to_string_lossy())
    } else {
        // Require provided path to already end with .teg; do not auto-append
        let pb = if Path::new(&cli.db_path).is_absolute() {
            std::path::PathBuf::from(&cli.db_path)
        } else {
            std::env::current_dir()?.join(&cli.db_path)
        };
        if pb.extension().and_then(|s| s.to_str()) != Some("teg") {
            eprintln!(
                "Error: Unsupported database file extension. Expected '.teg': {}",
                pb.display()
            );
            process::exit(1);
        }
        format!("file://{}", pb.to_string_lossy())
    };

    // Create database if it doesn't exist
    // Print create message if backing file does not exist
    if !cli.quiet {
        let fs_path = tegdb::protocol_utils::extract_path(&db_path);
        if !Path::new(fs_path).exists() {
            eprintln!("Creating new database: {db_path}");
        }
    }

    // Initialize CLI state
    let mut state = CliState::new(&db_path, engine_config)?;

    // Apply command line options
    if let Some(timer) = cli.timer {
        state.timer_enabled = timer == "on";
    }
    if let Some(echo) = cli.echo {
        state.echo_enabled = echo == "on";
    }
    if let Some(output) = cli.output {
        state.output_file = Some(output);
    }

    // Set output format
    match cli.mode.as_str() {
        "table" => state.output_format = OutputFormat::Table,
        "csv" => state.output_format = OutputFormat::Csv,
        "json" => state.output_format = OutputFormat::Json,
        _ => {
            eprintln!(
                "Error: Invalid output mode '{}'. Use table, csv, or json",
                cli.mode
            );
            process::exit(1);
        }
    }

    // Handle command mode
    if let Some(command) = cli.command {
        if !cli.quiet {
            eprintln!("Executing: {command}");
        }
        if let Err(e) = state.execute_sql(&command) {
            eprintln!("Error: {e}");
            process::exit(1);
        }
        return Ok(());
    }

    // Handle file mode
    if let Some(file) = cli.file {
        if !cli.quiet {
            eprintln!("Reading script: {file}");
        }
        let content = fs::read_to_string(&file)?;
        for line in content.lines() {
            let trimmed = line.trim();
            if !trimmed.is_empty() && !trimmed.starts_with("--") {
                if let Err(e) = state.execute_sql(trimmed) {
                    eprintln!("Error: {e}");
                    process::exit(1);
                }
            }
        }
        return Ok(());
    }

    // REPL mode
    if !cli.quiet {
        println!("TegDB CLI v{}", env!("CARGO_PKG_VERSION"));
        println!("Type '.help' for available commands, '.quit' to exit");
        println!("Connected to database: {db_path}");
    }

    // Create editor with in-memory history support
    let config = Config::default();
    let mut rl = Editor::<(), rustyline::history::DefaultHistory>::with_config(config)?;

    loop {
        // Show different prompt based on whether we're in multi-line mode
        let prompt = if state.sql_buffer.is_empty() {
            "tg> "
        } else {
            "  -> "
        };

        let readline = rl.readline(prompt);
        match readline {
            Ok(line) => {
                let trimmed = line.trim();
                if trimmed.is_empty() {
                    continue;
                }

                // Handle dot commands
                if trimmed.starts_with('.') {
                    // Add dot commands to history
                    if let Err(e) = rl.add_history_entry(&line) {
                        eprintln!("Warning: Could not add to history: {e}");
                    }

                    match state.handle_dot_command(trimmed) {
                        Ok(true) => break,     // Exit requested
                        Ok(false) => continue, // Command handled
                        Err(e) => {
                            eprintln!("Error: {e}");
                        }
                    }
                } else {
                    // Add SQL command to history when complete (ends with semicolon)
                    if trimmed.ends_with(';') || state.sql_buffer.is_empty() {
                        if let Err(e) = rl.add_history_entry(&line) {
                            eprintln!("Warning: Could not add to history: {e}");
                        }
                    }

                    // Handle SQL input (supports multi-line)
                    if let Err(e) = state.handle_sql_input(trimmed) {
                        eprintln!("Error: {e}");
                        // Clear buffer on error
                        state.sql_buffer.clear();
                    }
                }
            }
            Err(ReadlineError::Interrupted) => {
                println!("^C");
                continue;
            }
            Err(ReadlineError::Eof) => {
                println!("^D");
                break;
            }
            Err(err) => {
                eprintln!("Error: {err:?}");
                break;
            }
        }
    }

    Ok(())
}
