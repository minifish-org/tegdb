use clap::Parser;
use const_format::formatcp;
use rustyline::error::ReadlineError;
use rustyline::{Config, Editor};
use std::fs;
use std::io::Write;
use std::path::Path;
use std::process;
use tegdb::storage_engine::{
    EngineConfig, DEFAULT_COMPACTION_RATIO_STR, DEFAULT_COMPACTION_THRESHOLD_RATIO_STR,
    DEFAULT_INITIAL_CAPACITY_KEYS, DEFAULT_PREALLOCATE_SIZE_MB,
};
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
    #[arg(
        long,
        value_name = "KEYS",
        help = formatcp!(
            "Maximum number of keys to keep in memory (0 disables the cap, default: {})",
            DEFAULT_INITIAL_CAPACITY_KEYS
        )
    )]
    max_keys: Option<usize>,

    /// Maximum on-disk log size in bytes (0 disables the cap)
    #[arg(
        long,
        value_name = "BYTES",
        help = formatcp!(
            "Maximum on-disk log size (0 disables the cap, default: {}MB)",
            DEFAULT_PREALLOCATE_SIZE_MB
        )
    )]
    max_log_bytes: Option<u64>,

    /// Compaction threshold ratio relative to the preallocated log size
    #[arg(
        long,
        value_name = "RATIO",
        help = formatcp!(
            "Compaction threshold ratio relative to preallocated size (default: {})",
            DEFAULT_COMPACTION_THRESHOLD_RATIO_STR
        )
    )]
    compaction_threshold: Option<f64>,

    /// Compaction ratio threshold
    #[arg(
        long,
        value_name = "RATIO",
        help = formatcp!(
            "Compaction ratio threshold (default: {})",
            DEFAULT_COMPACTION_RATIO_STR
        )
    )]
    compaction_ratio: Option<f64>,
}

#[derive(Debug, Clone, Copy, PartialEq)]
enum OutputFormat {
    Table,
    Csv,
    Json,
}

#[derive(Default)]
struct SqlChunker {
    buffer: String,
    in_single_quote: bool,
    in_double_quote: bool,
    escape_next: bool,
}

impl SqlChunker {
    fn new() -> Self {
        Self::default()
    }

    fn clear(&mut self) {
        self.buffer.clear();
        self.in_single_quote = false;
        self.in_double_quote = false;
        self.escape_next = false;
    }

    fn in_quote(&self) -> bool {
        self.in_single_quote || self.in_double_quote
    }

    fn has_pending(&self) -> bool {
        self.in_quote() || !self.buffer.trim().is_empty()
    }

    fn feed_line(&mut self, line: &str) -> Vec<String> {
        let mut statements = Vec::new();
        let normalized = line.trim_end_matches('\r');
        let trimmed = normalized.trim_start();

        if !self.in_quote() && trimmed.starts_with("--") {
            if let Some(statement) = self.take_pending_statement() {
                statements.push(statement);
            }
            return statements;
        }

        let mut chars = normalized.chars().peekable();

        while let Some(ch) = chars.next() {
            if !self.in_quote() && ch == '-' && matches!(chars.peek(), Some('-')) {
                chars.next();
                if let Some(statement) = self.take_pending_statement() {
                    statements.push(statement);
                }
                break;
            }

            self.buffer.push(ch);

            if self.in_single_quote {
                if self.escape_next {
                    self.escape_next = false;
                    continue;
                }
                match ch {
                    '\\' => {
                        self.escape_next = true;
                    }
                    '\'' => {
                        if matches!(chars.peek(), Some('\'')) {
                            self.buffer.push('\'');
                            chars.next();
                        } else {
                            self.in_single_quote = false;
                        }
                    }
                    _ => {}
                }
                continue;
            }

            if self.in_double_quote {
                if self.escape_next {
                    self.escape_next = false;
                    continue;
                }
                match ch {
                    '\\' => {
                        self.escape_next = true;
                    }
                    '"' => {
                        if matches!(chars.peek(), Some('"')) {
                            self.buffer.push('"');
                            chars.next();
                        } else {
                            self.in_double_quote = false;
                        }
                    }
                    _ => {}
                }
                continue;
            }

            match ch {
                '\'' => {
                    self.in_single_quote = true;
                    self.escape_next = false;
                }
                '"' => {
                    self.in_double_quote = true;
                    self.escape_next = false;
                }
                ';' => {
                    let statement = self.buffer.trim().to_string();
                    if !statement.is_empty() {
                        statements.push(statement);
                    }
                    self.buffer.clear();
                }
                _ => {}
            }
        }

        if self.has_pending() {
            self.buffer.push('\n');
        }

        statements
    }

    fn feed_text(&mut self, text: &str) -> Vec<String> {
        let mut statements = Vec::new();
        for line in text.split('\n') {
            statements.extend(self.feed_line(line));
        }
        statements
    }

    fn take_pending_statement(&mut self) -> Option<String> {
        if self.in_quote() {
            return None;
        }
        let trimmed = self.buffer.trim();
        if trimmed.is_empty() {
            return None;
        }
        let statement = trimmed.to_string();
        self.buffer.clear();
        Some(statement)
    }
}

struct CliState {
    db: Database,
    timer_enabled: bool,
    echo_enabled: bool,
    output_file: Option<String>,
    output_format: OutputFormat,
    sql_chunker: SqlChunker,
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
            sql_chunker: SqlChunker::new(),
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

    fn execute_script(&mut self, script: &str) -> Result<(), Box<dyn std::error::Error>> {
        let mut chunker = SqlChunker::new();
        for statement in chunker.feed_text(script) {
            self.execute_sql(&statement)?;
        }
        if chunker.in_quote() {
            return Err("Incomplete SQL statement: unclosed quote".into());
        }
        if let Some(pending) = chunker.take_pending_statement() {
            self.execute_sql(&pending)?;
        }
        Ok(())
    }

    fn handle_sql_input(&mut self, input: &str) -> Result<(), Box<dyn std::error::Error>> {
        for statement in self.sql_chunker.feed_text(input) {
            self.execute_sql(&statement)?;
        }
        Ok(())
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
                    self.execute_script(&content)?;
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
                self.sql_chunker.clear();
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
    if let Some(threshold_ratio) = cli.compaction_threshold {
        engine_config.compaction_threshold_ratio = threshold_ratio;
    }
    if let Some(ratio) = cli.compaction_ratio {
        engine_config.compaction_ratio = ratio;
    }

    // Normalize DB identifier for file:// or rpc://
    let (protocol, _path) = tegdb::protocol_utils::parse_storage_identifier(&cli.db_path);
    let db_path = if protocol == "file" {
        if tegdb::protocol_utils::has_protocol(&cli.db_path, "file") {
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
        }
    } else if protocol == "rpc" {
        cli.db_path.clone()
    } else {
        eprintln!(
            "Error: Unsupported protocol '{protocol}'. Only 'file://' or 'rpc://' are supported."
        );
        process::exit(1);
    };

    // Create database if it doesn't exist
    // Print create message if backing file does not exist
    if !cli.quiet && protocol == "file" {
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
        if let Err(e) = state.execute_script(command.as_str()) {
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
        if let Err(e) = state.execute_script(&content) {
            eprintln!("Error: {e}");
            process::exit(1);
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
        let prompt = if state.sql_chunker.has_pending() {
            "  -> "
        } else {
            "tg> "
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
                    if trimmed.ends_with(';') || !state.sql_chunker.has_pending() {
                        if let Err(e) = rl.add_history_entry(&line) {
                            eprintln!("Warning: Could not add to history: {e}");
                        }
                    }

                    // Handle SQL input (supports multi-line)
                    if let Err(e) = state.handle_sql_input(&line) {
                        eprintln!("Error: {e}");
                        // Clear buffer on error
                        state.sql_chunker.clear();
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

#[cfg(test)]
mod tests {
    use super::SqlChunker;

    #[test]
    fn splits_multiple_statements_on_single_line() {
        let mut chunker = SqlChunker::new();
        let statements = chunker.feed_line("SELECT 1; SELECT 2;");
        assert_eq!(statements, vec!["SELECT 1;", "SELECT 2;"]);
    }

    #[test]
    fn preserves_multiline_statements() {
        let mut chunker = SqlChunker::new();
        assert!(chunker.feed_line("SELECT 1").is_empty());
        let statements = chunker.feed_line("FROM numbers;");
        assert_eq!(statements, vec!["SELECT 1\nFROM numbers;"]);
    }

    #[test]
    fn ignores_comments_and_semicolons_in_quotes() {
        let mut chunker = SqlChunker::new();
        assert!(chunker.feed_line("-- just a comment").is_empty());

        let statements = chunker.feed_line("SELECT '-- comment' AS note;");
        assert_eq!(statements, vec!["SELECT '-- comment' AS note;"]);
    }

    #[test]
    fn pending_statement_without_semicolon_executes_once_finished() {
        let mut chunker = SqlChunker::new();
        assert!(chunker.feed_line("SELECT 1").is_empty());
        assert!(chunker.feed_line("FROM dual").is_empty());
        assert!(!chunker.in_quote());
        assert_eq!(
            chunker.take_pending_statement(),
            Some("SELECT 1\nFROM dual".to_string())
        );
        assert!(chunker.take_pending_statement().is_none());
    }

    #[test]
    fn pending_statement_not_available_when_in_quotes() {
        let mut chunker = SqlChunker::new();
        assert!(chunker.feed_line("SELECT 'unterminated").is_empty());
        assert!(chunker.in_quote());
        assert!(chunker.take_pending_statement().is_none());
    }

    #[test]
    fn supports_backslash_and_double_quote_escape_sequences() {
        let mut chunker = SqlChunker::new();
        let statements = chunker.feed_line("INSERT INTO t VALUES ('Charlie\\'s Name');");
        assert_eq!(
            statements,
            vec!["INSERT INTO t VALUES ('Charlie\\'s Name');"]
        );

        let statements = chunker.feed_line("SELECT \"He said \"\"hi\"\"\";");
        assert_eq!(statements, vec!["SELECT \"He said \"\"hi\"\"\";"]);
    }

    #[test]
    fn comment_line_flushed_pending_statement() {
        let mut chunker = SqlChunker::new();
        assert!(chunker.feed_line("SELECT 42").is_empty());
        let statements = chunker.feed_line("-- comment");
        assert_eq!(statements, vec!["SELECT 42"]);
        assert!(!chunker.has_pending());
    }

    #[test]
    fn inline_comment_flushed_pending_statement() {
        let mut chunker = SqlChunker::new();
        let statements = chunker.feed_line("SELECT 1 -- comment");
        assert_eq!(statements, vec!["SELECT 1"]);
        assert!(!chunker.has_pending());
    }

    #[test]
    fn multiline_chunk_with_comment_and_sql() {
        let mut chunker = SqlChunker::new();
        let statements = chunker.feed_text(
            "-- heading line\n\nCREATE TABLE t (id INTEGER);\nINSERT INTO t VALUES (1);\n",
        );
        assert_eq!(
            statements,
            vec!["CREATE TABLE t (id INTEGER);", "INSERT INTO t VALUES (1);"]
        );
    }
}
