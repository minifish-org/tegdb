use clap::Parser;
use rustyline::error::ReadlineError;
use rustyline::Editor;
use std::fs;
use std::io::Write;
use std::path::Path;
use std::process;
use tegdb::{Database, QueryResult, SqlValue};

fn format_sql_value(value: &SqlValue) -> String {
    match value {
        SqlValue::Integer(i) => i.to_string(),
        SqlValue::Real(f) => f.to_string(),
        SqlValue::Text(s) => s.clone(),
        SqlValue::Vector(v) => format!("[{}]", v.iter().map(|x| x.to_string()).collect::<Vec<_>>().join(", ")),
        SqlValue::Null => "NULL".to_string(),
        SqlValue::Parameter(idx) => format!("?{}", idx + 1),
    }
}

fn format_query_result(result: &QueryResult) -> String {
    if result.rows().is_empty() {
        return "No rows returned".to_string();
    }
    
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
}

struct CliState {
    db: Database,
    timer_enabled: bool,
    echo_enabled: bool,
    output_file: Option<String>,
}

impl CliState {
    fn new(db_path: &str) -> Result<Self, Box<dyn std::error::Error>> {
        let db = Database::open(db_path)?;
        Ok(CliState {
            db,
            timer_enabled: false,
            echo_enabled: false,
            output_file: None,
        })
    }
    
    fn execute_sql(&mut self, sql: &str) -> Result<(), Box<dyn std::error::Error>> {
        if self.echo_enabled {
            println!("{}", sql);
        }
        
        let start = std::time::Instant::now();
        
        // Check if it's a SELECT statement
        let trimmed_sql = sql.trim().to_uppercase();
        let is_select = trimmed_sql.starts_with("SELECT");
        
        let result = if is_select {
            let query_result = self.db.query(sql)?;
            format_query_result(&query_result)
        } else {
            let rows_affected = self.db.execute(sql)?;
            format!("{}", rows_affected)
        };
        
        let duration = start.elapsed();
        
        if self.timer_enabled {
            eprintln!("Query executed in {:?}", duration);
        }
        
        // Handle output
        if let Some(ref output_file) = self.output_file {
            let mut file = fs::File::create(output_file)?;
            writeln!(file, "{}", result)?;
        } else {
            println!("{}", result);
        }
        
        Ok(())
    }
    
    fn handle_dot_command(&mut self, line: &str) -> Result<bool, Box<dyn std::error::Error>> {
        let parts: Vec<&str> = line.trim().split_whitespace().collect();
        if parts.is_empty() {
            return Ok(false);
        }
        
        match parts[0] {
            ".quit" | ".exit" => return Ok(true),
            ".help" => {
                println!("Available dot commands:");
                println!("  .help          - Show this help");
                println!("  .tables        - List all tables");
                println!("  .schema [table] - Show table schema");
                println!("  .output FILE   - Set output file");
                println!("  .read FILE     - Execute SQL from file");
                println!("  .timer on|off  - Toggle execution timing");
                println!("  .echo on|off   - Toggle SQL echo");
                println!("  .quit/.exit    - Exit REPL");
            }
            ".tables" => {
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
                        println!("{}", table);
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
                                tegdb::DataType::Text(Some(len)) => print!("TEXT({})", len),
                                tegdb::DataType::Text(None) => print!("TEXT"),
                                tegdb::DataType::Real => print!("REAL"),
                                tegdb::DataType::Vector(Some(dim)) => print!("VECTOR({})", dim),
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
                        println!("Table '{}' not found", table);
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
                        println!("Output set to {}", file);
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
            _ => return Ok(false),
        }
        Ok(false)
    }
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let cli = Cli::parse();
    
    // Convert to absolute path
    let db_path = if Path::new(&cli.db_path).is_absolute() {
        cli.db_path.clone()
    } else {
        std::env::current_dir()?.join(&cli.db_path).to_string_lossy().to_string()
    };
    
    // Create database if it doesn't exist
    if !Path::new(&db_path).exists() {
        if !cli.quiet {
            eprintln!("Creating new database: {}", db_path);
        }
    }
    
    // Initialize CLI state
    let mut state = CliState::new(&db_path)?;
    
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
    
    // Handle command mode
    if let Some(command) = cli.command {
        if !cli.quiet {
            eprintln!("Executing: {}", command);
        }
        if let Err(e) = state.execute_sql(&command) {
            eprintln!("Error: {}", e);
            process::exit(1);
        }
        return Ok(());
    }
    
    // Handle file mode
    if let Some(file) = cli.file {
        if !cli.quiet {
            eprintln!("Reading script: {}", file);
        }
        let content = fs::read_to_string(&file)?;
        for line in content.lines() {
            let trimmed = line.trim();
            if !trimmed.is_empty() && !trimmed.starts_with("--") {
                if let Err(e) = state.execute_sql(trimmed) {
                    eprintln!("Error: {}", e);
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
        println!("Connected to database: {}", db_path);
    }
    
    let mut rl = Editor::<(), rustyline::history::FileHistory>::new()?;
    
    loop {
        let readline = rl.readline("tg> ");
        match readline {
            Ok(line) => {
                let trimmed = line.trim();
                if trimmed.is_empty() {
                    continue;
                }
                
                // Handle dot commands
                if trimmed.starts_with('.') {
                    match state.handle_dot_command(trimmed) {
                        Ok(true) => break, // Exit requested
                        Ok(false) => continue, // Command handled
                        Err(e) => {
                            eprintln!("Error: {}", e);
                        }
                    }
                } else {
                    // Execute SQL
                    if let Err(e) = state.execute_sql(trimmed) {
                        eprintln!("Error: {}", e);
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
                eprintln!("Error: {:?}", err);
                break;
            }
        }
    }
    
    Ok(())
}
