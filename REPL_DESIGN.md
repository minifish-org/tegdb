# TegDB REPL/CLI Design Document

## Executive Summary

TegDB currently exists as a library-only solution, lacking the standalone executable experience that users expect from embedded databases like SQLite (`sqlite3`) and DuckDB (`duckdb` CLI). This document outlines a roadmap to transform TegDB into a complete embedded database solution with a command-line interface and REPL.

## Target User Experience

### Core Requirements
- **Single executable file**: `tg` can be used as `./tg my.db` to enter REPL mode, or `./tg my.db -c "SELECT 1"` for batch processing
- **REPL with dot commands**: Support for `.open`, `.tables`, `.schema`, `.output file`, `.read script.sql` etc. (following SQLite/DuckDB dot command conventions)

## Implementation Roadmap

### Phase 1: CLI Foundation (2-3 iterations)

#### Iteration 1: Basic CLI
**Goal**: Create a minimal working CLI

**Tasks**:
- [x] Create new `crates/tg` binary crate
- [x] Add dependencies: `clap` (argument parsing), `rustyline` (REPL), `crossterm` (terminal I/O)
- [x] Implement basic startup: `tg <db-path> [-c <SQL>] [--output FILE]`
- [x] Support two modes:
  - With `-c`: execute SQL and exit
  - Without `-c`: enter REPL mode
- [x] Add basic REPL commands: `.quit`/`.exit` to exit, `.help` to list available dot commands

#### Iteration 2: Scripting and Batch Processing
**Goal**: Enable script execution and better batch processing

**Tasks**:
- [x] Support `./tg my.db < script.sql` (stdin input)
- [x] Implement `.read script.sql` command
- [x] Add proper exit codes: return non-zero on SQL execution failure (for CI/script integration)
- [x] Add timing controls: `.timer on|off` (print execution time)
- [x] Add echo controls: `.echo on|off` (echo SQL statements)

#### Iteration 3: Portable Distribution
**Goal**: Create distributable single-file executables

**Tasks**:
- [ ] Build static-linked or dependency-included single-file executables
- [ ] Support Linux/macOS/Windows platforms
- [ ] Match sqlite3/duckdb distribution experience

### Phase 2: Advanced Features

#### Import/Export (COPY)
**Goal**: Enable data import/export capabilities

**Tasks**:
- [x] Implement `COPY mytable FROM 'x.csv'` (basic CSV import with header detection)
- [ ] Implement `COPY (SELECT ...) TO 'out.parquet' (FORMAT PARQUET)`
- [x] Support multiple output formats (CSV, JSON)

#### Dot Command Ecosystem
**Goal**: Complete the dot command set

**Tasks**:
- [x] Implement `.tables [pattern]` - list tables with optional pattern matching
- [x] Implement `.schema [table]` - show table schema
- [x] Implement `.stats` - show database statistics
- [x] Add more utility commands as needed

#### Output Formatting
**Goal**: Improve result presentation

**Tasks**:
- [x] Implement table formatting for query results
- [x] Add column alignment and width management
- [x] Support different output formats (table, csv, json)
- [ ] Add pagination for large result sets

#### Quality and User Experience
**Goal**: Professional-grade CLI experience

**Tasks**:
- [x] Multi-line SQL editing support
- [x] Auto-completion for keywords/tables/columns
- [x] Command history search
- [x] SQL syntax highlighting
- [ ] Progress indicators and ETA for long-running queries
- [x] Execution time reporting

## Engineering Implementation Details

### Architecture
- **Library API separation**: Keep `tegdb::Database` as pure library API
- **CLI as thin wrapper**: REPL/batch processing via unified execution interface
- **Repository structure**: Add `bin/` directory or separate CLI crate

### Technical Considerations
- **File locking/WAL**: Document single-process write constraints (similar to SQLite's locking strategy)
- **Error handling**: 
  - Ctrl-C interrupts current query but doesn't exit process
  - Fatal errors return non-zero exit codes for script integration
- **Compatibility documentation**: List SQL dialect coverage, unsupported features, data type mappings, limitations

### Distribution Strategy
- **GitHub Releases**: Provide binaries for Linux/macOS/Windows
- **Package managers**: Homebrew/brew tap, winget, apt repository scripts
- **Static linking**: Ensure minimal external dependencies

## Command Interface Design

### CLI Options
```
tg <db-path> [options]

Options:
  -c, --command "<SQL>"      Execute SQL and exit
  -f, --file script.sql      Read and execute script
  -o, --output <file>        Redirect results to file
      --timer on|off         Enable/disable execution timing
      --echo on|off          Enable/disable SQL echo
      --mode table|csv|json  Set output format
  -q, --quiet                Quiet mode (output results only)
  -v, --version              Show version
  -h, --help                 Show help
```

### REPL Dot Commands
```
.tables [pattern]            List tables (with optional pattern)
.schema [table]              Show table schema
.output FILE | stdout        Set output destination
.read FILE                   Execute SQL from file
.timer on|off                Toggle execution timing
.echo on|off                 Toggle SQL echo
.mode table|csv|json         Set output format
.stats                       Show database statistics
.help                        Show available commands
.quit/.exit                  Exit REPL
```

## Success Metrics

- [x] Users can download a single `tg` executable and use it immediately
- [x] CLI experience matches SQLite/DuckDB familiarity
- [x] CLI provides professional database management experience
- [x] Script integration works seamlessly in CI/CD pipelines
- [ ] Cross-platform compatibility across major operating systems

## Implementation Status

### ✅ Completed Features
- **CLI Foundation**: Complete binary with argument parsing and REPL
- **Core Dot Commands**: .help, .tables, .schema, .output, .read, .timer, .echo, .quit, .clear
- **Script Execution**: File and stdin input support
- **Output Formats**: Table, CSV, JSON with proper escaping
- **Database Statistics**: .stats command for metadata
- **CSV Import**: Basic COPY FROM functionality
- **Error Handling**: Proper exit codes and error messages
- **Execution Timing**: Performance measurement
- **Multi-line SQL**: Support for complex statements with semicolon termination
- **Enhanced Parser**: Improved SQL parsing with better multi-line support
- **String Escaping**: Proper handling of escape sequences in string literals
- **Comprehensive Testing**: 31 parser tests and 34 CLI tests covering all features

### 🚧 Remaining Features
- **COPY TO**: Export query results to files
- **Pagination**: Large result set handling
- **Cross-platform Distribution**: Binary releases

### ✅ Recently Completed Features (v0.3.0)
- **Auto-completion**: SQL keyword completion with Tab key
- **Command History**: Persistent history with Ctrl+R search functionality
- **SQL Syntax Highlighting**: Color-coded SQL with keywords, strings, numbers, and punctuation

## Future Considerations

- **File reading table functions**: Direct CSV/Parquet/JSON file access
- **Plugin system**: Allow extensions for custom file formats
- **Remote database support**: Connect to remote TegDB instances
- **Advanced analytics**: Built-in statistical functions
- **Performance profiling**: Query execution analysis tools
- **Backup/restore**: Database backup and recovery utilities

---

*This design document provides a comprehensive roadmap for transforming TegDB from a library into a complete embedded database solution with professional CLI tooling.*
