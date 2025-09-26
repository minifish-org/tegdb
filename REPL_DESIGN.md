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
- [ ] Create new `crates/tg` binary crate
- [ ] Add dependencies: `clap` (argument parsing), `rustyline` (REPL), `crossterm` (terminal I/O)
- [ ] Implement basic startup: `tg <db-path> [-c <SQL>] [--output FILE]`
- [ ] Support two modes:
  - With `-c`: execute SQL and exit
  - Without `-c`: enter REPL mode
- [ ] Add basic REPL commands: `.quit`/`.exit` to exit, `.help` to list available dot commands

#### Iteration 2: Scripting and Batch Processing
**Goal**: Enable script execution and better batch processing

**Tasks**:
- [ ] Support `./tg my.db < script.sql` (stdin input)
- [ ] Implement `.read script.sql` command
- [ ] Add proper exit codes: return non-zero on SQL execution failure (for CI/script integration)
- [ ] Add timing controls: `.timer on|off` (print execution time)
- [ ] Add echo controls: `.echo on|off` (echo SQL statements)

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
- [ ] Implement `COPY mytable FROM 'x.csv' WITH (header, delimiter=',')`
- [ ] Implement `COPY (SELECT ...) TO 'out.parquet' (FORMAT PARQUET)`
- [ ] Support multiple output formats (CSV, Parquet, JSON)

#### Dot Command Ecosystem
**Goal**: Complete the dot command set

**Tasks**:
- [ ] Implement `.tables [pattern]` - list tables with optional pattern matching
- [ ] Implement `.schema [table]` - show table schema
- [ ] Implement `.stats` - show database statistics
- [ ] Add more utility commands as needed

#### Output Formatting
**Goal**: Improve result presentation

**Tasks**:
- [ ] Implement table formatting for query results
- [ ] Add column alignment and width management
- [ ] Support different output formats (table, csv, json)
- [ ] Add pagination for large result sets

#### Quality and User Experience
**Goal**: Professional-grade CLI experience

**Tasks**:
- [ ] Multi-line SQL editing support
- [ ] Auto-completion for keywords/tables/columns
- [ ] Command history search
- [ ] SQL syntax highlighting
- [ ] Progress indicators and ETA for long-running queries
- [ ] Execution time reporting

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
tegdb <db-path> [options]

Options:
  -c, --command "<SQL>"      Execute SQL and exit
  -e, --file script.sql      Read and execute script
  -o, --output <file>        Redirect results to file
      --timer on|off         Enable/disable execution timing
      --echo on|off          Enable/disable SQL echo
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
.quit                        Exit REPL
```

## Success Metrics

- [ ] Users can download a single `tg` executable and use it immediately
- [ ] CLI experience matches SQLite/DuckDB familiarity
- [ ] CLI provides professional database management experience
- [ ] Script integration works seamlessly in CI/CD pipelines
- [ ] Cross-platform compatibility across major operating systems

## Future Considerations

- **File reading table functions**: Direct CSV/Parquet/JSON file access
- **Plugin system**: Allow extensions for custom file formats
- **Remote database support**: Connect to remote TegDB instances
- **Advanced analytics**: Built-in statistical functions
- **Performance profiling**: Query execution analysis tools
- **Backup/restore**: Database backup and recovery utilities

---

*This design document provides a comprehensive roadmap for transforming TegDB from a library into a complete embedded database solution with professional CLI tooling.*
