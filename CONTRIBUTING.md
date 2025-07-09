# Contributing to TegDB

Thank you for your interest in contributing to TegDB! This document provides guidelines for contributing to the project.

## Development Philosophy

TegDB follows these core principles:

1. **Simplicity First**: Prefer simple, understandable solutions over complex optimizations
2. **Reliability**: Prioritize correctness and data integrity over performance
3. **Standard Library**: Use Rust's standard library when possible to minimize dependencies
4. **Single-threaded**: Maintain the single-threaded design to eliminate concurrency issues
5. **Resource Efficient**: Optimize for memory usage and minimize resource consumption

## Development Setup

### Prerequisites

- Rust 1.70 or later
- Git

### Building the Project

```bash
# Clone the repository
git clone https://github.com/tegridydev/tegdb.git
cd tegdb

# Build the project
cargo build

# Build with development features (for testing and benchmarks)
cargo build --features dev

# Run tests
cargo test --features dev

# Run benchmarks
cargo bench --features dev
```

### Code Quality

TegDB provides convenient scripts for maintaining code quality:

```bash
# Fix both formatting and linting issues
./fix_all.sh

# Individual scripts (used internally by fix_all.sh)
./fix_format.sh  # Fix code formatting
./fix_lint.sh    # Fix clippy linting issues
```

### Running Tests

```bash
# Run all tests (native, WASM, browser)
./run_all_tests.sh

# Run only native tests
./run_all_tests.sh --skip-wasm --skip-browser

# Run in CI mode (skips browser tests)
./run_all_tests.sh --ci

# Run with verbose output
./run_all_tests.sh --verbose
```

### Project Structure

```text
src/
├── lib.rs           # Public API and feature flags
├── engine.rs        # Core storage engine with transactions
├── database.rs      # High-level SQLite-like interface
├── executor.rs      # SQL statement execution and optimization
├── parser.rs        # SQL parsing using nom
├── serialization.rs # Binary serialization utilities
└── error.rs         # Error types and handling

tests/               # Integration tests
benches/             # Performance benchmarks
examples/            # Usage examples
```

## Making Contributions

### Types of Contributions

- **Bug fixes**: Fix issues in existing functionality
- **Performance improvements**: Optimize algorithms or data structures
- **Feature additions**: Add new SQL features or engine capabilities
- **Documentation**: Improve code documentation, examples, or guides
- **Testing**: Add test cases for edge cases or new functionality

### Before You Start

1. Check existing issues and pull requests to avoid duplicating work
2. For major changes, open an issue first to discuss the approach
3. Ensure your changes align with TegDB's design principles

### Development Workflow

1. **Fork** the repository
2. **Create a branch** for your feature/fix: `git checkout -b feature/your-feature-name`
3. **Make your changes** following the guidelines below
4. **Add tests** for new functionality
5. **Run the test suite** to ensure nothing breaks
6. **Update documentation** if needed
7. **Commit your changes** with clear, descriptive messages
8. **Push** your branch and create a pull request

## Coding Guidelines

### Rust Style

Follow standard Rust conventions:

- Use `./fix_all.sh` to fix both formatting and linting issues
- Use `./fix_format.sh` or `cargo fmt` to format code
- Use `./fix_lint.sh` or run `cargo clippy` and address warnings
- Follow Rust naming conventions (snake_case for functions/variables, PascalCase for types)
- Add documentation comments (`///`) for public APIs

### Code Organization

#### Engine Layer (`engine.rs`)
- Keep the engine focused on key-value operations and transactions
- Maintain ACID properties in all operations
- Use Arc<[u8]> for efficient value sharing
- Handle errors gracefully with proper cleanup

#### Database Layer (`database.rs`)
- Provide a clean SQLite-like interface
- Cache schemas at the database level for performance
- Handle transaction management transparently
- Convert engine errors to user-friendly messages

#### Parser Layer (`parser.rs`)
- Use nom combinators for SQL parsing
- Keep AST structures simple and well-documented
- Support standard SQL syntax where possible
- Provide clear error messages for parsing failures

#### Executor Layer (`executor.rs`)
- Optimize queries using primary key lookups when possible
- Implement streaming for LIMIT queries
- Validate constraints and data types
- Use efficient binary serialization

### Error Handling

- Use TegDB's `Result<T>` type consistently
- Provide descriptive error messages that help users understand what went wrong
- Handle partial failures gracefully (e.g., during recovery)
- Clean up resources properly in error cases

### Testing

All contributions should include appropriate tests:

#### Unit Tests
```rust
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_your_feature() {
        // Test implementation
    }
}
```

#### Integration Tests
Place integration tests in the `tests/` directory:

```rust
// tests/your_feature_test.rs
use tegdb::{Database, Result};

#[test]
fn test_integration_scenario() -> Result<()> {
    // Integration test implementation
    Ok(())
}
```

#### ACID Tests
When adding transaction-related features, include ACID compliance tests:

```rust
#[test]
fn test_atomicity() {
    // Ensure all-or-nothing behavior
}

#[test]
fn test_consistency() {
    // Ensure constraints are maintained
}

#[test]
fn test_isolation() {
    // Ensure transaction isolation
}

#[test]
fn test_durability() {
    // Ensure committed data survives crashes
}
```

### Documentation

#### Code Comments
- Document all public APIs with `///` comments
- Explain complex algorithms or data structures
- Include examples for non-obvious usage

#### Examples
- Add examples to `examples/` directory for new features
- Keep examples simple and focused
- Include error handling in examples

#### Architecture Documentation
- Update `ARCHITECTURE.md` for significant architectural changes
- Document design decisions and trade-offs
- Explain performance characteristics

## Performance Considerations

### Benchmarking
Always benchmark performance-critical changes:

```bash
# Run benchmarks before your changes
cargo bench --features dev > before.txt

# Make your changes, then run benchmarks again
cargo bench --features dev > after.txt

# Compare results
```

### Memory Usage
- Prefer streaming over loading large datasets into memory
- Use Arc<[u8]> for shared value references
- Implement lazy initialization where appropriate
- Profile memory usage for large operations

### Storage Efficiency
- Maintain compact binary serialization formats
- Consider compression for large values (future enhancement)
- Implement efficient compaction strategies

## Testing Guidelines

### Test Coverage
- Aim for comprehensive test coverage of new code
- Include both positive and negative test cases
- Test edge cases and error conditions
- Verify ACID properties for transaction-related changes

### Test Organization
```rust
#[cfg(test)]
mod tests {
    mod unit_tests {
        // Test individual functions/methods
    }
    
    mod integration_tests {
        // Test component interactions
    }
    
    mod acid_tests {
        // Test ACID compliance
    }
    
    mod performance_tests {
        // Test performance characteristics
    }
}
```

### Test Data
- Use deterministic test data when possible
- Clean up test databases after tests complete
- Use temporary directories for file-based tests

## Pull Request Process

### Pull Request Requirements
1. **Clear description**: Explain what changes you made and why
2. **Tests included**: All new functionality must have tests
3. **Documentation updated**: Update relevant documentation
4. **No breaking changes**: Unless discussed in an issue first
5. **Clean commit history**: Squash commits if needed

### Pull Request Template
```markdown
## Description
Brief description of changes

## Type of Change
- [ ] Bug fix
- [ ] New feature
- [ ] Performance improvement
- [ ] Documentation update
- [ ] Other (please describe)

## Testing
- [ ] Unit tests added/updated
- [ ] Integration tests added/updated
- [ ] All tests pass
- [ ] Benchmarks run (if applicable)

## Checklist
- [ ] Code follows project style guidelines
- [ ] Self-review completed
- [ ] Documentation updated
- [ ] No breaking changes
```

### Review Process
1. Automated checks must pass (tests, clippy, formatting)
2. Code review by maintainers
3. Address any feedback or requested changes
4. Final approval and merge

## Performance Benchmarks

### Running Benchmarks
```bash
# Run all benchmarks
cargo bench --features dev

# Run specific benchmark
cargo bench --features dev parser_benchmark

# Compare with other databases
cargo bench --features dev database_vs_sqlite_benchmark
```

### Adding Benchmarks
Create benchmark files in `benches/`:

```rust
// benches/your_benchmark.rs
use criterion::{black_box, criterion_group, criterion_main, Criterion};
use tegdb::Database;

fn benchmark_your_feature(c: &mut Criterion) {
    c.bench_function("your_feature", |b| {
        b.iter(|| {
            // Benchmark implementation
        });
    });
}

criterion_group!(benches, benchmark_your_feature);
criterion_main!(benches);
```

## Release Process

### Version Numbering
TegDB follows semantic versioning:
- **Major**: Breaking changes
- **Minor**: New features (backwards compatible)
- **Patch**: Bug fixes (backwards compatible)

### Release Checklist
1. Update version in `Cargo.toml`
2. Update `CHANGELOG.md`
3. Run full test suite
4. Run benchmarks and verify performance
5. Update documentation if needed
6. Tag release in git
7. Publish to crates.io

## Getting Help

### Documentation
- Read `ARCHITECTURE.md` for implementation details
- Check existing issues and pull requests
- Review code comments and examples

### Communication
- Open an issue for questions or discussions
- Provide clear reproduction steps for bugs
- Include relevant code samples and error messages

### Maintainer Contact
For questions about contributing, please:
1. Check existing documentation first
2. Search through issues for similar questions
3. Open a new issue with the "question" label

## Code of Conduct

### Our Standards
- Be respectful and inclusive
- Focus on constructive feedback
- Help newcomers learn and contribute
- Maintain professional communication

### Enforcement
Instances of unacceptable behavior may result in:
- Warning
- Temporary ban from the project
- Permanent ban from the project

Report conduct issues to the project maintainers.

## License

By contributing to TegDB, you agree that your contributions will be licensed under the AGPL-3.0 License.

Thank you for contributing to TegDB!
