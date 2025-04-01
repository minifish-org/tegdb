# Contributing to TegDB

## Overview

Thank you for your interest in contributing to TegDB! This document provides guidelines and instructions for contributing to the project, including code style, testing requirements, and the pull request process.

## Getting Started

### Prerequisites

1. **Development Environment**
   - Rust 1.70+
   - Cargo
   - Git
   - A code editor (VS Code recommended)

2. **Fork and Clone**

   ```bash
   # Fork the repository on GitHub
   # Clone your fork
   git clone https://github.com/YOUR_USERNAME/tegdb.git
   cd tegdb

   # Add upstream remote
   git remote add upstream https://github.com/minifish-org/tegdb.git
   ```

3. **Setup Development Tools**

   ```bash
   # Install development dependencies
   cargo install cargo-fmt cargo-clippy cargo-test

   # Install pre-commit hooks
   pre-commit install
   ```

## Development Workflow

### 1. Create a Branch

```bash
# Update your fork
git fetch upstream
git checkout main
git merge upstream/main

# Create a new branch
git checkout -b feature/your-feature-name
```

### 2. Make Changes

1. **Code Style**
   - Follow Rust standard style guide
   - Use `cargo fmt` for formatting
   - Run `cargo clippy` for linting

2. **Documentation**
   - Add documentation for new features
   - Update existing documentation
   - Include examples where appropriate

3. **Testing**
   - Write unit tests
   - Add integration tests
   - Update existing tests

### 3. Commit Changes

```bash
# Stage changes
git add .

# Commit with a descriptive message
git commit -m "feat: add new feature X

- Add feature X implementation
- Add tests for feature X
- Update documentation"
```

### 4. Push Changes

```bash
# Push to your fork
git push origin feature/your-feature-name
```

## Code Style Guide

### Rust Style

1. **Formatting**

   ```rust
   // Use standard Rust formatting
   fn example_function(param1: String, param2: i32) -> Result<(), Error> {
       // Implementation
   }
   ```

2. **Naming Conventions**
   - Use snake_case for functions and variables
   - Use PascalCase for types and traits
   - Use SCREAMING_SNAKE_CASE for constants

3. **Documentation**

   ```rust
   /// Brief description of the function
   ///
   /// Detailed description if needed
   ///
   /// # Examples
   ///
   /// ```
   /// let result = example_function("test", 42)?;
   /// ```
   fn example_function(param1: &str, param2: i32) -> Result<(), Error> {
       // Implementation
   }
   ```

### Project Structure

1. **Module Organization**

   ```
   src/
   ├── lib.rs
   ├── engine/
   │   ├── mod.rs
   │   ├── skiplist.rs
   │   └── wal.rs
   ├── transaction/
   │   ├── mod.rs
   │   └── lock.rs
   └── database/
       ├── mod.rs
       └── error.rs
   ```

2. **File Naming**
   - Use snake_case for file names
   - Group related functionality in modules
   - Keep files focused and concise

## Testing Guidelines

### Unit Tests

1. **Test Organization**

   ```rust
   #[cfg(test)]
   mod tests {
       use super::*;

       #[test]
       fn test_feature() {
           // Test implementation
       }
   }
   ```

2. **Test Coverage**
   - Aim for high test coverage
   - Test edge cases
   - Test error conditions

### Integration Tests

1. **Test Structure**

   ```rust
   // tests/integration_test.rs
   use tegdb;

   #[test]
   fn test_feature_integration() {
       // Integration test implementation
   }
   ```

2. **Test Environment**
   - Use isolated test environments
   - Clean up after tests
   - Handle test dependencies

## Pull Request Process

### 1. Create Pull Request

1. **Title Format**

   ```text
   type(scope): description

   Examples:
   feat(engine): add new storage backend
   fix(transaction): resolve deadlock issue
   docs(api): update API documentation
   ```

2. **Description Template**

   ```markdown
   ## Description
   Brief description of the changes

   ## Related Issues
   Fixes #123
   Closes #456

   ## Testing
   - [ ] Unit tests added/updated
   - [ ] Integration tests added/updated
   - [ ] Manual testing completed

   ## Documentation
   - [ ] Code documentation updated
   - [ ] API documentation updated
   - [ ] README updated
   ```

### 2. Review Process

1. **Code Review Checklist**
   - [ ] Code follows style guide
   - [ ] Tests are comprehensive
   - [ ] Documentation is complete
   - [ ] No breaking changes
   - [ ] Performance impact considered

2. **Addressing Feedback**
   - Respond to all comments
   - Make requested changes
   - Update PR description
   - Request re-review when ready

### 3. Merge Process

1. **Pre-merge Checklist**
   - [ ] All CI checks pass
   - [ ] All review comments addressed
   - [ ] Documentation updated
   - [ ] Tests passing
   - [ ] No conflicts

2. **Merge Guidelines**
   - Use squash merge
   - Update version if needed
   - Update changelog
   - Tag release if applicable

## Release Process

### 1. Version Bumping

1. **Semantic Versioning**
   - MAJOR: Breaking changes
   - MINOR: New features
   - PATCH: Bug fixes

2. **Update Version**

   ```toml
   # Cargo.toml
   [package]
   version = "1.0.0"
   ```

### 2. Changelog

1. **Update CHANGELOG.md**

   ```markdown
   # Changelog

   ## [1.0.0] - 2024-01-01
   ### Added
   - New feature X
   - New feature Y

   ### Changed
   - Updated feature Z

   ### Fixed
   - Bug in feature A
   ```

2. **Release Notes**
   - Summarize changes
   - Highlight breaking changes
   - Include migration guide if needed

## Community Guidelines

### 1. Communication

1. **Issues**
   - Use issue templates
   - Provide detailed information
   - Be respectful and professional

2. **Discussions**
   - Stay on topic
   - Be constructive
   - Follow code of conduct

### 2. Code of Conduct

1. **Principles**
   - Be respectful
   - Be inclusive
   - Be professional

2. **Reporting**
   - Report violations
   - Provide evidence
   - Follow process

## Development Tools

### 1. Recommended Tools

1. **IDE Setup**
   - VS Code
   - rust-analyzer
   - CodeLLDB

2. **Development Tools**
   - cargo-watch
   - cargo-edit
   - cargo-expand

### 2. CI/CD Tools

1. **GitHub Actions**

   ```yaml
   name: CI
   on: [push, pull_request]
   jobs:
     test:
       runs-on: ubuntu-latest
       steps:
         - uses: actions/checkout@v2
         - uses: actions-rs/toolchain@v1
           with:
             toolchain: stable
         - name: Test
           run: cargo test
   ```

2. **Code Quality**
   - clippy
   - rustfmt
   - coverage

## Getting Help

### 1. Resources

1. **Documentation**
   - API documentation
   - Architecture guide
   - Examples

2. **Community**
   - GitHub Discussions
   - Discord server
   - Stack Overflow

### 2. Support Channels

1. **Questions**
   - Use GitHub Discussions
   - Check existing issues
   - Search documentation

2. **Bugs**
   - Create detailed issue
   - Include reproduction steps
   - Provide environment info

## License

### 1. Code License

- All code is licensed under MIT License
- Include license header in new files
- Update license year if needed

### 2. Documentation License

- Documentation is licensed under CC BY 4.0
- Include attribution when using
- Keep documentation up to date

## Acknowledgments

Thank you for contributing to TegDB! Your contributions help make the project better for everyone.
