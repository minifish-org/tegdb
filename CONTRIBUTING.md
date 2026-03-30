# Contributing to TegDB

Thank you for your interest in contributing to TegDB! This guide explains how to set up your development environment, run tests, and submit high-quality pull requests.

## Design Principles

All contributions must respect TegDB's core principles:

1. **Simplicity First** — prefer simple, understandable solutions over clever ones.
2. **Reliability** — correctness comes before performance.
3. **Standard Library** — use `std` when possible; minimize new dependencies.
4. **Single-Threaded** — do not introduce concurrency unless there is a compelling, documented reason.
5. **Resource Efficient** — optimize for memory and CPU; keep the footprint small.

## Prerequisites

- Rust stable toolchain (see `rust-toolchain.toml` if present, otherwise latest stable).
- `cargo fmt`, `cargo clippy`, and `cargo test` must all pass before submitting.
- Optional: Docker or Podman for running MinIO locally when testing `tgstream`.

## Getting Started

```bash
# Clone the repository
git clone https://github.com/minifish-org/tegdb.git
cd tegdb

# Build the library and all binaries
cargo build --all-features

# Run the test suite
cargo test

# Run the full CI-equivalent precheck (required before submitting)
./ci_precheck.sh
```

## Code Quality Standards

TegDB enforces zero-warning builds. The following suppressions are **prohibited**:

- `#[allow(dead_code)]`
- `#[allow(unused_*)]`
- `#[allow(clippy::*)]`

If code produces a warning, fix the root cause — remove unused code, rename variables, or restructure logic.

### Formatting

```bash
cargo fmt --all
```

### Linting

```bash
cargo clippy --all-targets --all-features -- -D warnings
```

### Pre-submission Check

Run the full CI suite locally before opening a pull request:

```bash
./ci_precheck.sh
```

This script checks formatting, linting, builds, documentation, tests, and key examples. A pull request will not be merged if this script fails.

## Running Tests

```bash
# Unit and integration tests
cargo test

# All tests with all feature flags
cargo test --all-features

# Verbose output
cargo test -- --nocapture

# Full native test suite (integration + examples)
./run_all_tests.sh
```

## Project Structure

| Path | Description |
|---|---|
| `src/` | Library source code |
| `src/bin/` | CLI binaries (`tg`, `tgstream`, `tglogd`) |
| `tests/` | Integration tests |
| `examples/` | Runnable example programs |
| `benches/` | Criterion benchmarks |
| `docs/` | User-facing documentation |
| `capnp/` | Cap'n Proto schema files |

See [ARCHITECTURE.md](ARCHITECTURE.md) for a detailed description of each source module.

## Adding New Features

1. **Discuss first** — open an issue to describe the feature and its design before writing code. This prevents duplicate effort and ensures the change aligns with TegDB's design principles.
2. **Write tests** — every new behavior must be covered by a unit or integration test.
3. **Update documentation** — update `README.md`, the relevant file under `docs/`, and `ARCHITECTURE.md` if the change affects the architecture.
4. **No breaking changes without discussion** — the library API is still evolving, but changes that break existing users must be discussed in the tracking issue.

## Submitting a Pull Request

1. Fork the repository and create a feature branch from `main`.
2. Make your changes following the standards above.
3. Run `./ci_precheck.sh` and confirm it passes completely.
4. Open a pull request against `main` with:
   - A clear title describing the change.
   - A description explaining *what* changed and *why*.
   - Links to any related issues.

## Benchmarks

Benchmarks live in `benches/`. To run them:

```bash
cargo bench
```

Skip benchmarks unless your change is performance-sensitive; they take significantly longer than the test suite.

## Reporting Bugs

Please open a GitHub issue with:

- A minimal reproducible example (SQL snippet or code).
- The TegDB version (`cargo pkgid tegdb`).
- Your OS and Rust toolchain version (`rustc --version`).
- Expected vs. actual behavior.

## License

By contributing, you agree that your contributions will be licensed under the [AGPL-3.0 License](LICENSE).
