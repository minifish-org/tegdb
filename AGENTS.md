# Repository Guidelines

## Project Structure & Module Organization
Core crate code lives in `src/`, with `lib.rs` exposing the public API, `database.rs` providing the SQLite-style façade, `storage_engine.rs` handling WAL-backed persistence, and `parser.rs`, `planner.rs`, and `query_processor.rs` covering SQL parsing, planning, and execution. Vector search logic resides in `vector_index.rs` and `backends/`. Integration and regression suites sit under `tests/`—use `integration/` for end-to-end coverage, `arithmetic/` for expression checks, and `performance/` for throughput scenarios. Top-level helpers such as `test_aggregate.rs` and `test_aggregate_execution.rs` exercise legacy interfaces. `examples/` contains runnable samples, while `benches/` hosts Criterion benchmarks and flamegraph assets.

## Build, Test, and Development Commands
- `cargo build --all-features` builds the crate with optional vector support enabled.
- `./fix_all.sh` runs formatting and clippy passes; use `./fix_format.sh` or `./fix_lint.sh` individually during iteration.
- `./run_all_tests.sh [--ci|--verbose]` runs the native suite; use `--ci` to keep test output in logs.
- `cargo bench --bench vector_search_benchmark --features dev` exercises performance benchmarks before publishing tuning changes.

## Coding Style & Naming Conventions
Rustfmt is the source of truth—run `cargo fmt --all` before committing. Keep code idiomatic Rust: 4-space indentation, `snake_case` for functions and modules, `PascalCase` for types, and exhaustive `match` statements where practical. Prefer explicit error propagation with `Result` over `unwrap`. Maintain the single-threaded architecture; avoid introducing `Send`/`Sync` requirements without discussion. Document new public items with `///` comments and keep modules focused on one layer of the stack.

## Testing Guidelines
Add unit tests beside implementations and integration tests under `tests/integration/` using descriptive file names such as `feature_name_test.rs`. When touching SQL semantics, mirror cases in `tests/arithmetic/` or `tests/performance/` as appropriate. For vector search features, extend `tests/integration_vector_features_test.rs` and rerun `cargo bench --bench vector_search_benchmark --features dev`. Always finish with `cargo test --all-features`; run `./run_all_tests.sh --ci` before submitting sizable changes to capture full logs.

## Commit & Pull Request Guidelines
Follow the existing conventional-style history (`feat:`, `fix:`, `refactor:`, `perf:`). Keep commits scoped and message bodies imperative, referencing issue IDs when available. Pull requests should include a concise summary, testing notes (e.g., `cargo test --all-features`, `./run_all_tests.sh --ci`), and screenshots or logs for UI-facing demos. Ensure documentation and scripts stay in sync with behavioral changes and request early review for schema or storage format updates.
